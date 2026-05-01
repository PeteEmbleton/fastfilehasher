using Microsoft.Data.Sqlite;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace FastFileHasher;

public class Program
{
    const int DEFAULT_BATCH_SIZE = 5000;
    const int ROBOCOPY_FILE_BATCH_SIZE = 200;
    const long ROBOCOPY_TARGET_BATCH_BYTES = 1L << 30; // 1 GiB
    const int STALE_BATCH_SECONDS = 1800;

    public static void Main(string[] args)
    {
        bool inMemory = false;
        bool useRobocopy = false;
        var argList = args.ToList();

        if (argList.Contains("--in-memory"))
        {
            inMemory = true;
            argList.Remove("--in-memory");
        }
        if (argList.Contains("--use-robocopy"))
        {
            useRobocopy = true;
            argList.Remove("--use-robocopy");
        }

        args = argList.ToArray();

        if (args.Length > 0 && args[0].ToLowerInvariant() == "warmup" && args.Length >= 2)
        {
            RunWarmup(args[1]);
            return;
        }

        if (args.Length < 4)
        {
            Console.WriteLine("""
            Usage:
              FastFileHasher scan   <rootPath> <dbPath> <phase> [threads] [--in-memory]
              FastFileHasher verify <rootPath> <dbPath> <phase> [threads] [--in-memory]
              FastFileHasher export <outputDir> <dbPath> <phase>
              FastFileHasher copy   <srcRoot> <destRoot> <dbPath> <srcPhase> <destPhase> [threads] [--use-robocopy] [--in-memory]
              FastFileHasher migrate <srcRoot> <destRoot> <dbPath> <srcPhase> <destPhase> [threads] [--use-robocopy] [--in-memory]
              FastFileHasher warmup <dbPath>

            Phases are typically: source | dest
            """);
            return;
        }

        string mode = args[0].ToLowerInvariant();

        if (mode == "scan" || mode == "verify")
        {
            string root = NormalizePath(args[1]);
            string dbPath = args[2];
            string phase = args[3];
            int threads = args.Length >= 5 ? int.Parse(args[4]) : Environment.ProcessorCount;

            RunScan(root, dbPath, phase, threads, inMemory);

            if (mode == "verify")
                RunVerifyReport(root, dbPath, phase);
        }
        else if (mode == "copy" || mode == "migrate")
        {
            if (args.Length < 6)
            {
                Console.WriteLine("Missing arguments for copy/migrate.");
                return;
            }
            string srcRoot = NormalizePath(args[1]);
            string destRoot = NormalizePath(args[2]);
            string dbPath = args[3];
            string srcPhase = args[4];
            string destPhase = args[5];
            int threads = args.Length >= 7 ? int.Parse(args[6]) : 0;
            bool isMigrate = mode == "migrate";

            RunMigrate(srcRoot, destRoot, dbPath, srcPhase, destPhase, threads, inMemory, useRobocopy, isMigrate);
        }
        else if (mode == "export")
        {
            string outputDir = args[1];
            string dbPath = args[2];
            string phase = args[3];

            ExportCsv(outputDir, dbPath, phase);
        }
        else
        {
            Console.Error.WriteLine("Invalid mode.");
        }
    }

    static void RunScan(string root, string dbPath, string phase, int threads, bool inMemory)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(dbPath)) ?? ".");
        var queue = new BlockingCollection<object>(boundedCapacity: 10000);

        var writerTask = Task.Run(() => DbWriter(queue, dbPath, inMemory));

        Parallel.ForEach(
            EnumerateFiles(root),
            new ParallelOptions { MaxDegreeOfParallelism = threads },
            file =>
            {
                try
                {
                    var (size, hash) = ComputeSha256AndSize(file);
                    queue.Add(new FileRow(
                        file,
                        NormalizeFolder(Path.GetRelativePath(root, Path.GetDirectoryName(file)!)),
                        Path.GetFileName(file),
                        size,
                        hash,
                        phase
                    ));
                }
                catch (Exception ex)
                {
                    queue.Add(new FileRow(
                        file,
                        NormalizeFolder(Path.GetRelativePath(root, Path.GetDirectoryName(file)!)),
                        Path.GetFileName(file),
                        0,
                        $"ERROR:{ex.GetType().Name}",
                        phase
                    ));
                }
            });

        queue.CompleteAdding();
        int processed = writerTask.Result;
        Console.WriteLine($"Scan phase '{phase}' complete. Total files processed: {processed}");
    }

    static void RunMigrate(string srcRoot, string destRoot, string dbPath, string srcPhase, string destPhase, int userThreads, bool inMemory, bool useRobocopy, bool isMigrate)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(dbPath)) ?? ".");
        Directory.CreateDirectory(destRoot);

        int threads = userThreads;
        if (threads <= 0)
        {
            bool isUnc = destRoot.StartsWith(@"\\?\UNC\") || destRoot.StartsWith(@"\\");
            threads = isUnc ? Math.Min(8, Environment.ProcessorCount) : Math.Min(32, Environment.ProcessorCount * 2);
        }

        Console.WriteLine($"Starting {(isMigrate ? "migration" : "copy")} engine. Threads: {threads}, Robocopy: {useRobocopy}");

        // Initialize schema synchronously so the scanner and workers can immediately see tables.
        InitSchema(dbPath);

        // Reset stale COPYING states left by a previous crash/abrupt stop.
        using (var rc = new SqliteConnection($"Data Source={dbPath}"))
        {
            rc.Open();
            using var c = rc.CreateCommand();
            c.CommandText = """
                UPDATE file_copy
                SET state='PENDING', batch_id=NULL, batch_claimed_at=NULL, updated_at=$t
                WHERE state='COPYING' AND (batch_claimed_at IS NULL OR batch_claimed_at < $staleBefore)
            """;
            long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            c.Parameters.AddWithValue("$t", now);
            c.Parameters.AddWithValue("$staleBefore", now - STALE_BATCH_SECONDS);
            c.ExecuteNonQuery();
        }

        // dbOps: all DB writes go through a single serial writer to avoid concurrent-write contention.
        var dbOps = new BlockingCollection<object>(boundedCapacity: 50000);
        // copyQueue / verifyQueue: in-process handoff between stages (no DB round-trip needed for new files).
        var copyQueue  = new BlockingCollection<CopyJob>(boundedCapacity: 10000);
        var verifyQueue = new BlockingCollection<VerifyJob>(boundedCapacity: 10000);

        var writerTask = Task.Run(() => DbWriter(dbOps, dbPath, inMemory));

        // ── Scanner ──────────────────────────────────────────────────────────
        // Hashes every source file and feeds directly into copyQueue (streaming).
        // Also writes file_hashes rows and file_copy PENDING rows for durability.
        var scannerTask = Task.Run(() =>
        {
            // Recover any PENDING files from a previous interrupted run first.
            int resumePending = 0;
            using (var rc = new SqliteConnection($"Data Source={dbPath}"))
            {
                rc.Open();
                using var cmd = rc.CreateCommand();
                cmd.CommandText = "SELECT COUNT(*) FROM file_copy WHERE state='PENDING'";
                resumePending = Convert.ToInt32(cmd.ExecuteScalar() ?? 0);
            }
            if (!useRobocopy)
            {
                List<CopyJob> resumeJobs = LoadPendingCopyJobs(dbPath, srcPhase);
                foreach (var j in resumeJobs)
                    copyQueue.Add(j);
            }
            if (resumePending > 0)
                Console.WriteLine($"Resuming {resumePending} pending file(s) from previous run.");

            // Also recover any COPIED-but-not-verified files.
            List<VerifyJob> resumeVerify = new();
            using (var rc = new SqliteConnection($"Data Source={dbPath}"))
            {
                rc.Open();
                using var cmd = rc.CreateCommand();
                cmd.CommandText = @"
                    SELECT fc.src_path, fc.dest_path, h.sha256
                    FROM file_copy fc
                    JOIN file_hashes h ON fc.src_path = h.path AND h.phase = $ph
                    WHERE fc.state = 'COPIED'";
                cmd.Parameters.AddWithValue("$ph", srcPhase);
                using var rdr = cmd.ExecuteReader();
                while (rdr.Read())
                    resumeVerify.Add(new VerifyJob(rdr.GetString(0), rdr.GetString(1), rdr.GetString(2)));
            }
            foreach (var j in resumeVerify)
                verifyQueue.Add(j);

            // Now enumerate + hash new source files.
            var alreadyKnown = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            using (var rc = new SqliteConnection($"Data Source={dbPath}"))
            {
                rc.Open();
                using var cmd = rc.CreateCommand();
                cmd.CommandText = "SELECT src_path FROM file_copy";
                using var rdr = cmd.ExecuteReader();
                while (rdr.Read()) alreadyKnown.Add(rdr.GetString(0));
            }

            Parallel.ForEach(
                EnumerateFiles(srcRoot),
                new ParallelOptions { MaxDegreeOfParallelism = threads },
                file =>
                {
                    try
                    {
                        var (size, hash) = ComputeSha256AndSize(file);
                        string folder = NormalizeFolder(Path.GetRelativePath(srcRoot, Path.GetDirectoryName(file)!));
                        string filename = Path.GetFileName(file);
                        dbOps.Add(new FileRow(file, folder, filename, size, hash, srcPhase));

                        if (!alreadyKnown.Contains(file))
                        {
                            string destP = NormalizePath(Path.Combine(destRoot, folder, filename));
                            dbOps.Add(new CopyInsert(file, destP));
                            if (!useRobocopy)
                                copyQueue.Add(new CopyJob(file, destP, hash, size));
                        }
                    }
                    catch (Exception ex)
                    {
                        dbOps.Add(new FileRow(
                            file,
                            NormalizeFolder(Path.GetRelativePath(srcRoot, Path.GetDirectoryName(file)!)),
                            Path.GetFileName(file), 0, $"ERROR:{ex.GetType().Name}", srcPhase));
                    }
                });

            if (!useRobocopy)
                copyQueue.CompleteAdding();
            Console.WriteLine("Scanner complete.");
        });

        Task[] copyWorkers;
        if (useRobocopy)
        {
            copyWorkers = new[] { Task.Run(() =>
            {
                scannerTask.Wait();
                RunRobocopyBatches(dbPath, srcPhase, threads, dbOps, verifyQueue);
            }) };
        }
        else
        {
            copyWorkers = Enumerable.Range(0, threads).Select(_ => Task.Run(() =>
            {
                foreach (var job in copyQueue.GetConsumingEnumerable())
                {
                    dbOps.Add(new CopyStateUpdate(job.SrcPath, "COPYING", ""));
                    try
                    {
                        string destDir = Path.GetDirectoryName(job.DestPath)!;
                        Directory.CreateDirectory(destDir);
                        string tempDest = Path.Combine(destDir, Guid.NewGuid().ToString("N") + ".tmp");
                        File.Copy(job.SrcPath, tempDest, overwrite: false);
                        File.Move(tempDest, job.DestPath, overwrite: true);
                        dbOps.Add(new CopyStateUpdate(job.SrcPath, "COPIED", ""));
                        verifyQueue.Add(new VerifyJob(job.SrcPath, job.DestPath, job.SrcSha256));
                    }
                    catch (Exception ex)
                    {
                        dbOps.Add(new CopyStateUpdate(job.SrcPath, "FAILED", ex.Message));
                    }
                }
            })).ToArray();
        }

        // ── Verify Workers ───────────────────────────────────────────────────
        var verifyWorkers = Enumerable.Range(0, threads).Select(_ => Task.Run(() =>
        {
            foreach (var job in verifyQueue.GetConsumingEnumerable())
            {
                try
                {
                    var (size, destHash) = ComputeSha256AndSize(job.DestPath);
                    string destFolder = NormalizeFolder(Path.GetRelativePath(destRoot, Path.GetDirectoryName(job.DestPath)!));
                    dbOps.Add(new FileRow(job.DestPath, destFolder, Path.GetFileName(job.DestPath), size, destHash, destPhase));

                    if (destHash == job.SrcSha256)
                    {
                        dbOps.Add(new CopyVerified(job.SrcPath));
                        if (isMigrate) try { File.Delete(job.SrcPath); } catch { }
                    }
                    else
                        dbOps.Add(new CopyStateUpdate(job.SrcPath, "FAILED", "Hash mismatch after copy"));
                }
                catch (Exception ex)
                {
                    dbOps.Add(new CopyStateUpdate(job.SrcPath, "FAILED", $"Verification failed: {ex.Message}"));
                }
            }
        })).ToArray();

        // Wait for all stages to complete in pipeline order.
        scannerTask.Wait();
        Task.WaitAll(copyWorkers);   // all copies done
        verifyQueue.CompleteAdding();
        Task.WaitAll(verifyWorkers); // all verifications done
        dbOps.CompleteAdding();
        writerTask.Wait();           // all DB writes flushed

        Console.WriteLine("Engine run complete. Generating Verification Report...");
        int issues = RunVerifyReport(destRoot, dbPath, destPhase, srcPhase);

        using (var cc = new SqliteConnection($"Data Source={dbPath}"))
        {
            cc.Open();
            using var cmd = cc.CreateCommand();
            cmd.CommandText = """
                SELECT
                    SUM(CASE WHEN state IN ('PENDING','COPYING') THEN 1 ELSE 0 END) AS active_count,
                    SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
                    SUM(CASE WHEN state = 'COPIED' AND verified_at IS NULL THEN 1 ELSE 0 END) AS unverified_count
                FROM file_copy
            """;
            using var rdr = cmd.ExecuteReader();
            rdr.Read();
            long active = rdr.IsDBNull(0) ? 0 : rdr.GetInt64(0);
            long failed = rdr.IsDBNull(1) ? 0 : rdr.GetInt64(1);
            long unverified = rdr.IsDBNull(2) ? 0 : rdr.GetInt64(2);

            if (active > 0 || failed > 0 || unverified > 0 || issues > 0)
            {
                Console.Error.WriteLine($"Migration incomplete. Active={active}, Failed={failed}, Unverified={unverified}, VerificationIssues={issues}");
                Environment.ExitCode = 1;
            }
        }
    }

    // Initialises the DB schema synchronously. Safe to call multiple times (IF NOT EXISTS).
    static void InitSchema(string dbPath)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(dbPath)) ?? ".");
        using var conn = new SqliteConnection($"Data Source={dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA temp_store=MEMORY;
            PRAGMA mmap_size=2000000000;

            CREATE TABLE IF NOT EXISTS file_hashes (
                path       TEXT NOT NULL,
                folder     TEXT NOT NULL,
                filename   TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                sha256     TEXT NOT NULL,
                phase      TEXT NOT NULL,
                scanned_at INTEGER NOT NULL,
                PRIMARY KEY (path, phase)
            );
            CREATE INDEX IF NOT EXISTS idx_folder_phase ON file_hashes(folder, phase);

            CREATE TABLE IF NOT EXISTS file_copy (
                path        TEXT PRIMARY KEY,
                src_path    TEXT NOT NULL,
                dest_path   TEXT NOT NULL,
                state       TEXT NOT NULL,
                last_error  TEXT,
                updated_at  INTEGER NOT NULL,
                verified_at INTEGER,
                batch_id TEXT,
                batch_claimed_at INTEGER,
                batch_attempt INTEGER NOT NULL DEFAULT 0
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_file_copy_src_path ON file_copy(src_path);
            CREATE INDEX IF NOT EXISTS idx_file_copy_state ON file_copy(state);
            CREATE INDEX IF NOT EXISTS idx_file_copy_batch_id ON file_copy(batch_id);
        """;
        cmd.ExecuteNonQuery();

        EnsureFileCopySchema(conn);
    }

    static void EnsureFileCopySchema(SqliteConnection conn)
    {
        var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        string pkColumn = "";
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "PRAGMA table_info(file_copy)";
            using var rdr = cmd.ExecuteReader();
            while (rdr.Read())
            {
                string name = rdr.GetString(1);
                columns.Add(name);
                int pk = rdr.GetInt32(5);
                if (pk == 1)
                    pkColumn = name;
            }
        }

        bool needsMigration =
            !columns.Contains("path") ||
            !columns.Contains("src_path") ||
            !columns.Contains("dest_path") ||
            !columns.Contains("state") ||
            !columns.Contains("last_error") ||
            !columns.Contains("updated_at") ||
            !columns.Contains("verified_at") ||
            !columns.Contains("batch_id") ||
            !columns.Contains("batch_claimed_at") ||
            !columns.Contains("batch_attempt") ||
            !pkColumn.Equals("path", StringComparison.OrdinalIgnoreCase);

        if (!needsMigration)
            return;

        string pathExpr = columns.Contains("path") ? "path" : "src_path";
        string srcPathExpr = columns.Contains("src_path") ? "src_path" : "path";
        string verifiedAtExpr = columns.Contains("verified_at") ? "verified_at" : "NULL";
        string batchIdExpr = columns.Contains("batch_id") ? "batch_id" : "NULL";
        string batchClaimedExpr = columns.Contains("batch_claimed_at") ? "batch_claimed_at" : "NULL";
        string batchAttemptExpr = columns.Contains("batch_attempt") ? "batch_attempt" : "0";

        using var tx = conn.BeginTransaction();
        using var migrateCmd = conn.CreateCommand();
        migrateCmd.Transaction = tx;
        migrateCmd.CommandText = $"""
            CREATE TABLE file_copy_new (
                path        TEXT PRIMARY KEY,
                src_path    TEXT NOT NULL,
                dest_path   TEXT NOT NULL,
                state       TEXT NOT NULL,
                last_error  TEXT,
                updated_at  INTEGER NOT NULL,
                verified_at INTEGER,
                batch_id TEXT,
                batch_claimed_at INTEGER,
                batch_attempt INTEGER NOT NULL DEFAULT 0
            );

            INSERT INTO file_copy_new(path, src_path, dest_path, state, last_error, updated_at, verified_at, batch_id, batch_claimed_at, batch_attempt)
            SELECT
                {pathExpr},
                {srcPathExpr},
                dest_path,
                state,
                last_error,
                updated_at,
                {verifiedAtExpr},
                {batchIdExpr},
                {batchClaimedExpr},
                {batchAttemptExpr}
            FROM file_copy;

            DROP TABLE file_copy;
            ALTER TABLE file_copy_new RENAME TO file_copy;
            CREATE UNIQUE INDEX IF NOT EXISTS idx_file_copy_src_path ON file_copy(src_path);
            CREATE INDEX IF NOT EXISTS idx_file_copy_state ON file_copy(state);
            CREATE INDEX IF NOT EXISTS idx_file_copy_batch_id ON file_copy(batch_id);
        """;
        migrateCmd.ExecuteNonQuery();
        tx.Commit();
    }

    static int DbWriter(BlockingCollection<object> queue, string dbPath, bool inMemory)
    {
        using SqliteConnection workConn = inMemory
            ? new SqliteConnection($"Data Source={Guid.NewGuid()};Mode=Memory;Cache=Shared")
            : new SqliteConnection($"Data Source={dbPath}");

        workConn.Open();

        if (inMemory && File.Exists(dbPath))
        {
            using var tempDiskConn = new SqliteConnection($"Data Source={dbPath}");
            tempDiskConn.Open();
            tempDiskConn.BackupDatabase(workConn);
        }

        // Schema already initialised by InitSchema() before this task starts.
        using var pragmas = workConn.CreateCommand();
        pragmas.CommandText = """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA temp_store=MEMORY;
            PRAGMA mmap_size=2000000000;
        """;
        pragmas.ExecuteNonQuery();

        var tx = workConn.BeginTransaction();
        var cmdHash = workConn.CreateCommand();
        cmdHash.CommandText = """
            INSERT OR REPLACE INTO file_hashes
            (path, folder, filename, size_bytes, sha256, phase, scanned_at)
            VALUES ($p,$f,$n,$s,$h,$ph,$t)
        """;

        var cmdInsertCopy = workConn.CreateCommand();
        cmdInsertCopy.CommandText = """
            INSERT OR IGNORE INTO file_copy
            (path, src_path, dest_path, state, updated_at, verified_at, batch_id, batch_claimed_at, batch_attempt)
            VALUES ($p,$sp,$dp,'PENDING',$t,NULL,NULL,NULL,0)
        """;

        var cmdUpdateCopy = workConn.CreateCommand();
        cmdUpdateCopy.CommandText = """
            UPDATE file_copy
            SET
                state=$st,
                last_error=$err,
                updated_at=$t,
                verified_at=CASE WHEN $st='COPIED' THEN NULL ELSE verified_at END,
                batch_id=CASE WHEN $st IN ('COPIED','FAILED') THEN NULL ELSE batch_id END,
                batch_claimed_at=CASE WHEN $st IN ('COPIED','FAILED') THEN NULL ELSE batch_claimed_at END
            WHERE src_path=$sp
        """;

        var cmdMarkVerified = workConn.CreateCommand();
        cmdMarkVerified.CommandText = """
            UPDATE file_copy
            SET verified_at=$t, last_error=NULL, updated_at=$t
            WHERE src_path=$sp
        """;

        int count = 0;

        try
        {
            bool more = true;
            while (more)
            {
                if (queue.TryTake(out var msg, 50))
                {
                    if (msg is FileRow row)
                    {
                        cmdHash.Parameters.Clear();
                        cmdHash.Parameters.AddWithValue("$p", row.Path);
                        cmdHash.Parameters.AddWithValue("$f", row.Folder);
                        cmdHash.Parameters.AddWithValue("$n", row.Filename);
                        cmdHash.Parameters.AddWithValue("$s", row.SizeBytes);
                        cmdHash.Parameters.AddWithValue("$h", row.Sha256);
                        cmdHash.Parameters.AddWithValue("$ph", row.Phase);
                        cmdHash.Parameters.AddWithValue("$t", DateTimeOffset.UtcNow.ToUnixTimeSeconds());
                        cmdHash.ExecuteNonQuery();
                    }
                    else if (msg is CopyInsert ci)
                    {
                        cmdInsertCopy.Parameters.Clear();
                        cmdInsertCopy.Parameters.AddWithValue("$p", ci.SrcPath);
                        cmdInsertCopy.Parameters.AddWithValue("$sp", ci.SrcPath);
                        cmdInsertCopy.Parameters.AddWithValue("$dp", ci.DestPath);
                        cmdInsertCopy.Parameters.AddWithValue("$t", DateTimeOffset.UtcNow.ToUnixTimeSeconds());
                        cmdInsertCopy.ExecuteNonQuery();
                    }
                    else if (msg is CopyStateUpdate cu)
                    {
                        cmdUpdateCopy.Parameters.Clear();
                        cmdUpdateCopy.Parameters.AddWithValue("$st", cu.State);
                        cmdUpdateCopy.Parameters.AddWithValue("$err", cu.LastError ?? (object)DBNull.Value);
                        cmdUpdateCopy.Parameters.AddWithValue("$t", DateTimeOffset.UtcNow.ToUnixTimeSeconds());
                        cmdUpdateCopy.Parameters.AddWithValue("$sp", cu.SrcPath);
                        cmdUpdateCopy.ExecuteNonQuery();
                    }
                    else if (msg is CopyVerified cv)
                    {
                        cmdMarkVerified.Parameters.Clear();
                        cmdMarkVerified.Parameters.AddWithValue("$sp", cv.SrcPath);
                        cmdMarkVerified.Parameters.AddWithValue("$t", DateTimeOffset.UtcNow.ToUnixTimeSeconds());
                        cmdMarkVerified.ExecuteNonQuery();
                    }

                    count++;
                    if (count % DEFAULT_BATCH_SIZE == 0)
                    {
                        tx.Commit();
                        tx.Dispose();
                        tx = workConn.BeginTransaction();
                        cmdHash.Transaction = tx;
                        cmdInsertCopy.Transaction = tx;
                        cmdUpdateCopy.Transaction = tx;
                        cmdMarkVerified.Transaction = tx;
                    }
                }
                else
                {
                    if (queue.IsCompleted)
                        more = false;

                    if (count > 0)
                    {
                        // Commit partial batch so readers can see fresh data
                        tx.Commit();
                        tx.Dispose();
                        tx = workConn.BeginTransaction();
                        cmdHash.Transaction = tx;
                        cmdInsertCopy.Transaction = tx;
                        cmdUpdateCopy.Transaction = tx;
                        cmdMarkVerified.Transaction = tx;
                        count = 0;
                    }
                }
            }
            tx.Commit();
        }
        finally
        {
            tx?.Dispose();
        }

        if (inMemory)
        {
            Console.WriteLine("Flushing in-memory database to disk...");
            using var diskConnOut = new SqliteConnection($"Data Source={dbPath}");
            diskConnOut.Open();
            workConn.BackupDatabase(diskConnOut);
        }

        return count;
    }

    static void ExportCsv(string outputRoot, string dbPath, string phase)
    {
        Directory.CreateDirectory(outputRoot);

        using var conn = new SqliteConnection($"Data Source={dbPath}");
        conn.Open();

        var folders = new List<string>();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT DISTINCT folder FROM file_hashes WHERE phase=$p";
            cmd.Parameters.AddWithValue("$p", phase);
            using var rdr = cmd.ExecuteReader();
            while (rdr.Read())
                folders.Add(rdr.GetString(0));
        }

        int totalExported = 0;
        foreach (var folder in folders)
        {
            string outDir = Path.Combine(outputRoot, folder);
            Directory.CreateDirectory(outDir);

            string csvPath = Path.Combine(outDir, "folder_hashes.csv");
            using var sw = new StreamWriter(csvPath, false, Encoding.UTF8);
            sw.WriteLine("Filename,SHA256");

            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                SELECT filename, sha256
                FROM file_hashes
                WHERE folder=$f AND phase=$p
            """;
            cmd.Parameters.AddWithValue("$f", folder);
            cmd.Parameters.AddWithValue("$p", phase);

            using var rdr = cmd.ExecuteReader();
            while (rdr.Read())
            {
                totalExported++;
                sw.WriteLine($"{Csv(rdr.GetString(0))},{rdr.GetString(1)}");
            }
        }
        Console.WriteLine($"Export complete. Total files exported: {totalExported}");
    }

    static IEnumerable<string> EnumerateFiles(string root)
    {
        var stack = new Stack<string>();
        stack.Push(root);

        while (stack.Count > 0)
        {
            var dir = stack.Pop();
            string[] subs;
            try
            {
                subs = Directory.GetDirectories(dir);
            }
            catch
            {
                continue;
            }

            foreach (var s in subs)
                stack.Push(s);

            IEnumerable<string> files;
            try
            {
                files = Directory.EnumerateFiles(dir);
            }
            catch
            {
                continue;
            }

            IEnumerator<string> enumerator;
            try
            {
                enumerator = files.GetEnumerator();
            }
            catch
            {
                continue;
            }

            using (enumerator)
            {
                while (true)
                {
                    string f;
                    try
                    {
                        if (!enumerator.MoveNext())
                            break;
                        f = enumerator.Current;
                    }
                    catch
                    {
                        break;
                    }
                    yield return NormalizePath(f);
                }
            }
        }
    }

    static (long Size, string Hash) ComputeSha256AndSize(string path)
    {
        using var fs = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            1 << 20,
            FileOptions.SequentialScan);

        long size = fs.Length;
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        byte[] buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(1 << 20);
        try
        {
            int read;
            while ((read = fs.Read(buffer, 0, buffer.Length)) > 0)
                hasher.AppendData(buffer, 0, read);
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
        }

        return (size, Convert.ToHexString(hasher.GetHashAndReset()).ToLowerInvariant());
    }

    static string NormalizePath(string path)
    {
        path = Path.GetFullPath(path);
        if (path.StartsWith(@"\\?\"))
            return path;
        if (path.StartsWith(@"\\"))
            return @"\\?\UNC\" + path.Substring(2);
        return @"\\?\" + path;
    }

    static string StripExtendedPrefix(string path)
    {
        if (path.StartsWith(@"\\?\UNC\"))
            return @"\\" + path.Substring(8);
        if (path.StartsWith(@"\\?\"))
            return path.Substring(4);
        return path;
    }

    // ".", which Path.GetRelativePath returns for the root itself, is normalised to "" so that
    // folder comparisons between source and dest phases always match.
    static string NormalizeFolder(string folder) => folder == "." ? "" : folder;

    static string Csv(string v)
    {
        return v.Contains(',') || v.Contains('"')
            ? $"\"{v.Replace("\"", "\"\"")}\""
            : v;
    }

    static int RunVerifyReport(string destRoot, string dbPath, string destPhase, string? knownSrcPhase = null)
    {
        using var conn = new SqliteConnection($"Data Source={dbPath}");
        conn.Open();

        string srcPhase;
        if (knownSrcPhase != null)
        {
            srcPhase = knownSrcPhase;
        }
        else
        {
            using var cmdSrc = conn.CreateCommand();
            cmdSrc.CommandText = "SELECT DISTINCT phase FROM file_hashes WHERE phase != $p LIMIT 1";
            cmdSrc.Parameters.AddWithValue("$p", destPhase);
            var srcObj = cmdSrc.ExecuteScalar();
            if (srcObj == null || srcObj is DBNull)
            {
                Console.WriteLine("No source phase found in DB to compare against.");
                return 0;
            }
            srcPhase = (string)srcObj;
        }

        string outCsv = Path.Combine(Path.GetDirectoryName(Path.GetFullPath(dbPath)) ?? ".", "verification_issues.csv");
        int issues = 0;

        using (var sw = new StreamWriter(outCsv, false, Encoding.UTF8))
        {
            sw.WriteLine("Path,Status,SourceSHA256,DestSHA256,LastError");

            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                SELECT 
                    fc.src_path AS reported_path,
                    s.sha256 AS src_hash,
                    d.sha256 AS dst_hash,
                    fc.last_error,
                    fc.state,
                    fc.verified_at
                FROM file_copy fc
                LEFT JOIN file_hashes s
                    ON s.path = fc.src_path AND s.phase = $src
                LEFT JOIN file_hashes d
                    ON d.path = fc.dest_path AND d.phase = $dst
                WHERE
                    s.sha256 IS NULL
                    OR d.sha256 IS NULL
                    OR s.sha256 != d.sha256
                    OR fc.state = 'FAILED'
                    OR fc.verified_at IS NULL
            """;
            cmd.Parameters.AddWithValue("$src", srcPhase);
            cmd.Parameters.AddWithValue("$dst", destPhase);

            using var rdr = cmd.ExecuteReader();
            while (rdr.Read())
            {
                issues++;
                string reportedPath = rdr.GetString(0);
                string srcHash = rdr.IsDBNull(1) ? "" : rdr.GetString(1);
                string dstHash = rdr.IsDBNull(2) ? "" : rdr.GetString(2);
                string lastErr = rdr.IsDBNull(3) ? "" : rdr.GetString(3);
                string state = rdr.IsDBNull(4) ? "" : rdr.GetString(4);
                bool verified = !rdr.IsDBNull(5);

                string status;
                if (!string.IsNullOrEmpty(lastErr)) status = $"FAILED: {lastErr}";
                else if (state == "FAILED") status = "FAILED";
                else if (!verified) status = "UNVERIFIED";
                else if (string.IsNullOrEmpty(srcHash)) status = "EXTRA";
                else if (string.IsNullOrEmpty(dstHash)) status = "MISSING";
                else status = "MISMATCH";

                sw.WriteLine($"{Csv(reportedPath)},{Csv(status)},{srcHash},{dstHash},{Csv(lastErr)}");
            }
        }

        if (issues == 0)
        {
            File.Delete(outCsv);
            Console.WriteLine("Verification passed. No mismatches, missing, or extra files.");
        }
        else
        {
            Console.WriteLine($"Verification complete. Found {issues} issues. Report saved to {outCsv}");
        }
        return issues;
    }

    static void RunWarmup(string dbPath)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(dbPath)) ?? ".");
        using var conn = new SqliteConnection($"Data Source={dbPath}");
        conn.Open();

        using var setup = conn.CreateCommand();
        setup.CommandText = """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA temp_store=MEMORY;
            PRAGMA mmap_size=2000000000;
            
            CREATE TABLE IF NOT EXISTS warmup (
                id INTEGER PRIMARY KEY,
                data TEXT NOT NULL
            );
        """;
        setup.ExecuteNonQuery();

        var tx = conn.BeginTransaction();
        var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = "INSERT INTO warmup (data) VALUES ($d)";

        try
        {
            for (int i = 0; i < 50000; i++)
            {
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("$d", "DUMMY_DATA_FOR_ANTIVIRUS_WARMUP_" + Guid.NewGuid().ToString());
                cmd.ExecuteNonQuery();
            }
            tx.Commit();
        }
        finally
        {
            tx?.Dispose();
        }

        Console.WriteLine($"Warmup complete for database '{dbPath}'. Inserted 50000 dummy rows to trigger AV scans.");
    }

    static List<CopyJob> LoadPendingCopyJobs(string dbPath, string srcPhase)
    {
        List<CopyJob> jobs = new();
        using var conn = new SqliteConnection($"Data Source={dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT fc.src_path, fc.dest_path, h.sha256, h.size_bytes
            FROM file_copy fc
            JOIN file_hashes h ON fc.src_path = h.path AND h.phase = $ph
            WHERE fc.state = 'PENDING'
        """;
        cmd.Parameters.AddWithValue("$ph", srcPhase);
        using var rdr = cmd.ExecuteReader();
        while (rdr.Read())
            jobs.Add(new CopyJob(rdr.GetString(0), rdr.GetString(1), rdr.GetString(2), rdr.GetInt64(3)));
        return jobs;
    }

    static void RunRobocopyBatches(string dbPath, string srcPhase, int threads, BlockingCollection<object> dbOps, BlockingCollection<VerifyJob> verifyQueue)
    {
        while (true)
        {
            var planned = LoadPendingCopyJobs(dbPath, srcPhase);
            if (planned.Count == 0)
                break;

            var grouped = planned
                .GroupBy(j => (SrcDir: Path.GetDirectoryName(j.SrcPath)!, DestDir: Path.GetDirectoryName(j.DestPath)!))
                .ToList();

            bool anyClaimed = false;
            foreach (var group in grouped)
            {
                var chunk = new List<CopyJob>(ROBOCOPY_FILE_BATCH_SIZE);
                long chunkBytes = 0;
                foreach (var job in group)
                {
                    long nextSize = Math.Max(1, job.SizeBytes);
                    bool shouldFlushBeforeAdd =
                        chunk.Count > 0 &&
                        (chunk.Count >= ROBOCOPY_FILE_BATCH_SIZE || chunkBytes + nextSize > ROBOCOPY_TARGET_BATCH_BYTES);

                    if (shouldFlushBeforeAdd)
                    {
                        if (TryRunRobocopyBatch(dbPath, threads, dbOps, verifyQueue, group.Key.SrcDir, group.Key.DestDir, chunk))
                            anyClaimed = true;
                        chunk = new List<CopyJob>(ROBOCOPY_FILE_BATCH_SIZE);
                        chunkBytes = 0;
                    }

                    chunk.Add(job);
                    chunkBytes += nextSize;
                    if (chunk.Count >= ROBOCOPY_FILE_BATCH_SIZE || chunkBytes >= ROBOCOPY_TARGET_BATCH_BYTES)
                    {
                        if (TryRunRobocopyBatch(dbPath, threads, dbOps, verifyQueue, group.Key.SrcDir, group.Key.DestDir, chunk))
                            anyClaimed = true;
                        chunk = new List<CopyJob>(ROBOCOPY_FILE_BATCH_SIZE);
                        chunkBytes = 0;
                    }
                }
                if (chunk.Count > 0)
                {
                    if (TryRunRobocopyBatch(dbPath, threads, dbOps, verifyQueue, group.Key.SrcDir, group.Key.DestDir, chunk))
                        anyClaimed = true;
                }
            }

            if (!anyClaimed)
                break;
        }
    }

    static bool TryRunRobocopyBatch(string dbPath, int threads, BlockingCollection<object> dbOps, BlockingCollection<VerifyJob> verifyQueue, string srcDirPath, string destDirPath, List<CopyJob> chunk)
    {
        if (chunk.Count == 0)
            return false;

        string batchId = Guid.NewGuid().ToString("N");
        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        List<CopyJob> claimed = new();

        using (var conn = new SqliteConnection($"Data Source={dbPath}"))
        {
            conn.Open();
            using var tx = conn.BeginTransaction();
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;

            foreach (var job in chunk)
            {
                cmd.Parameters.Clear();
                cmd.CommandText = """
                    UPDATE file_copy
                    SET state='COPYING', batch_id=$bid, batch_claimed_at=$t, batch_attempt=batch_attempt+1, updated_at=$t
                    WHERE src_path=$sp AND state='PENDING'
                """;
                cmd.Parameters.AddWithValue("$bid", batchId);
                cmd.Parameters.AddWithValue("$t", now);
                cmd.Parameters.AddWithValue("$sp", job.SrcPath);
                int changed = cmd.ExecuteNonQuery();
                if (changed > 0)
                    claimed.Add(job);
            }
            tx.Commit();
        }

        if (claimed.Count == 0)
            return false;

        string sDir = StripExtendedPrefix(srcDirPath);
        string dDir = StripExtendedPrefix(destDirPath);
        Directory.CreateDirectory(dDir);

        string fileArgs = string.Join(' ', claimed.Select(j => $"\"{Path.GetFileName(j.SrcPath)}\""));
        var psi = new ProcessStartInfo
        {
            FileName = "robocopy",
            Arguments = $"\"{sDir}\" \"{dDir}\" {fileArgs} /Z /B /COPYALL /R:1 /W:1 /MT:{threads} /NFL /NDL /NJH /NJS",
            CreateNoWindow = true,
            UseShellExecute = false
        };

        string batchError = "";
        try
        {
            using var proc = Process.Start(psi);
            if (proc != null)
            {
                proc.WaitForExit();
                if (proc.ExitCode >= 8)
                    batchError = $"Robocopy exit code {proc.ExitCode}";
            }
            else
            {
                batchError = "Robocopy process failed to start";
            }
        }
        catch (Exception ex)
        {
            batchError = ex.Message;
        }

        foreach (var job in claimed)
        {
            if (File.Exists(job.DestPath))
            {
                dbOps.Add(new CopyStateUpdate(job.SrcPath, "COPIED", ""));
                verifyQueue.Add(new VerifyJob(job.SrcPath, job.DestPath, job.SrcSha256));
            }
            else
            {
                string err = string.IsNullOrWhiteSpace(batchError)
                    ? $"Batch {batchId}: destination file missing after robocopy"
                    : $"Batch {batchId}: {batchError}";
                dbOps.Add(new CopyStateUpdate(job.SrcPath, "FAILED", err));
            }
        }

        return true;
    }
}

record FileRow(string Path, string Folder, string Filename, long SizeBytes, string Sha256, string Phase);
record CopyStateUpdate(string SrcPath, string State, string LastError);
record CopyVerified(string SrcPath);
record CopyInsert(string SrcPath, string DestPath);
record CopyJob(string SrcPath, string DestPath, string SrcSha256, long SizeBytes);
record VerifyJob(string SrcPath, string DestPath, string SrcSha256);