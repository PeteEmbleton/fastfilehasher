using Microsoft.Data.Sqlite;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

const int DEFAULT_BATCH_SIZE = 5000;

bool inMemory = false;
var argList = args.ToList();
if (argList.Contains("--in-memory"))
{
    inMemory = true;
    argList.Remove("--in-memory");
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
    {
        RunVerifyReport(root, dbPath, phase);
    }
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

static void RunScan(string root, string dbPath, string phase, int threads, bool inMemory)
{
    Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(dbPath)) ?? ".");
    var queue = new BlockingCollection<FileRow>(boundedCapacity: 10000);

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
                    Path.GetRelativePath(root, Path.GetDirectoryName(file)!),
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
                    Path.GetRelativePath(root, Path.GetDirectoryName(file)!),
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

static int DbWriter(BlockingCollection<FileRow> queue, string dbPath, bool inMemory)
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

    using var setup = workConn.CreateCommand();
    setup.CommandText = """
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;
    PRAGMA temp_store=MEMORY;
    PRAGMA mmap_size=2000000000;

    CREATE TABLE IF NOT EXISTS file_hashes (
        path TEXT NOT NULL,
        folder TEXT NOT NULL,
        filename TEXT NOT NULL,
        size_bytes INTEGER NOT NULL,
        sha256 TEXT NOT NULL,
        phase TEXT NOT NULL,
        scanned_at INTEGER NOT NULL,
        PRIMARY KEY (path, phase)
    );

    CREATE INDEX IF NOT EXISTS idx_folder_phase ON file_hashes(folder, phase);
    """;
    setup.ExecuteNonQuery();

    var tx = workConn.BeginTransaction();
    var cmd = workConn.CreateCommand();
    cmd.Transaction = tx;
    cmd.CommandText = """
        INSERT OR REPLACE INTO file_hashes
        (path, folder, filename, size_bytes, sha256, phase, scanned_at)
        VALUES ($p,$f,$n,$s,$h,$ph,$t)
    """;

    int count = 0;

    try
    {
        foreach (var row in queue.GetConsumingEnumerable())
        {
            cmd.Parameters.Clear();
            cmd.Parameters.AddWithValue("$p", row.Path);
            cmd.Parameters.AddWithValue("$f", row.Folder);
            cmd.Parameters.AddWithValue("$n", row.Filename);
            cmd.Parameters.AddWithValue("$s", row.SizeBytes);
            cmd.Parameters.AddWithValue("$h", row.Sha256);
            cmd.Parameters.AddWithValue("$ph", row.Phase);
            cmd.Parameters.AddWithValue("$t", DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            cmd.ExecuteNonQuery();

            if (++count % DEFAULT_BATCH_SIZE == 0)
            {
                tx.Commit();
                tx.Dispose();
                tx = workConn.BeginTransaction();
                cmd.Transaction = tx;
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
            // Evaluate immediately so access exceptions are caught here
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
                    break; // Skip remaining files in dir if access denied mid-enumeration
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
    // Already extended
    if (path.StartsWith(@"\\?\"))
        return path;
    // UNC path
    if (path.StartsWith(@"\\"))
        return @"\\?\UNC\" + path.Substring(2);
    // Local path
    return @"\\?\" + path;
}

static string Csv(string v)
{
    return v.Contains(',') || v.Contains('"')
        ? $"\"{v.Replace("\"", "\"\"")}\""
        : v;
}

static void RunVerifyReport(string destRoot, string dbPath, string destPhase)
{
    using var conn = new SqliteConnection($"Data Source={dbPath}");
    conn.Open();

    using var cmdSrc = conn.CreateCommand();
    cmdSrc.CommandText = "SELECT DISTINCT phase FROM file_hashes WHERE phase != $p LIMIT 1";
    cmdSrc.Parameters.AddWithValue("$p", destPhase);
    var srcObj = cmdSrc.ExecuteScalar();
    if (srcObj == null || srcObj is DBNull)
    {
        Console.WriteLine("No source phase found in DB to compare against.");
        return;
    }
    string srcPhase = (string)srcObj;

    string outCsv = Path.Combine(Path.GetDirectoryName(Path.GetFullPath(dbPath)) ?? ".", "verification_issues.csv");
    int issues = 0;

    using (var sw = new StreamWriter(outCsv, false, Encoding.UTF8))
    {
        sw.WriteLine("Path,Status,SourceSHA256,DestSHA256");

        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT 
                s.path AS reported_path,
                s.sha256 AS src_hash,
                d.sha256 AS dst_hash
            FROM file_hashes s
            LEFT JOIN file_hashes d
              ON s.path = d.path AND d.phase = $dst
            WHERE s.phase = $src 
              AND (d.sha256 IS NULL OR s.sha256 != d.sha256)

            UNION ALL

            SELECT 
                d.path AS reported_path,
                NULL AS src_hash,
                d.sha256 AS dst_hash
            FROM file_hashes d
            LEFT JOIN file_hashes s
              ON s.path = d.path AND s.phase = $src
            WHERE d.phase = $dst
              AND s.path IS NULL
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

            string status;
            if (string.IsNullOrEmpty(srcHash)) status = "EXTRA";
            else if (string.IsNullOrEmpty(dstHash)) status = "MISSING";
            else status = "MISMATCH";

            sw.WriteLine($"{Csv(reportedPath)},{status},{srcHash},{dstHash}");
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

record FileRow(
    string Path,
    string Folder,
    string Filename,
    long SizeBytes,
    string Sha256,
    string Phase
);