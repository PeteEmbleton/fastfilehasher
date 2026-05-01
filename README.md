# FastFileHasher

FastFileHasher is a high-performance command-line utility built in .NET 8 designed to aggressively scan, hash (SHA-256), and index massive directory trees (scaling smoothly to tens of millions of files). The hashes are stored locally in a highly optimized SQLite database.

## Features
- **Concurrent Execution:** Uses multi-threading to hash files in parallel maximizing disk IO and CPU.
- **O(1) Memory Footprint:** Streams file enumeration iteratively. Safely bypasses `UnauthorizedAccessException` blocks without loading massive file lists into memory.
- **Optimized SQLite Ingestion:** Batches records using transactions and Write-Ahead Logging (WAL) for rapid persistent ingestion.
- **Long Path Support:** Unconditionally handles Windows extended length paths (`\\?\`) to safely hash deeply nested folders beyond the 260 character limit.

## Build Instructions

You can build the tool via the .NET 8 CLI.

**Standard Build:**
```powershell
dotnet build -c Release
```

**Publish as a Standalone Single-File Executable (Recommended):**
```powershell
dotnet publish -c Release -r win-x64 --self-contained true -p:PublishSingleFile=true -p:IncludeNativeLibrariesForSelfExtract=true
```
*(You will find the generated `.exe` inside `bin\Release\net8.0\win-x64\publish\`)*

## Usage

The utility operates in 3 distinct modes.

```text
Usage:
  FastFileHasher scan   <rootPath> <dbPath> <phase> [threads] [--in-memory]
  FastFileHasher verify <rootPath> <dbPath> <phase> [threads] [--in-memory]
  FastFileHasher export <outputDir> <dbPath> <phase>
  FastFileHasher warmup <dbPath>
```

### 1. Warmup Phase (Optional but Recommended)
Creates the database and rapidly inserts dummy data into an isolated table to intentionally trigger and satisfy enterprise Antivirus (AV) heuristic scans. Doing this once ensures subsequent scans run at maximum IO speed without AV throttling.

```powershell
FastFileHasher warmup "C:\hashes.db"
```

### 2. Scan Phase
Traverses the given root directory, hashing files and saving them to the database.

```powershell
FastFileHasher scan "C:\MyLargeFolder" "C:\hashes.db" source
```
*Note: `phase` is an arbitrary string (e.g., `source` or `dest`) used to tag the run so you can compare two different locations in the same database later.*

### 3. Verify Phase
Works identically to the scan phase, but is typically executed on the destination directory after a migration or backup copy to verify data integrity.

```powershell
FastFileHasher verify "D:\MyBackupFolder" "C:\hashes.db" dest
```

### 5. Memory Mode (`--in-memory`)
If you append `--in-memory` to a `scan` or `verify` command, the tool will construct and use an in-memory SQLite database (`Mode=Memory;Cache=Shared`) for the duration of the scan. Once the scan is entirely complete, it performs a bulk dump (using SQLite's online backup API) directly into the physical database file.
* This avoids aggressive, per-transaction Antivirus heuristic monitoring blocks by completely decoupling the inserts from disk IO.
* Make sure you have adequate RAM available (around 100MB per million hashed files).

### 4. Export Phase
Exports the calculated hashes from the SQLite database into CSV files. A separate `.csv` is created for every subfolder.

```powershell
FastFileHasher export "C:\ExportedCsvs" "C:\hashes.db" source
```

## Performance Note
By default, the scanner will consume the maximum number of logical processors available on your machine. You can constrain the threads by passing an integer as the final argument during the `scan` or `verify` commands:

```powershell
FastFileHasher scan "C:\MyLargeFolder" "C:\hashes.db" source 4
```
