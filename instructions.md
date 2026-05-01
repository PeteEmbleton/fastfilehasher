# FastFileHasher – Usage Guide

FastFileHasher is a **read‑only file integrity tool** used to:

*   scan files and record SHA‑256 hashes
*   verify files after they have been moved by external systems
*   flag any integrity issues (missing, extra, or mismatched files)
*   produce CSV outputs for audit or EDRMS use

The tool **never modifies scanned files**.

***

## Command syntax
    FastFileHasher warmup <dbPath>
    FastFileHasher scan   <rootPath> <dbPath> <phase> [threads]
    FastFileHasher verify <rootPath> <dbPath> <phase> [threads]
    FastFileHasher export <outputDir> <dbPath> <phase>
    

***

## Arguments

### `<rootPath>`

The root directory to scan.

*   All files under this path are scanned recursively
*   Files are opened **read‑only**
*   Long paths are supported

Examples:

    D:\Records
    X:\ArchiveVolume

***

### `<dbPath>`

Path to the SQLite database used to store scan results.

*   Created automatically if it does not exist
*   Reused across runs
*   Must be writable

Example:

    D:\hashes\migration_hashes.db

***

### `<phase>`

A label identifying what the scan represents.

Typical values:

*   `source` – before files are moved
*   `dest` – after files are moved

The phase is used to compare scans during verification.  
It is a logical label only and does not affect file access.

***

### `[threads]` (optional)

Maximum number of parallel hashing threads.

*   Default: number of CPU cores
*   Typical effective range: **8–16**
*   Increasing beyond storage capability will not improve performance

Example:

    16

***

## Modes

### `warmup`

Creates the database and rapidly inserts dummy data into an isolated table.

*   Used to intentionally trigger Antivirus (AV) heuristic scans
*   Ensures subsequent `scan` or `verify` operations run at full speed without AV interference
*   Does not scan any files

Example:

    FastFileHasher warmup D:\hashes.db

***

### `scan`

Performs a read‑only scan of all files under `<rootPath>` and records:

*   relative folder
*   filename
*   file size
*   SHA‑256 hash
*   phase label

No comparison is performed.

Example:

    FastFileHasher scan D:\SourceData D:\hashes.db source 16

***

### `verify`

Performs a scan **and immediately verifies** results against an existing source phase.

Verify does the following in one operation:

1.  Scans destination files
2.  Compares source and destination hashes
3.  Generates a CSV containing **only problems**

No files are modified or repaired.

Example:

    FastFileHasher verify X:\MovedData D:\hashes.db dest 16

#### Verification CSV output

If issues are found, a file is created next to the database:

    verification_issues.csv

CSV format:

    Path,Status,SourceSHA256,DestSHA256

Possible `Status` values:

*   `MISMATCH` – file exists in both locations but contents differ
*   `MISSING` – file exists in source but not in destination
*   `EXTRA` – file exists in destination but not in source

If **no issues are found**, the CSV is automatically deleted and the verify run reports success.

***

### `export`

Exports hashes for a given phase as per‑folder CSV files.

*   One CSV per relative folder
*   Intended for EDRMS or long‑term retention
*   Does not perform verification

Example:

    FastFileHasher export D:\EDRMS_OUTPUT D:\hashes.db source

Output structure:

    EDRMS_OUTPUT\
      FolderA\
        folder_hashes.csv
      FolderB\
        folder_hashes.csv

***

## Important behaviour notes

*   The tool is **read‑only by design**
*   No files are rewritten, repaired, deleted, or moved
*   Verification detects and flags issues only
*   Matching is based on **relative folder + filename**
*   File contents are verified using SHA‑256

***

## Typical workflow

1.  **Antivirus Warmup (Optional, but recommended on first run)**
        FastFileHasher warmup <dbPath>

2.  **Initial scan (before move)**
        FastFileHasher scan <sourcePath> <dbPath> source

2.  **Files moved externally**  
    (SAN, replication, storage tooling)

3.  **Verification scan (after move)**
        FastFileHasher verify <destPath> <dbPath> dest

4.  **Review `verification_issues.csv` if present**

5.  **Export CSVs for EDRMS (optional)**
        FastFileHasher export <outputDir> <dbPath> source


