# FastFileHasher – Usage Guide

FastFileHasher is a **hash‑verified file migration engine** used to:

*   scan files and record SHA‑256 hashes
*   copy files deterministically with exact path mapping
*   migrate files (copy and securely remove source upon verification)
*   verify files after they have been moved by external systems or by this tool
*   flag any integrity issues (missing, extra, or mismatched files)
*   produce CSV outputs for audit or EDRMS use

***

## Command syntax
    FastFileHasher warmup <dbPath>
    FastFileHasher scan    <rootPath> <dbPath> <phase> [threads]
    FastFileHasher verify  <rootPath> <dbPath> <phase> [threads]
    FastFileHasher export  <outputDir> <dbPath> <phase>
    FastFileHasher copy    <srcRoot> <destRoot> <dbPath> <srcPhase> <destPhase> [threads] [--use-robocopy] [--in-memory]
    FastFileHasher migrate <srcRoot> <destRoot> <dbPath> <srcPhase> <destPhase> [threads] [--use-robocopy] [--in-memory]
    
***

## Arguments

### `<rootPath>` or `<srcRoot>` / `<destRoot>`

The directory to scan, copy from, or copy to.

*   All files under this path are processed recursively
*   Long paths are supported (internally normalized to `\\?\` prefix)

Examples:

    D:\Records
    X:\ArchiveVolume
    \\Server\Share\Data

***

### `<dbPath>`

Path to the SQLite database used to store scan and copy state.

*   Created automatically if it does not exist
*   Reused across runs for resuming
*   Must be writable

Example:

    D:\hashes\migration_hashes.db

***

### `<phase>`

A label identifying what the scan represents.

Typical values:

*   `source` – before files are moved
*   `dest` – after files are moved

***

### `[threads]` (optional)

Maximum number of parallel hashing and copy threads.

*   Default: dynamically tuned (typically 8 for UNC paths, up to 32 for local)
*   Increasing beyond storage capability will not improve performance

Example:

    16

***

### `--use-robocopy`

Opts-in to using robocopy to handle file data movement, maintaining full NTFS fidelity (Owner, ACLs, ADS, Dates). If not specified, the system will perform a managed copy (which generates a temporary destination, copies the data, and performs an atomic rename).

***

## Modes

### `copy`

Streams files from `<srcRoot>` to `<destRoot>`.
As soon as a file is successfully hashed in the source, it is placed in a queue and copied. Once copied, it is hashed at the destination and cryptographically verified against the source hash.

Example:

    FastFileHasher copy D:\SourceData X:\MovedData D:\hashes.db source dest 16 --use-robocopy

***

### `migrate`

Performs the exact same workflow as `copy`, but **deletes the source file** immediately upon successful cryptographic verification at the destination.

Example:

    FastFileHasher migrate D:\SourceData X:\MovedData D:\hashes.db source dest 16

***

### `warmup`

Creates the database and rapidly inserts dummy data into an isolated table.

*   Used to intentionally trigger Antivirus (AV) heuristic scans
*   Ensures subsequent `scan` or `verify` operations run at full speed without AV interference
*   Does not scan any files

Example:

    FastFileHasher warmup D:\hashes.db

***

### `scan`

Performs a read‑only scan of all files under `<rootPath>` and records hashes. No comparison is performed.

Example:

    FastFileHasher scan D:\SourceData D:\hashes.db source 16

***

### `verify`

Performs a scan **and immediately verifies** results against an existing source phase.
Generates a `verification_issues.csv` if issues are found.

Example:

    FastFileHasher verify X:\MovedData D:\hashes.db dest 16

***

### `export`

Exports hashes for a given phase as per‑folder CSV files.

*   One CSV per relative folder
*   Intended for EDRMS or long‑term retention

Example:

    FastFileHasher export D:\EDRMS_OUTPUT D:\hashes.db source

***

## Verification CSV output

If issues are found during `verify`, `copy`, or `migrate`, a file is created next to the database:

    verification_issues.csv

CSV format:

    Path,Status,SourceSHA256,DestSHA256,LastError

Possible `Status` values:

*   `MISMATCH` – file exists in both locations but contents differ
*   `MISSING` – file exists in source but not in destination
*   `EXTRA` – file exists in destination but not in source
*   `FAILED` – copy or verification process threw an error (check `LastError`)

If **no issues are found**, the CSV is automatically deleted.

***

## Typical workflow for Migration

1.  **Antivirus Warmup (Optional, but recommended on first run)**
        FastFileHasher warmup <dbPath>

2.  **Run Migration (with managed copy)**
        FastFileHasher migrate <sourcePath> <destPath> <dbPath> source dest

3.  **Review `verification_issues.csv` if present**
    (Note: You can re-run the exact same `migrate` command to retry any failures or pick up where it left off after an interruption)

4.  **Export CSVs for EDRMS (optional)**
        FastFileHasher export <outputDir> <dbPath> source
