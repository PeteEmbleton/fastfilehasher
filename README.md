# FastFileHasher

FastFileHasher is a .NET 8 CLI for deterministic, resumable, hash-verified file copy and migration workflows on Windows paths (including `\\?\` and UNC forms).

## Build

```powershell
dotnet build -c Release
```

```powershell
dotnet publish -c Release -r win-x64 --self-contained true -p:PublishSingleFile=true -p:IncludeNativeLibrariesForSelfExtract=true
```

## Usage

```text
FastFileHasher warmup <dbPath>
FastFileHasher scan    <rootPath> <dbPath> <phase> [threads] [--in-memory]
FastFileHasher verify  <rootPath> <dbPath> <phase> [threads] [--in-memory]
FastFileHasher export  <outputDir> <dbPath> <phase>
FastFileHasher copy    <srcRoot> <destRoot> <dbPath> <srcPhase> <destPhase> [threads] [--use-robocopy] [--in-memory]
FastFileHasher migrate <srcRoot> <destRoot> <dbPath> <srcPhase> <destPhase> [threads] [--use-robocopy] [--in-memory]
```

## Notes

- `copy` and `migrate` stream files through scan -> copy -> verify.
- Verification uses explicit `src_path -> dest_path` mapping from `file_copy`, so root/path remaps are supported.
- `migrate` deletes the source file only after destination hash verification succeeds.
- `--use-robocopy` enables robocopy-based transfer mode; default is managed copy with temp file + atomic rename.
- `verification_issues.csv` is written next to the DB when issues exist, and removed when verification is clean.
