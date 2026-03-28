# Release on Windows

This guide is for packaging the `dutad` repo binaries on Windows.

For the current public release line, package them as `1.0.4`.

It covers:

- `dutad`
- `duta-cli`
- `dutaminer`

## Requirements

- Windows 10 or newer
- Rust toolchain from [rustup](https://rustup.rs/)
- Microsoft C++ build tools
- PowerShell

## Build the release binaries

From the repository root:

```powershell
cargo build --release -p dutad
```

This produces the binaries under `target\release\`.

## Build a Windows release bundle

From the repository root:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\build_release_bundle.ps1 -Version 1.0.4
```

The bundle will be created in:

```text
dist\duta-release-1.0.4-windows-x86_64
```

The bundle includes:

- the release binaries
- `manifest.json`
- `sha256sums.txt`

## Cross-build Windows GNU binaries

If you want a `windows-gnu` bundle instead of the default MSVC build:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\build_release_bundle.ps1 -Version 1.0.4 -TargetTriple x86_64-pc-windows-gnu
```

## Quick verification

After the bundle is built, test the binaries directly:

```powershell
.\dist\duta-release-1.0.4-windows-x86_64\dutad-1.0.4-windows-x86_64.exe --help
.\dist\duta-release-1.0.4-windows-x86_64\duta-cli-1.0.4-windows-x86_64.exe --help
.\dist\duta-release-1.0.4-windows-x86_64\dutaminer-1.0.4-windows-x86_64.exe --help
```

## Suggested release checklist

Before publishing a Windows release:

- run `cargo test`
- build the bundle
- verify the main binaries start and print help output
- confirm `sha256sums.txt` matches the versioned bundled files
- keep daemon admin RPC private by default in release notes
- label the package version clearly as `1.0.4`
- label the bundle version clearly as `1.0.4`
