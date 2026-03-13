# Release on Windows

This guide is for packaging the `dutad` repo binaries on Windows.

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
powershell -ExecutionPolicy Bypass -File .\tools\build_release_bundle.ps1 -Version 0.0.1-beta
```

The bundle will be created in:

```text
dist\duta-release-0.0.1-beta-windows-x86_64
```

The bundle includes:

- the release binaries
- `manifest.json`
- `sha256sums.txt`

## Cross-build Windows GNU binaries

If you want a `windows-gnu` bundle instead of the default MSVC build:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\build_release_bundle.ps1 -Version 0.0.1-beta -TargetTriple x86_64-pc-windows-gnu
```

## Quick verification

After the bundle is built, test the binaries directly:

```powershell
.\dist\duta-release-0.0.1-beta-windows-x86_64\dutad.exe --help
.\dist\duta-release-0.0.1-beta-windows-x86_64\duta-cli.exe --help
.\dist\duta-release-0.0.1-beta-windows-x86_64\dutaminer.exe --help
```

## Suggested release checklist

Before publishing a Windows release:

- run `cargo test`
- build the bundle
- verify the main binaries start and print help output
- confirm `sha256sums.txt` matches the bundled files
- keep daemon admin RPC private by default in release notes
- label the release clearly as `0.0.1 beta`
