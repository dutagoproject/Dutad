# Build on Windows

This is the easiest setup if you want to build and test the `dutad` repo on Windows.

## What you need

- Windows 10 or newer
- Rust from [rustup](https://rustup.rs/)
- Microsoft C++ build tools
- PowerShell

## Install Rust

After installing Rust, check that it is available:

```powershell
rustc --version
cargo --version
```

## Build

From the repository root:

```powershell
cargo build
```

Run tests:

```powershell
cargo test
```

## Build release binaries

```powershell
cargo build --release
```

The release binaries will be in `target\release\`.

## Build a release bundle

If you want a Windows bundle with checksums and a manifest for the service binaries:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\build_release_bundle.ps1 -Version 0.0.1-beta
```

For the full release flow, see [Release on Windows](RELEASE_WINDOWS.md).

## Run the main services

Run the node:

```powershell
cargo run -p dutad -- --datadir .\data\mainnet
```

Run the solo miner:

```powershell
cargo run -p dutad --bin dutaminer -- --rpc http://127.0.0.1:19085 --address YOUR_DUTA_ADDRESS --threads 8
```

## Important

- Keep node admin RPC private
- Do not commit `target\` or local release bundles
