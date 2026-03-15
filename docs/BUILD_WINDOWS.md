# Build on Windows

This is the easiest setup if you want to build and test the `dutad` repo on Windows without PowerShell.

## What you need

- Windows 10 or newer
- Rust from [rustup](https://rustup.rs/)
- Microsoft C++ build tools
- `cmd.exe`

## Install Rust

After installing Rust, check that it is available:

```bat
rustc --version
cargo --version
```

## Build

From the repository root:

```bat
scripts\build-windows.cmd
```

Run tests:

```bat
cargo test
```

The release binaries will be in `target\release\`.

## Run the main services

Run the node:

```bat
scripts\run-node-mining.cmd .\data\mainnet 127.0.0.1:19085
```

Run the solo miner:

```bat
scripts\mine-solo.cmd http://127.0.0.1:19085 YOUR_DUTA_ADDRESS 8
```

## Important

- Keep node admin RPC private
- Do not commit `target\` or local release bundles
