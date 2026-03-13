# Build on Linux

This is the standard setup for Linux development and testing for the `dutad` repo.

## What you need

- a recent Linux distribution
- `curl`
- `build-essential` or an equivalent compiler toolchain
- `pkg-config`
- Rust from [rustup](https://rustup.rs/)

On Debian or Ubuntu:

```bash
sudo apt update
sudo apt install -y build-essential pkg-config curl
```

## Install Rust

```bash
curl https://sh.rustup.rs -sSf | sh
source "$HOME/.cargo/env"
rustc --version
cargo --version
```

## Build

From the repository root:

```bash
cargo build
```

Run tests:

```bash
cargo test
```

## Build release binaries

```bash
cargo build --release
```

The release binaries will be in `target/release/`.

If you want a packaged release bundle with checksums and a manifest for the service binaries:

```bash
chmod +x ./tools/build_release_bundle.sh
./tools/build_release_bundle.sh 0.0.1-beta
```

For the full release flow, see [Release on Linux](RELEASE_LINUX.md).

## Run the main services

Run the node:

```bash
cargo run -p dutad -- --datadir ./data/mainnet
```

Run the solo miner:

```bash
cargo run -p dutad --bin dutaminer -- --rpc http://127.0.0.1:19085 --address YOUR_DUTA_ADDRESS --threads 8
```

## Important

- Keep node admin RPC private
- Use separate data directories for different networks
