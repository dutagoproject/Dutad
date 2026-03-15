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
./scripts/build-linux.sh
```

Run tests:

```bash
cargo test
```

The release binaries will be in `target/release/`.

## Run the main services

Run the node:

```bash
./scripts/run-node-mining.sh ./data/mainnet 127.0.0.1:19085
```

Run the solo miner:

```bash
./scripts/mine-solo.sh http://127.0.0.1:19085 YOUR_DUTA_ADDRESS 8
```

## Important

- Keep node admin RPC private
- Use separate data directories for different networks
