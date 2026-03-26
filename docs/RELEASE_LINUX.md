# Release on Linux

This guide is for packaging the `dutad` repo binaries on Linux.

For the current public release line, package them as `1.0.3`.

It covers:

- `dutad`
- `duta-cli`
- `dutaminer`

## Requirements

- a recent Linux distribution
- `curl`
- `build-essential` or equivalent compiler tools
- `pkg-config`
- Rust toolchain from [rustup](https://rustup.rs/)

On Debian or Ubuntu:

```bash
sudo apt update
sudo apt install -y build-essential pkg-config curl
```

## Build the release binaries

From the repository root:

```bash
cargo build --release -p dutad
```

This produces the binaries under `target/release/`.

## Build a Linux release bundle

From the repository root:

```bash
chmod +x ./tools/build_release_bundle.sh
./tools/build_release_bundle.sh 1.0.3
```

The bundle will be created in:

```text
dist/duta-release-1.0.3-linux-x86_64
```

A compressed archive will also be created:

```text
dist/duta-release-1.0.3-linux-x86_64.tar.gz
```

## Build for another Linux target

Example with musl:

```bash
rustup target add x86_64-unknown-linux-musl
./tools/build_release_bundle.sh 1.0.3 x86_64-unknown-linux-musl
```

## Quick verification

After the bundle is built, test the binaries directly:

```bash
./dist/duta-release-1.0.3-linux-x86_64/dutad --help
./dist/duta-release-1.0.3-linux-x86_64/duta-cli --help
./dist/duta-release-1.0.3-linux-x86_64/dutaminer --help
```

## Suggested release checklist

Before publishing a Linux release:

- run `cargo test`
- build the bundle
- verify the main binaries start and print help output
- check `sha256sums.txt` or the published `SHA256SUMS.txt` if you are assembling a release folder manually
- keep daemon admin RPC private by default in deployment docs
- label the package version clearly as `1.0.3`
- label the bundle version clearly as `1.0.3`
