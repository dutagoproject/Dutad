# dutad

`dutad` is the DUTA node repository.

It contains the chain-critical parts of the project: consensus rules, block and transaction validation, storage, daemon RPC, P2P networking, and the built-in solo mining flow.

Current release line: `1.0.1`

Website: https://dutago.xyz

## Repository scope

This repository includes:

- `core`
  Shared chain primitives such as addresses, network parameters, hashing, and consensus helpers.
- `daemon`
  The node daemon, RPC handlers, P2P implementation, mining endpoints, and CLI binaries.
- `docs`
  Build, release, deployment, mining, backup, and operator notes.
- `examples`
  Example service and config material.
- `scripts`
  Helper scripts for local and operational workflows.

This repository does not include:

- the wallet daemon
- the public stratum service
- the reference stratum miner
- the desktop GUI

## Main binaries

- `dutad`
- `duta-cli`
- `dutaminer`

## Build

From the repository root:

```sh
cargo build
cargo build --release
```

Quick helpers:

- Windows: `scripts\\build-windows.cmd`
- Linux: `./scripts/build-linux.sh`

## Release position

This repo is intended for operators, infrastructure builders, and integrators who need the node itself.

If you need wallet management, use the `wallet` repository.
If you need a public mining bridge, use the `stratum` repository.

## Documentation

- Windows build: [docs/BUILD_WINDOWS.md](./docs/BUILD_WINDOWS.md)
- Linux build: [docs/BUILD_LINUX.md](./docs/BUILD_LINUX.md)
- Usage: [docs/USAGE.md](./docs/USAGE.md)
- Install from binary: [docs/INSTALL_FROM_BINARY.md](./docs/INSTALL_FROM_BINARY.md)
- Mining guide: [docs/MINING_GUIDE.md](./docs/MINING_GUIDE.md)
- Linux service deployment: [docs/DEPLOY_LINUX_SERVICES.md](./docs/DEPLOY_LINUX_SERVICES.md)
- Backup and recovery: [docs/BACKUP_AND_RECOVERY.md](./docs/BACKUP_AND_RECOVERY.md)
- Security notes: [docs/SECURITY_NOTES.md](./docs/SECURITY_NOTES.md)
- Linux release flow: [docs/RELEASE_LINUX.md](./docs/RELEASE_LINUX.md)
- Windows release flow: [docs/RELEASE_WINDOWS.md](./docs/RELEASE_WINDOWS.md)
