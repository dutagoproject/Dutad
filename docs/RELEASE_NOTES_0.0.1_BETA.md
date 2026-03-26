# DUTA 0.0.1 Beta (Historical Archive)

This document is kept as an archive of the original beta release draft.
It should not be used as the release status for the current repository state.

## Included components

- `dutad`
- `duta-cli`
- `dutaminer`

## Highlights

- native Rust blockchain runtime
- consensus, daemon, and solo mining kept together in one repo
- node-focused docs for build, release, usage, mining, and Linux service deployment
- beta-ready release bundles for Linux and Windows node binaries

## Important notes

- this text describes the original beta draft, not the current final release line
- use the active `1.0.3` release docs elsewhere in this repo for current packaging and operator guidance
- daemon admin RPC should stay local-only
- public mining listeners should be exposed deliberately, not by accident

## Devfee schedule

Mainnet consensus uses a height-based devfee schedule:

- year 1: `8%`
- year 2: `4%`
- year 3 and later: `2%`

At the current mainnet target of 60 seconds per block, the schedule boundaries are:

- heights `0..525599`: `8%`
- heights `525600..1051199`: `4%`
- heights `1051200+`: `2%`

This schedule is consensus-critical and should be treated as frozen before public launch.

## Default ports

Mainnet:

- P2P: `19082`
- daemon RPC: `19083`
- mining HTTP: `19085`

Testnet:

- P2P: `18082`
- daemon RPC: `18083`
- mining HTTP: `18085`

## Download and install

See:

- [Build on Windows](BUILD_WINDOWS.md)
- [Build on Linux](BUILD_LINUX.md)
- [Release on Windows](RELEASE_WINDOWS.md)
- [Release on Linux](RELEASE_LINUX.md)
- [Install from binary bundles](INSTALL_FROM_BINARY.md)
- [Mining guide](MINING_GUIDE.md)
- [Linux service deployment](DEPLOY_LINUX_SERVICES.md)

## Operator reminder

If you are running a public node or mining endpoint:

- protect admin RPC
- monitor logs
- verify wallet addresses carefully
- keep backups of node config and deployment notes
