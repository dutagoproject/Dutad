# dutad 0.0.1-beta (Historical Archive)

This document is kept only as an archive of the first public beta package notes.
It is not the current release guidance for the repository.

For the active release line, use the `1.0.0` packaging and install guides in this repo.

## Highlights

- Full node daemon for the DUTA network
- Private admin RPC and chain sync tooling
- Built-in solo mining path for operator use

## Included files

- `dutad`
- `duta-cli`
- `dutaminer`

## Who should use this

Use this package if you want to:

- run a DUTA full node
- sync the network
- expose private admin RPC on a trusted host
- run solo mining directly against the daemon

## Quick start

1. Extract the archive.
2. Review the example config and operator notes.
3. Start `dutad`.
4. Wait for sync to complete.
5. Use `duta-cli` only from a trusted environment.

## Security notes

- Keep admin RPC private.
- Do not expose recovery or operator-only endpoints to the public internet.
- Use stable datadirs and keep backups of node configuration.
- If you run a public-facing setup, place explorer and web services behind separate boundaries from admin RPC.

## Checksums and archives

Choose the archive that matches your platform:

- Linux x86_64
- Windows x86_64

If a checksum file is attached to the release, verify it before running the binaries.

## Archive note

This beta package note is preserved for historical reference only.
For current operator guidance, use the final `1.0.1` release and deployment documents in this repository.

For build, deployment, and security notes, see the repository documentation.
