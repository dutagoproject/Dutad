# Usage Guide

This is the basic way to run the `dutad` repo binaries.

## Node

Start the node:

```bash
cargo run -p dutad -- --datadir ./data/mainnet
```

Common flags:

- `--testnet`
- `--stagenet`
- `--datadir <path>`
- `--daemon`
- `--bind <ip>`
- `--mining-bind <ip:port>`

## HTTP mining

If you want the node to expose a mining HTTP listener:

```bash
cargo run -p dutad -- --datadir ./data/mainnet --mining-bind 0.0.0.0:19085
```

This listener is used for endpoints such as:

- `/work`
- `/submit_work`
- `/getmininginfo`

## Basic operational rules

- keep node admin RPC private
- use separate data directories for mainnet, testnet, and stagenet
- treat the public mining listener as an internet-facing service
