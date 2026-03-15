# Mining Guide

This guide covers the mining flow that belongs to the `dutad` repo.

It is written for:

- `dutad`
- `dutaminer`

Stratum and pool-oriented mining should live in their own repo.

## Default ports

Mainnet defaults:

- daemon admin RPC: `127.0.0.1:19083`
- mining listener: disabled by default, `19085` when explicitly enabled

Testnet defaults:

- daemon admin RPC: `127.0.0.1:18083`
- mining listener: disabled by default, `18085` when explicitly enabled

## Solo mining with `dutaminer`

First start the daemon with an explicit mining listener:

```bash
./dutad --datadir ./data/mainnet --mining-bind 127.0.0.1:19085
```

Then start the miner:

```bash
./dutaminer --rpc http://127.0.0.1:19085 --address YOUR_DUTA_ADDRESS --threads 12
```

Notes:

- `--rpc` must point to the mining listener, not the admin RPC port
- `--address` must be a valid DUTA address
- `--threads` should match your CPU budget

## Devfee schedule

Mainnet block templates and block validation use the same consensus devfee schedule:

- year 1: `8%`
- year 2: `4%`
- year 3 and later: `2%`

With the current mainnet target of 60 seconds per block, that means:

- heights `0..525599`: `8%`
- heights `525600..1051199`: `4%`
- heights `1051200+`: `2%`

This is height-based, not wall-clock based, so all miners and nodes reach the same result.

## Quick operator checklist

- keep daemon admin RPC on `127.0.0.1`
- keep mining listener on `127.0.0.1` unless you intentionally need remote miners
- expose only the mining listener if you actually need remote solo miners
- use a real wallet address for mining rewards
- watch daemon logs for `BLOCK_REJECT` and wallet address errors

## Common mistakes

### Miner points to `19083`

That is the daemon admin RPC port.

For solo mining, point the miner to the public mining listener instead:

```text
19085 mainnet
18085 testnet
```

### Wallet address errors

If the daemon rejects work with `invalid_address`, check:

- the miner reward address
- the current network
- any fixed reward or devfee address in the runtime configuration and source

## Recommended public setup

For a solo mining node:

1. run `dutad` with admin RPC local-only
2. keep mining on loopback by default, or expose it only after firewall review
3. keep reward destination management outside the public daemon surface

This keeps the public mining surface separate from the admin surface.
