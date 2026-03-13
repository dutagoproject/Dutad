# Backup and Recovery

This document is for node operators who want a simple recovery plan.

It covers:

- node data

## What matters most

If you only remember one thing:

- chain data can be rebuilt
- node config and deployment notes are what you are most likely to need quickly

## Node backup

For `dutad`, the main data lives under the DUTA data directory.

Typical mainnet path:

```text
/root/.duta/mainnet
```

What to keep:

- `duta.conf`
- any custom deployment notes
- any service unit overrides you created

You can back up full node data if you want faster restore, but it is not the most critical backup.

## Minimum backup set

At minimum, keep:

- `duta.conf`
- service unit overrides
- deployment notes such as bound ports, peers, and host rules

If you use custom seed nodes or custom startup flags, back those up too.

## Recommended practice

- keep one local encrypted backup
- keep one offline backup
- test restore before trusting the backup

## Linux backup example

Example config backup:

```bash
cp /root/.duta/mainnet/duta.conf /root/backups/duta.conf
```

## Restore a node

If the node data is damaged or lost:

1. reinstall the binaries
2. restore `duta.conf` if you use one
3. restore service unit files if needed
4. either restore node data or let the node sync again

The chain can be rebuilt by syncing from peers.

## Recovery checklist

If something goes wrong:

1. check whether `duta.conf` still exists
2. check whether the datadir path is correct
3. check whether service units or startup flags changed
4. check whether the daemon can still sync from peers
5. avoid overwriting the last known good backup

## Beta reminder

This release line is still beta.

Do not wait for an incident before testing your backup and recovery path.
