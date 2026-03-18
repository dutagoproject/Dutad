# Deploy on Linux with systemd

This is a simple production-style layout for Linux servers.

It covers:

- `dutad`

Wallet, stratum, and GUI services should be documented in their own repos.

## Suggested binary paths

Install the binaries to:

```text
/usr/local/bin/dutad
/usr/local/bin/duta-cli
/usr/local/bin/dutaminer
```

## Suggested data path

Mainnet:

```text
/root/.duta
```

Testnet:

```text
/root/.duta/testnet
```

## Service unit examples

Example unit files are included here:

- [examples/systemd/dutad.service](../examples/systemd/dutad.service)

Copy them to:

```text
/etc/systemd/system/
```

## Enable the services

```bash
mkdir -p /root/.duta
chmod 700 /root/.duta

systemctl daemon-reload
systemctl enable dutad

systemctl start dutad
```

## Check service status

```bash
systemctl status dutad
```

## Follow logs

```bash
journalctl -u dutad -f
```

## Recommended network exposure

Recommended public surface:

- keep daemon admin RPC local-only on `127.0.0.1:19083`
- expose daemon mining listener only if you want HTTP mining on `19085`

That keeps the admin surface narrower than the mining surface.

## If the service fails with `203/EXEC`

That usually means one of these:

- the binary path in `ExecStart` is wrong
- the file is not executable
- you copied a Windows binary to Linux by mistake

Quick checks:

```bash
ls -l /usr/local/bin/dutad
file /usr/local/bin/dutad
/usr/local/bin/dutad --help
```

Linux binaries should show up as `ELF`, not `PE32`.
