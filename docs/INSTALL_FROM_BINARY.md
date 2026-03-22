# Install from Binary Bundles

This guide is for operators who want to run the `dutad` repo binaries from release bundles instead of building from source.

It covers:

- `dutad`
- `duta-cli`
- `dutaminer`

## What to download

For each release, download the bundle that matches your platform:

- Windows: `duta-release-<version>-windows-x86_64`
- Linux: `duta-release-<version>-linux-x86_64`

Also keep:

- `manifest.json`
- `sha256sums.txt`

## Verify the checksums

On Linux:

```bash
sha256sum -c sha256sums.txt
```

On Windows:

```bat
certutil -hashfile dutad.exe SHA256
certutil -hashfile duta-cli.exe SHA256
certutil -hashfile dutaminer.exe SHA256
```

Compare the output to `sha256sums.txt`.

## Linux install example

Extract the archive:

```bash
tar -xzf duta-release-1.0.1-linux-x86_64.tar.gz
cd duta-release-1.0.1-linux-x86_64
```

Install the binaries:

```bash
install -m 0755 dutad /usr/local/bin/dutad
install -m 0755 duta-cli /usr/local/bin/duta-cli
install -m 0755 dutaminer /usr/local/bin/dutaminer
```

Create the data directory:

```bash
mkdir -p /root/.duta
chmod 700 /root/.duta
```

Then continue with [Linux service deployment](DEPLOY_LINUX_SERVICES.md).

## Windows install example

Extract the ZIP archive, then place the binaries in a folder you control, for example:

```text
C:\DUTA
```

You can test the binaries directly:

```bat
dutad.exe --help
duta-cli.exe --help
dutaminer.exe --help
```

## Suggested layout

Linux:

```text
/usr/local/bin/dutad
/usr/local/bin/duta-cli
/usr/local/bin/dutaminer
/root/.duta
```

Windows:

```text
C:\DUTA\dutad.exe
C:\DUTA\duta-cli.exe
C:\DUTA\dutaminer.exe
```

## Important notes

- use only binaries that match the final mainnet release
- keep daemon admin RPC local-only
- expose the mining port only if you mean to serve external miners
