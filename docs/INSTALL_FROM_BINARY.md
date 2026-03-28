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

- `sha256sums.txt`
- the release notes or operator guide that came with the bundle
- `chain5644.zip` if you want to start from the published bootstrap snapshot

## Verify the checksums

On Linux:

```bash
sha256sum -c SHA256SUMS.txt
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
tar -xzf duta-release-1.0.4-linux-x86_64.tar.gz
cd duta-release-1.0.4-linux-x86_64
```

Install the binaries:

```bash
install -m 0755 dutad-1.0.4-linux-x86_64 /usr/local/bin/dutad
install -m 0755 duta-cli-1.0.4-linux-x86_64 /usr/local/bin/duta-cli
install -m 0755 dutaminer-1.0.4-linux-x86_64 /usr/local/bin/dutaminer
```

Create the data directory:

```bash
mkdir -p /root/.duta
chmod 700 /root/.duta
```

If you are using the published bootstrap snapshot, extract `chain5644.zip` into the datadir before first start.

Then continue with [Linux service deployment](DEPLOY_LINUX_SERVICES.md).

## Windows install example

Extract the ZIP archive, then place the binaries in a folder you control, for example:

```text
C:\DUTA
```

You can test the binaries directly:

```bat
dutad-1.0.4-windows-x86_64.exe --help
duta-cli-1.0.4-windows-x86_64.exe --help
dutaminer-1.0.4-windows-x86_64.exe --help
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
C:\DUTA\dutad-1.0.4-windows-x86_64.exe
C:\DUTA\duta-cli-1.0.4-windows-x86_64.exe
C:\DUTA\dutaminer-1.0.4-windows-x86_64.exe
```

## Important notes

- use only binaries and checksums that belong to the same final release bundle
- keep daemon admin RPC local-only
- expose the mining port only if you mean to serve external miners
