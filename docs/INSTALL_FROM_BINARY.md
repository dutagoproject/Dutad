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

On Windows PowerShell:

```powershell
Get-FileHash .\dutad.exe -Algorithm SHA256
Get-FileHash .\duta-cli.exe -Algorithm SHA256
Get-FileHash .\dutaminer.exe -Algorithm SHA256
```

Compare the output to `sha256sums.txt`.

## Linux install example

Extract the archive:

```bash
tar -xzf duta-release-0.0.1-beta-linux-x86_64.tar.gz
cd duta-release-0.0.1-beta-linux-x86_64
```

Install the binaries:

```bash
install -m 0755 dutad /usr/local/bin/dutad
install -m 0755 duta-cli /usr/local/bin/duta-cli
install -m 0755 dutaminer /usr/local/bin/dutaminer
```

Create the data directory:

```bash
mkdir -p /root/.duta/mainnet
chmod 700 /root/.duta
chmod 700 /root/.duta/mainnet
```

Then continue with [Linux service deployment](DEPLOY_LINUX_SERVICES.md).

## Windows install example

Extract the ZIP archive, then place the binaries in a folder you control, for example:

```text
C:\DUTA
```

You can test the binaries directly:

```powershell
.\dutad.exe --help
.\duta-cli.exe --help
.\dutaminer.exe --help
```

## Suggested layout

Linux:

```text
/usr/local/bin/dutad
/usr/local/bin/duta-cli
/usr/local/bin/dutaminer
/root/.duta/mainnet
```

Windows:

```text
C:\DUTA\dutad.exe
C:\DUTA\duta-cli.exe
C:\DUTA\dutaminer.exe
```

## Important notes

- this is still a beta release
- keep daemon admin RPC local-only
- expose the mining port only if you mean to serve external miners
