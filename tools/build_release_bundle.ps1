param(
    [string]$Version = "1.0.0",
    [string]$OutputRoot = "dist",
    [switch]$SkipBuild,
    [string]$TargetTriple = "x86_64-pc-windows-msvc"
)

$ErrorActionPreference = "Stop"

$workspaceRoot = Split-Path -Parent $PSScriptRoot
Set-Location $workspaceRoot

$releaseDir = if ($TargetTriple -eq "x86_64-pc-windows-msvc") {
    Join-Path $workspaceRoot "target\release"
} else {
    Join-Path $workspaceRoot (Join-Path "target" (Join-Path $TargetTriple "release"))
}

$archLabel = switch ($TargetTriple) {
    "x86_64-pc-windows-msvc" { "windows-x86_64" }
    "x86_64-pc-windows-gnu" { "windows-x86_64-gnu" }
    default { $TargetTriple.Replace("pc-", "").Replace("-msvc", "").Replace("-gnu", "") }
}

$bundleName = "duta-release-$Version-$archLabel"
$bundleDir = Join-Path $workspaceRoot (Join-Path $OutputRoot $bundleName)

$artifacts = @(
    @{ package = "dutad"; target = "dutad.exe"; kind = "bin" },
    @{ package = "dutad"; target = "duta-cli.exe"; kind = "bin" },
    @{ package = "dutad"; target = "dutaminer.exe"; kind = "bin" }
)

$packages = $artifacts.package | Sort-Object -Unique

if (-not $SkipBuild) {
    foreach ($package in $packages) {
        Write-Host "Building release package $package"
        if ($TargetTriple -eq "x86_64-pc-windows-msvc") {
            cargo build --release -p $package
        } else {
            cargo build --release --target $TargetTriple -p $package
        }
    }
}

New-Item -ItemType Directory -Force -Path $bundleDir | Out-Null

$manifestArtifacts = @()
$hashLines = New-Object System.Collections.Generic.List[string]

foreach ($artifact in $artifacts) {
    $sourcePath = Join-Path $releaseDir $artifact.target
    if (-not (Test-Path $sourcePath)) {
        throw "missing_release_artifact: $($artifact.target)"
    }

    $destPath = Join-Path $bundleDir $artifact.target
    Copy-Item -Force $sourcePath $destPath

    $hash = (Get-FileHash -Algorithm SHA256 $destPath).Hash.ToLowerInvariant()
    $size = (Get-Item $destPath).Length

    $manifestArtifacts += [pscustomobject]@{
        package = $artifact.package
        file = $artifact.target
        kind = $artifact.kind
        sha256 = $hash
        bytes = $size
    }
    $hashLines.Add("$hash  $($artifact.target)")
}

$manifest = [ordered]@{
    name = $bundleName
    version = $Version
    generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    workspace = $workspaceRoot
    host_os = "windows"
    target_triple = $TargetTriple
    cargo_version = (& cargo --version)
    rustc_version = (& rustc --version)
    build_commands = @(
        $packages | ForEach-Object {
            if ($TargetTriple -eq "x86_64-pc-windows-msvc") {
                "cargo build --release -p $_"
            } else {
                "cargo build --release --target $TargetTriple -p $_"
            }
        }
    )
    artifacts = $manifestArtifacts
}

$manifestPath = Join-Path $bundleDir "manifest.json"
$hashPath = Join-Path $bundleDir "sha256sums.txt"

$manifest | ConvertTo-Json -Depth 6 | Set-Content -Encoding ASCII $manifestPath
$hashLines | Set-Content -Encoding ASCII $hashPath

Write-Host "Release bundle ready: $bundleDir"
