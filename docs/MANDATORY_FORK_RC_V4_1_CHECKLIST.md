# Mandatory Fork RC v4.1 Checklist

Status basis:
- Local consensus gate: PASS
- Runtime split gate: PASS
- Anti-GPU baseline gate: PASS (initial evidence)
- High-end GPU validation: external evidence item

Release-candidate freeze:
- PoW candidate: `dutahash v4.1`
- Dataset global: 256 MB
- Steps: 4096
- Scratchpad per hash: 256 KB
- Random dataset reads per step: 4
- Bits schedule: unchanged from current candidate baseline

Pre-activation checks:
- Build Linux release artifacts from the frozen `v4.1` source only.
- Verify patched node mines and validates `pow_version=4` jobs post-activation.
- Verify legacy node stalls on patched chain after activation.
- Verify legacy datadir upgraded in place can rejoin patched chain.
- Verify patched node survives abrupt crash and restart without tip or UTXO drift.
- Verify `gettxoutsetinfo` and sampled coinbase UTXO remain consistent before and after restart.
- Record release artifact hashes in the RC bundle manifest.

Activation planning checks:
- Activation height proposal:
  stable mainnet height observed on public seeds: 4,400.
  option cepat = 4,800.
  option aman = 5,400.
  testnet = immediate/manual for lab only.
  stagenet = separate/manual if used.
- Freeze one of the two mainnet activation heights in the final release announcement and manifest before publishing binaries.
- Publish exact upgrade window and operator deadline.
- Require patched node deployment before activation height.
- Require mining operators to switch to patched miner/node path before activation.
- Publish explicit statement that legacy nodes will stall after activation.
- Publish rollback rule:
  if activation has not happened yet, operators may downgrade.
  after activation, do not downgrade onto the active production datadir.

External validation items:
- Benchmark one modern NVIDIA GPU.
- Benchmark one modern AMD GPU if available.
- Compare GPU/CPU ratio against the frozen `v4.1` CPU baseline.

RC exit criteria:
- Restart/crash/recovery proof: PASS
- Legacy-upgrade rejoin proof: PASS
- Accounting/state/funds proof: PASS
- Linux RC bundle built and hashed: PASS
