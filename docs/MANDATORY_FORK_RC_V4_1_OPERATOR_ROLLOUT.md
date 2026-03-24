# Mandatory Fork RC v4.1 Operator Rollout

Scope:
- Patched node: `dutad`
- Patched CLI: `duta-cli`
- Patched miner: `dutaminer`
- Network path: mandatory fork candidate `v4.1`

Activation proposal:
- Stable mainnet height observed on public seeds: `4,400`.
- Option cepat: activation at `4,800`.
- Option aman: activation at `5,400`.
- Do not publish binaries without freezing one of those two heights in the operator announcement.
- Operator notice should explicitly state which option was chosen before rollout starts.

Operator rollout sequence:
1. Stage patched binaries on every node host.
2. Back up each production datadir before replacing binaries.
3. Stop legacy `dutad`.
4. Replace `dutad`, `duta-cli`, and `dutaminer` with the RC bundle artifacts.
5. Restart patched `dutad` and confirm:
   - `getblockchaininfo` answers
   - peer count is non-zero
   - node status returns `ready` before activation
6. If mining is enabled, point miners only at patched node/mining endpoints.
7. Before activation, verify the node advertises and accepts normal work.
8. After activation, monitor:
   - tip height progress
   - best block hash agreement
   - `gettxoutsetinfo`
   - absence of legacy acceptance

Operational guardrails:
- Do not mix patched and legacy binaries on the same active node after activation.
- Do not reuse a production datadir for downgrade after activation.
- Keep at least one backup copy of the pre-upgrade datadir until the fork is stable.
- Keep release artifact hashes with the deployed binaries.

Runtime checks after upgrade:
- `getblockchaininfo`:
  verify `blocks`, `headers`, `bestblockhash`, and `verificationprogress`.
- `getchaintips`:
  verify only the active patched chain remains.
- `gettxoutsetinfo`:
  verify `height`, `txouts`, and `total_amount` remain coherent.
- Sample a known coinbase output with `gettxout`:
  verify the output is still present after restart/recovery.

Incident handling:
- If node fails before activation:
  stop the service, restore the backup datadir if needed, and investigate offline.
- If node stalls after activation:
  do not downgrade.
  collect logs, preserve datadir, and compare tip/hash with a healthy patched peer.
- If legacy node is still running after activation:
  treat it as stalled/outdated and upgrade it in place to the patched RC binaries.

Short operator note:
- Upgrade before the announced activation height.
- After activation, legacy blocks and legacy miners are rejected by patched nodes.
- Do not downgrade an activated production datadir back onto legacy binaries.
- Keep one pre-upgrade datadir backup until the network is stable on the patched chain.
