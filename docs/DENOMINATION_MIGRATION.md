# Denomination Migration Impact

This codebase now treats `dut` as the consensus/base unit and `DUTA` as the display unit.

## Consensus Layer

- On-chain values remain integer amounts.
- Block subsidy, fees, UTXO values, and transaction outputs are all stored and validated in `dut`.
- No floating-point amount handling was introduced.

## RPC And Wallet Surface

- Display-facing fields such as `amount`, `fee`, `change`, and `balance` are formatted in `DUTA`.
- Raw/base-unit values are exposed as `*_dut`.
- Unit metadata is exposed as:
  - `unit = "DUTA"`
  - `display_unit = "DUTA"`
  - `base_unit = "dut"`
  - `decimals = 8`

## Migration Impact

- Existing wallets remain usable because wallet/database state already stores integer amounts.
- Existing automation that read `amount` as a raw integer must migrate to `amount_dut`.
- Existing automation that submits display amounts can continue to use decimal `DUTA` strings.
- No wallet file conversion is required solely because of the denomination change.

## Operator Action

- Prefer raw `*_dut` fields for automation and accounting.
- Treat `amount`, `fee`, `change`, and `balance` as display-layer fields only.
- Validate any older `/utxo` consumers: that endpoint now aligns `amount` with the display layer and keeps raw values in `amount_dut`.
