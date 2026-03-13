#!/usr/bin/env bash
set -euo pipefail
DATA_DIR="${1:-./data/mainnet}"
MINING_BIND="${2:-0.0.0.0:19085}"
cargo run -p dutad -- --datadir "$DATA_DIR" --mining-bind "$MINING_BIND"
