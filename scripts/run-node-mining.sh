#!/usr/bin/env bash
set -euo pipefail
DATA_DIR="${1:-./data/mainnet}"
MINING_BIND="${2:-127.0.0.1:19085}"
cargo run -p dutad -- --datadir "$DATA_DIR" --mining-bind "$MINING_BIND"
