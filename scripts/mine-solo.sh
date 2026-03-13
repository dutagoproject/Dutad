#!/usr/bin/env bash
set -euo pipefail
RPC="${1:-http://127.0.0.1:19085}"
ADDRESS="${2:-YOUR_DUTA_ADDRESS}"
THREADS="${3:-8}"
cargo run -p dutad --bin dutaminer -- --rpc "$RPC" --address "$ADDRESS" --threads "$THREADS"
