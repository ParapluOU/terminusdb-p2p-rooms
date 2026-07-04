#!/usr/bin/env bash
# Build the browser-side room replica (crates/room-client-wasm) into
# frontend/wasm/. Requirements:
#   rustup target add wasm32-unknown-unknown
#   cargo install wasm-bindgen-cli --version 0.2.126
set -euo pipefail
cd "$(dirname "$0")/.."

cargo build -p room-client-wasm --target wasm32-unknown-unknown --release
wasm-bindgen --target web --out-dir frontend/wasm --no-typescript \
  target/wasm32-unknown-unknown/release/room_client_wasm.wasm
ls -lh frontend/wasm/
