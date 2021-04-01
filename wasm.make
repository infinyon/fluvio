wasm_target:
	rustup target add wasm32-unknown-unknown

build_filter: wasm_target
	cargo build --release --target wasm32-unknown-unknown --package fluvio-filter-simple

check_filter: wasm_target
	cargo check --target wasm32-unknown-unknown --package fluvio-filter-simple
