default: build

build:
	rustup target add wasm32-unknown-unknown
	cargo build --profile release-lto --package fluvio-wasm-component --target wasm32-unknown-unknown
