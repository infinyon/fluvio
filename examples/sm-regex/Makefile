default: build

build:
	rustup target add wasm32-unknown-unknown
	cargo build --profile release-lto --package fluvio-sm-regex --target wasm32-unknown-unknown
