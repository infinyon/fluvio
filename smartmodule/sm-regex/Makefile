default: build

build:
	rustup target add wasm32-unknown-unknown
	cargo build --profile release-lto --target wasm32-unknown-unknown
