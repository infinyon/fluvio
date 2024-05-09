default: build

setup:
	rustup target add wasm32-wasi

build: setup
	cargo run --bin smdk -- build

build-raw:
	cargo build --target wasm32-wasi

