default: build

build:
	rustup target add wasm32-unknown-unknown
	rustup target add wasm32-wasi
	cargo build --release

clean:
	cargo clean
