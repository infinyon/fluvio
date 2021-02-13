RUSTV = stable

build:
	cargo build --all-features 

test-all:	test-crates test-codec

test-crates:
	cargo test
	cargo test -p fluvio-protocol-api
	cargo test -p fluvio-protocol-core
	cargo test -p fluvio-protocol-derive


# can't test codec as package due to dev-dependencies
test-codec:
	cd fluvio-protocol-codec;cargo test


install-fmt:
	rustup component add rustfmt --toolchain $(RUSTV)

check-fmt:	install-fmt
	cargo +$(RUSTV) fmt -- --check


install-clippy:
	rustup component add clippy --toolchain $(RUSTV)

check-clippy:	install-clippy
	cargo +$(RUSTV) clippy --all-targets --all-features -- -D warnings