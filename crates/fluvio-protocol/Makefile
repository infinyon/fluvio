RUSTV = stable
TARGET_FLAG=$(if $(TARGET),--target $(TARGET),)

build:
	cargo build --all-features $(TARGET_FLAG)

test-all:	test-crates 

test-crates:
	cargo test $(TARGET_FLAG) --all-features

install-fmt:
	rustup component add rustfmt --toolchain $(RUSTV)

check-fmt:	install-fmt
	cargo +$(RUSTV) fmt -- --check


install-clippy:
	rustup component add clippy --toolchain $(RUSTV)

check-clippy:	install-clippy
	cargo +$(RUSTV) clippy --all-targets --all-features -- -D warnings
