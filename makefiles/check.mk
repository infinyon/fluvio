install-fmt:
	rustup component add rustfmt --toolchain $(RUSTV)

check-fmt:
	cargo +$(RUSTV) fmt -- --check


check_version:
	make check_version -C k8-util/helm

install-clippy:
	rustup component add clippy --toolchain $(RUSTV)

# Use check first to leverage sccache, the clippy piggybacks
check-clippy: install-clippy install_rustup_target
	cargo +$(RUSTV) check --all --all-features --tests $(VERBOSE_FLAG) $(TARGET_FLAG)
	cargo +$(RUSTV) clippy --all --all-features --tests $(VERBOSE_FLAG) -- -D warnings -A clippy::upper_case_acronyms $(TARGET_FLAG)

install-deny:
	cargo install --locked cargo-deny	

check-crate-audit: install-deny
	cargo deny check

build_smartmodules:
	make -C crates/fluvio-smartmodule/examples build

run-all-unit-test: install_rustup_target
	cargo test --lib --all-features $(BUILD_FLAGS)
	cargo test -p fluvio-smartmodule $(BUILD_FLAGS)
	cargo test -p fluvio-storage $(BUILD_FLAGS)
	make test-all -C crates/fluvio-protocol

run-integration-test: build_smartmodules install_rustup_target
	cargo test  --lib --all-features $(BUILD_FLAGS) -p fluvio-spu -- --ignored --test-threads=1
	cargo test  --lib --all-features $(BUILD_FLAGS) -p fluvio-socket -- --ignored --test-threads=1


run-k8-test:	install_rustup_target k8-setup build_k8_image
	cargo test --lib  -p fluvio-sc  -- --ignored --test-threads=1


run-all-doc-test: install_rustup_target 
	cargo test --all-features --doc  $(BUILD_FLAGS)

run-client-doc-test: install_rustup_target 
	cargo test --all-features --doc -p fluvio-cli $(BUILD_FLAGS)
	cargo test --all-features --doc -p fluvio-cluster $(BUILD_FLAGS) 
	cargo test --all-features --doc -p fluvio $(BUILD_FLAGS)


fluvio_run_bin: install_rustup_target
	cargo build --bin fluvio-run $(RELEASE_FLAG) --target $(TARGET)
