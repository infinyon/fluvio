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


