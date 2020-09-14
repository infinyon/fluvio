test-all:	test-crates test-transport

test-crates:
	cargo test
	cargo test -p kf-protocol-api
	cargo test -p kf-protocol-core
	cargo test -p kf-protocol-derive
	cargo test -p kf-protocol-fs


test-transport:
	cd kf-protocol-transport; cargo test
