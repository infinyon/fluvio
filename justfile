
default:
	just -l

# arm7 spu build (works)
build-spu:
	cargo build --target=armv7-unknown-linux-gnueabi -p fluvio-spu --no-default-features --release

# cross build armv7 (works)
build-spu-cross:
	cross build --target=armv7-unknown-linux-gnueabi -p fluvio-spu --no-default-features

# cross build armv7 musl (works)
build-spu-cross-musl:
	cross build --target=armv7-unknown-linux-musleabi -p fluvio-spu --no-default-features --release

# cargo build armv7 musl (issue)
build-spu-musl:
	cargo build --target=armv7-unknown-linux-musleabi -p fluvio-spu --no-default-features --release

#  cargo zigbuild armv7 gnu (link issue)
build-spu-zig-gnu:
	cargo zigbuild --target=armv7-unknown-linux-gnueabi -p fluvio-spu --no-default-features --release

# cargo zigbuild armv7 musl (issue)
build-spu-zig-musl:
	cargo zigbuild --target=armv7-unknown-linux-musleabi -p fluvio-spu --no-default-features --release


# build native default config
build-spu-se: clean-spu
	cargo build -p fluvio-spu

# build native, no smartengine
build-spu2:
	cargo build -p fluvio-spu --no-default-features

# build w/ and w/o smartengine feature enabled
build-spu-feat: clean-spu
	just build-spu-se
	just clean-spu
	just build-spu

clean-spu:
	cargo clean -p fluvio-spu

# not running in arm7 for quick dev, in
run: build-spu2
	cargo run -p fluvio-spu -- \
		--id 1
