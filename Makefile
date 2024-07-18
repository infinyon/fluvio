VERSION ?= $(shell cat VERSION)
RUSTV?=stable
GIT_COMMIT=$(shell git rev-parse HEAD)
ARCH=$(shell uname -m)
TARGET?=
IMAGE_VERSION?=					# If set, this indicates that the image is pre-built and should not be built
BUILD_PROFILE=$(if $(RELEASE),release,debug)
CARGO_BUILDER?=cargo
FLUVIO_BIN?=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/fluvio,./target/$(BUILD_PROFILE)/fluvio)
SMDK_BIN?=$(if $(TARGET),$(shell pwd)/target/$(TARGET)/$(BUILD_PROFILE)/smdk,$(shell pwd)/target/$(BUILD_PROFILE)/smdk)
CDK_BIN?=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/cdk,./target/$(BUILD_PROFILE)/cdk)
RELEASE_FLAG=$(if $(RELEASE),--release,)
TARGET_FLAG=$(if $(TARGET),--target $(TARGET),)
VERBOSE_FLAG=$(if $(VERBOSE),--verbose,)
DEBUG_SMARTMODULE_FLAG=$(if $(DEBUG_SMARTMODULE),--features wasi,)
SMARTENGINE_FLAG=$(if $(SMARTENGINE),--features smartengine,)

BUILD_FLAGS = $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)

include makefiles/build.mk
include makefiles/test.mk
include makefiles/check.mk
include makefiles/release.mk
include crates/fluvio-storage/Makefile


# misc stuff

helm_pkg:
	make -C k8-util/helm package

clean:
	cargo clean
	make -C k8-util/helm clean
	make -C smartmodule/examples clean


.EXPORT_ALL_VARIABLES:
FLUVIO_BUILD_ZIG ?= zig
CC_aarch64_unknown_linux_musl=$(PWD)/build-scripts/aarch64-linux-musl-zig-cc
CC_x86_64_unknown_linux_musl=$(PWD)/build-scripts/x86_64-linux-musl-zig-cc
CC_arm_unknown_linux_gnueabihf=${PWD}/build-scripts/arm-linux-gnu-zig-cc
CC_armv7_unknown_linux_gnueabihf=${PWD}/build-scripts/arm-linux-gnu-zig-cc
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=$(PWD)/build-scripts/aarch64-linux-musl-zig-cc
CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=$(PWD)/build-scripts/x86_64-linux-musl-zig-cc
CARGO_TARGET_ARM_UNKNOWN_LINUX_GNUEABIHF_LINKER=${PWD}/build-scripts/arm-linux-gnu-zig-cc
CARGO_TARGET_ARMV7_UNKNOWN_LINUX_GNUEABIHF_LINKER=${PWD}/build-scripts/arm-linux-gnu-zig-cc
