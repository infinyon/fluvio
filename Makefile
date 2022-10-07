VERSION ?= $(shell cat VERSION)
RUSTV?=stable
GIT_COMMIT=$(shell git rev-parse HEAD)
ARCH=$(shell uname -m)
TARGET?=
IMAGE_VERSION?=					# If set, this indicates that the image is pre-built and should not be built
BUILD_PROFILE=$(if $(RELEASE),release,debug)
CARGO_BUILDER=$(if $(findstring arm,$(TARGET)),cross,cargo) # If TARGET contains the substring "arm"
FLUVIO_BIN?=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/fluvio,./target/$(BUILD_PROFILE)/fluvio)
SMDK_BIN?=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/smdk,./target/$(BUILD_PROFILE)/smdk)
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


# misc stuff

helm_pkg:
	make -C k8-util/helm package

clean:
	cargo clean
	make -C k8-util/helm clean
	make -C smartmodule/examples clean


.EXPORT_ALL_VARIABLES:
FLUVIO_BUILD_ZIG ?= zig
FLUVIO_BUILD_LLD ?= lld
CC_aarch64_unknown_linux_musl=$(PWD)/build-scripts/aarch64-linux-musl-zig-cc
CC_x86_64_unknown_linux_musl=$(PWD)/build-scripts/x86_64-linux-musl-zig-cc
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=$(PWD)/build-scripts/ld.lld
CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=$(PWD)/build-scripts/ld.lld
