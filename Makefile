VERSION := $(shell cat VERSION)
RUSTV?=stable
GIT_COMMIT=$(shell git rev-parse HEAD)
ARCH=$(shell uname -m)
TARGET?=
IMAGE_VERSION?=					# If set, this indicates that the image is pre-built and should not be built
BUILD_PROFILE=$(if $(RELEASE),release,debug)
CARGO_BUILDER=$(if $(findstring arm,$(TARGET)),cross,cargo) # If TARGET contains the substring "arm"
FLUVIO_BIN=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/fluvio,./target/$(BUILD_PROFILE)/fluvio)
RELEASE_FLAG=$(if $(RELEASE),--release,)
TARGET_FLAG=$(if $(TARGET),--target $(TARGET),)
VERBOSE_FLAG=$(if $(VERBOSE),--verbose,)
CLIENT_LOG=warn
SERVER_LOG=info
TEST_BIN=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/fluvio-test,./target/$(BUILD_PROFILE)/fluvio-test)
DEFAULT_SPU=2
REPL=2
DEFAULT_ITERATION=1000
SPU_DELAY=5
SC_AUTH_CONFIG=./crates/fluvio-sc/test-data/auth_config
EXTRA_ARG=

# Test env
TEST_ENV_AUTH_POLICY=
TEST_ENV_FLV_SPU_DELAY=

# Test args
TEST_ARG_SPU=--spu ${DEFAULT_SPU}
TEST_ARG_LOG=--client-log ${CLIENT_LOG} --server-log ${SERVER_LOG}
TEST_ARG_REPLICATION=-r ${REPL}
TEST_ARG_DEVELOP=$(if $(IMAGE_VERSION),--image-version ${IMAGE_VERSION}, --develop)
TEST_ARG_SKIP_CHECKS=
TEST_ARG_EXTRA=
TEST_ARG_CONSUMER_WAIT=
TEST_ARG_PRODUCER_ITERATION=--producer-iteration=${DEFAULT_ITERATION}

export PATH := $(shell pwd)/target/$(BUILD_PROFILE):${PATH}


# install all tools required
install_tools_mac:
	brew install yq
	brew install helm

helm_pkg:	
	make -C k8-util/helm package


build-cli: install_rustup_target
	$(CARGO_BUILDER) build --bin fluvio $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)


build-cli-minimal: install_rustup_target
	# https://github.com/infinyon/fluvio/issues/1255
	cargo build --bin fluvio $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG) --no-default-features --features consumer --manifest-path ./crates/fluvio-cli/Cargo.toml


build-cluster: install_rustup_target
	cargo build --bin fluvio-run $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)

build-test:	install_rustup_target 
	cargo build --bin fluvio-test $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)

install_rustup_target:
	./build-scripts/install_target.sh

#
# List of smoke test steps.  This is used by CI
#

smoke-test: test-setup
	# Set ENV
	$(TEST_ENV_AUTH_POLICY) \
	$(TEST_ENV_FLV_SPU_DELAY) \
		$(TEST_BIN) smoke \
			${TEST_ARG_SPU} \
			${TEST_ARG_LOG} \
			${TEST_ARG_REPLICATION} \
			${TEST_ARG_DEVELOP} \
			${TEST_ARG_EXTRA} \
			-- \
			${TEST_ARG_CONSUMER_WAIT} \
			${TEST_ARG_PRODUCER_ITERATION}

smoke-test-local: TEST_ARG_EXTRA=--local  $(EXTRA_ARG)
smoke-test-local: smoke-test

smoke-test-stream: TEST_ARG_EXTRA=--local $(EXTRA_ARG)
smoke-test-stream: TEST_ARG_CONSUMER_WAIT=--consumer-wait=true
smoke-test-stream: smoke-test

smoke-test-tls: TEST_ARG_EXTRA=--tls --local $(EXTRA_ARG)
smoke-test-tls: smoke-test

smoke-test-tls-policy: TEST_ENV_AUTH_POLICY=AUTH_POLICY=$(SC_AUTH_CONFIG)/policy.json X509_AUTH_SCOPES=$(SC_AUTH_CONFIG)/scopes.json
smoke-test-tls-policy: TEST_ENV_FLV_SPU_DELAY=FLV_SPU_DELAY=$(SPU_DELAY)
smoke-test-tls-policy: TEST_ARG_EXTRA=--tls --local --skip-checks $(EXTRA_ARG)
smoke-test-tls-policy: smoke-test

# test rbac with ROOT user
smoke-test-tls-root: smoke-test-tls-policy test-permission-user1

# election test only runs on local
election-test: TEST_ARG_EXTRA=--local $(EXTRA_ARG)	
election-test: test-setup
	$(TEST_BIN) election  \
		${TEST_ARG_SPU} \
		${TEST_ARG_LOG} \
		${TEST_ARG_REPLICATION} \
		${TEST_ARG_DEVELOP} \
		${TEST_ARG_EXTRA}

multiple-partition-test: TEST_ARG_EXTRA=--local $(EXTRA_ARG)
multiple-partition-test: test-setup
	$(TEST_BIN) multiple_partition --partition 10 \
		${TEST_ARG_SPU} \
                ${TEST_ARG_LOG} \
                ${TEST_ARG_REPLICATION} \
                ${TEST_ARG_DEVELOP} \
                ${TEST_ARG_EXTRA}


reconnection-test: TEST_ARG_EXTRA=--local $(EXTRA_ARG)
reconnection-test: DEFAULT_SPU=1
reconnection-test: REPL=1
reconnection-test: test-setup
	$(TEST_BIN) reconnection  \
                ${TEST_ARG_SPU} \
                ${TEST_ARG_LOG} \
                ${TEST_ARG_REPLICATION} \
                ${TEST_ARG_DEVELOP} \
                ${TEST_ARG_EXTRA}

# test rbac with user1 who doesn't have topic creation permission
# assumes cluster is set
SC_HOST=localhost
SC_PORT=9003
test-permission-user1:
	rm -f /tmp/topic.err
	- $(FLUVIO_BIN) --cluster ${SC_HOST}:${SC_PORT} \
		--tls --enable-client-cert --domain fluvio.local \
		--ca-cert tls/certs/ca.crt \
		--client-cert tls/certs/client-user1.crt \
		--client-key tls/certs/client-user1.key \
		 topic create test3 2> /tmp/topic.err
	grep -q permission /tmp/topic.err


k8-setup:	ensure_fluvio_bin
	$(FLUVIO_BIN) cluster start --setup --develop


ifeq (${CI},true)
ensure_fluvio_bin:
else
# When not in CI (i.e. development), need build cli
ensure_fluvio_bin: build-cli
endif



# Kubernetes Tests

smoke-test-k8: TEST_ARG_EXTRA=$(EXTRA_ARG)
smoke-test-k8: build_k8_image smoke-test 

smoke-test-k8-tls: TEST_ARG_EXTRA=--tls $(EXTRA_ARG)
smoke-test-k8-tls: build_k8_image smoke-test

smoke-test-k8-tls-policy-setup:
	kubectl delete configmap authorization --ignore-not-found
	kubectl create configmap authorization --from-file=POLICY=${SC_AUTH_CONFIG}/policy.json --from-file=SCOPES=${SC_AUTH_CONFIG}/scopes.json
smoke-test-k8-tls-policy: TEST_ENV_FLV_SPU_DELAY=FLV_SPU_DELAY=$(SPU_DELAY)
smoke-test-k8-tls-policy: TEST_ARG_EXTRA=--tls --authorization-config-map authorization $(EXTRA_ARG)
smoke-test-k8-tls-policy: build_k8_image smoke-test-k8-tls-policy-setup smoke-test

test-permission-k8:	SC_HOST=$(shell kubectl get node -o json | jq '.items[].status.addresses[0].address' | tr -d '"' )
test-permission-k8:	SC_PORT=$(shell kubectl get svc fluvio-sc-public -o json | jq '.spec.ports[0].nodePort' )
test-permission-k8:	test-permission-user1

smoke-test-k8-tls-root: smoke-test-k8-tls-policy test-permission-k8


ifeq (${CI},true)
# In CI, we expect all artifacts to already be built and loaded for the script
upgrade-test:
	FLUVIO_BIN=./fluvio ./tests/upgrade-test.sh
else
# When not in CI (i.e. development), load the dev k8 image before running test
upgrade-test: build-cli build_k8_image
	./tests/upgrade-test.sh
endif

# When running in development, might need to run `cargo clean` to ensure correct fluvio binary is used
validate-release-stable:
	./tests/fluvio-validate-release.sh $(VERSION) $(GIT_COMMIT)

ifeq (${CI},true)
# In CI, we expect all artifacts to already be built and loaded for the script
longevity-test:
	$(TEST_BIN) longevity -- --runtime-seconds=1800 --producers 10 --consumers 10
else
# When not in CI (i.e. development), load the dev k8 image before running test
longevity-test: build-test
	$(TEST_BIN) longevity -- $(VERBOSE_FLAG) --runtime-seconds=60
endif

cli-platform-cross-version-test:
	bats -t ./tests/cli/cli-platform-cross-version.bats

cli-smoke:
	bats $(shell ls -1 ./tests/cli/smoke_tests/*.bats | sort -R)

# test rbac
#
#
#

test-rbac:
	AUTH_POLICY=$(POLICY_FILE) X509_AUTH_SCOPES=$(SCOPE) make smoke-test-tls DEFAULT_LOG=fluvio=debug

# In CI mode, do not run any build steps
ifeq (${CI},true)
build-test-ci:
else
# When not in CI (i.e. development), need build cli and cluster
build-test-ci: build-test build-cli build-cluster
endif


ifeq ($(UNINSTALL),noclean)
clean_cluster:
	echo "no clean"
else
clean_cluster:
	echo "clean up previous installation"
	$(FLUVIO_BIN) cluster delete
	$(FLUVIO_BIN) cluster delete --local
endif

test-setup:	build-test-ci clean_cluster


#
#  Various Lint tools
#

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

build_smartmodules:
	make -C crates/fluvio-smartmodule/examples build

run-all-unit-test: install_rustup_target
	cargo test --lib --all-features $(RELEASE_FLAG) $(TARGET_FLAG)
	cargo test -p fluvio-smartmodule $(RELEASE_FLAG) $(TARGET_FLAG)
	cargo test -p fluvio-storage $(RELEASE_FLAG) $(TARGET_FLAG)
	make test-all -C crates/fluvio-protocol

run-integration-test: build_smartmodules install_rustup_target
	cargo test  --lib --all-features $(RELEASE_FLAG) $(TARGET_FLAG) -p fluvio-spu -- --ignored --test-threads=1
	cargo test  --lib --all-features $(RELEASE_FLAG) $(TARGET_FLAG) -p fluvio-socket -- --ignored --test-threads=1


run-k8-test:	install_rustup_target k8-setup build_k8_image
	cargo test --lib  -p fluvio-sc  -- --ignored --test-threads=1


run-all-doc-test: install_rustup_target 
	cargo test --all-features --doc  $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)

run-client-doc-test: install_rustup_target 
	cargo test --all-features --doc -p fluvio-cli $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)
	cargo test --all-features --doc -p fluvio-cluster $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)
	cargo test --all-features --doc -p fluvio $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)



build_k8_image: K8_CLUSTER?=$(shell ./k8-util/cluster/cluster-type.sh)

# In CI mode, do not build k8 image
ifeq (${CI},true)
build_k8_image:
else ifeq (${IMAGE_VERSION},true)
build_k8_image:
else
# When not in CI (i.e. development), build image before testing
build_k8_image: fluvio_image
endif


# Build docker image for Fluvio.
ifndef TARGET
ifeq ($(ARCH),arm64)
fluvio_image: TARGET= aarch64-unknown-linux-musl
else
fluvio_image: TARGET=x86_64-unknown-linux-musl
endif
endif
fluvio_image: fluvio_run_bin
	echo "Building Fluvio $(TARGET) image with tag: $(GIT_COMMIT) k8 type: $(K8_CLUSTER)"
	k8-util/docker/build.sh $(TARGET) $(GIT_COMMIT) "./target/$(TARGET)/$(BUILD_PROFILE)/fluvio-run" $(K8_CLUSTER)



fluvio_run_bin: install_rustup_target
	cargo build --bin fluvio-run $(RELEASE_FLAG) --target $(TARGET)


# upgrade existing cluster
upgrade: build-cli build_k8_image
	$(FLUVIO_BIN) cluster upgrade --sys
	$(FLUVIO_BIN) cluster upgrade --rust-log $(SERVER_LOG) --develop

clean_charts:
	make -C k8-util/helm clean

clean:	clean_charts
	cargo clean


.EXPORT_ALL_VARIABLES:
FLUVIO_BUILD_ZIG ?= zig
FLUVIO_BUILD_LLD ?= lld
CC_aarch64_unknown_linux_musl=$(PWD)/build-scripts/aarch64-linux-musl-zig-cc
CC_x86_64_unknown_linux_musl=$(PWD)/build-scripts/x86_64-linux-musl-zig-cc
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=$(PWD)/build-scripts/ld.lld
CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=$(PWD)/build-scripts/ld.lld
