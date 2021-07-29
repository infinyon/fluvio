VERSION := $(shell cat VERSION)
RUSTV?=stable
GITHUB_TAG=v$(VERSION)
GIT_COMMIT=$(shell git rev-parse HEAD)
DOCKER_TAG=$(VERSION)-$(GIT_COMMIT)
DOCKER_REGISTRY=infinyon
K8_CLUSTER?=$(shell ./k8-util/cluster/cluster-type.sh)
DOCKER_IMAGE=$(DOCKER_REGISTRY)/fluvio
TARGET_MUSL=x86_64-unknown-linux-musl
TARGET?=
BUILD_PROFILE=$(if $(RELEASE),release,debug)
CARGO_BUILDER=$(if $(findstring arm,$(TARGET)),cross,cargo) # If TARGET contains the substring "arm"
FLUVIO_BIN=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/fluvio,./target/$(BUILD_PROFILE)/fluvio)
RELEASE_FLAG=$(if $(RELEASE),--release,)
TARGET_FLAG=$(if $(TARGET),--target $(TARGET),)
VERBOSE_FLAG=$(if $(VERBOSE),--verbose,)
CLIENT_LOG=warn
SERVER_LOG=fluvio=debug
TEST_BIN=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/flv-test,./target/$(BUILD_PROFILE)/flv-test)
TEST_LOG=--client-log ${CLIENT_LOG} --server-log ${SERVER_LOG}
DEFAULT_SPU=2
REPL=2
DEFAULT_ITERATION=1000
SPU_DELAY=5
SC_AUTH_CONFIG=./src/sc/test-data/auth_config
EXTRA_ARG=

# Test env
TEST_ENV_AUTH_POLICY=
TEST_ENV_FLV_SPU_DELAY=

# Test args
TEST_ARG_SPU=--spu ${DEFAULT_SPU}
TEST_ARG_LOG=--client-log ${CLIENT_LOG} --server-log ${SERVER_LOG}
TEST_ARG_REPLICATION=-r ${REPL}
TEST_ARG_DEVELOP=--develop
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

build-cli: install_rustup_target helm_pkg
	$(CARGO_BUILDER) build --bin fluvio $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)

build-cli-minimal: install_rustup_target
	# https://github.com/infinyon/fluvio/issues/1255
	cargo build --bin fluvio $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG) --no-default-features --features consumer --manifest-path ./src/cli/Cargo.toml


build-cluster: install_rustup_target
	cargo build --bin fluvio-run $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)

build-test:	
	cargo build --bin flv-test $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)

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
smoke-test-tls-policy: TEST_ARG_EXTRA=--tls --local --skip-checks --keep-cluster
smoke-test-tls-policy: smoke-test

# test rbac with ROOT user
smoke-test-tls-root: smoke-test-tls-policy test-permission-user1

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

k8-setup:
	$(FLUVIO_BIN) cluster start --setup --develop

# Kubernetes Tests

smoke-test-k8: TEST_ARG_EXTRA=$(EXTRA_ARG)
smoke-test-k8: build_k8_image smoke-test

smoke-test-k8-tls: TEST_ARG_EXTRA=--tls $(EXTRA_ARG)
smoke-test-k8-tls: build_k8_image smoke-test

smoke-test-k8-tls-policy-setup:
	kubectl delete configmap authorization --ignore-not-found
	kubectl create configmap authorization --from-file=POLICY=${SC_AUTH_CONFIG}/policy.json --from-file=SCOPES=${SC_AUTH_CONFIG}/scopes.json
smoke-test-k8-tls-policy: TEST_ENV_FLV_SPU_DELAY=FLV_SPU_DELAY=$(SPU_DELAY)
smoke-test-k8-tls-policy: TEST_ARG_EXTRA=--tls --authorization-config-map authorization --keep-cluster
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
upgrade-test: build_k8_image
	DEBUG=true ./tests/upgrade-test.sh
endif


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
# When not in CI (i.e. development), build before testing
build-test-ci: build-test
endif


test-setup:	build-test-ci
ifeq ($(UNINSTALL),noclean)
	echo "no clean"
else
	echo "clean up previous installation"
	$(FLUVIO_BIN) cluster delete
	$(FLUVIO_BIN) cluster delete --local
endif

# Test multiplexor
# this should generate error in multiplexor
test-multiplexor:
	make smoke-test DEFAULT_ITERATION=4  EXTRA_ARG=--keep-cluster

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
check-clippy: install-clippy install_rustup_target helm_pkg
	cargo +$(RUSTV) check --all --all-features --tests $(VERBOSE_FLAG) $(TARGET_FLAG)
	cargo +$(RUSTV) clippy --all --all-features --tests $(VERBOSE_FLAG) -- -D warnings -A clippy::upper_case_acronyms $(TARGET_FLAG)

build_smartstreams:
	make -C src/smartstream/examples build

run-all-unit-test: build_smartstreams install_rustup_target helm_pkg
	cargo test --lib --all-features $(RELEASE_FLAG) $(TARGET_FLAG)
	cargo test -p fluvio-storage $(RELEASE_FLAG) $(TARGET_FLAG)
	make test-all -C src/protocol

run-integration-test:build_smartstreams install_rustup_target helm_pkg
	cargo test  --lib --all-features $(RELEASE_FLAG) $(TARGET_FLAG) -- --ignored --test-threads=1

run-all-doc-test: install_rustup_target helm_pkg
	cargo test --all-features --doc  $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)

run-client-doc-test: install_rustup_target helm_pkg
	cargo test --all-features --doc -p fluvio-cli $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)
	cargo test --all-features --doc -p fluvio-cluster $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)
	cargo test --all-features --doc -p fluvio $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)


clean_build:
	rm -rf /tmp/cli-*


release:	update_version release_image helm_publish_app publish_cli

# This needed to be run every time we increment VERSION
update_version:
	cp VERSION	src/cli/src


#
# Docker actions
#
release_image: RELEASE=true
release_image: fluvio_image
	docker tag $(DOCKER_IMAGE):$(GIT_COMMIT) $(DOCKER_IMAGE):$(VERSION)
	docker push $(DOCKER_IMAGE):$(VERSION)

# Build latest image and push to Docker registry
latest_image: RELEASE=true
latest_image: fluvio_image
	docker tag $(DOCKER_IMAGE):$(GIT_COMMIT) $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker tag $(DOCKER_IMAGE):$(GIT_COMMIT) $(DOCKER_IMAGE):latest
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker push $(DOCKER_IMAGE):latest


# In CI mode, do not build k8 image
ifeq (${CI},true)
build_k8_image:
else
# When not in CI (i.e. development), build image before testing
build_k8_image: fluvio_image
endif


# Build docker image for Fluvio.
# This use musl target
fluvio_image: fluvio_bin_musl
	echo "Building Fluvio musl image with tag: $(GIT_COMMIT) k8 type: $(K8_CLUSTER)"
	k8-util/docker/build.sh $(GIT_COMMIT) "./target/$(TARGET_MUSL)/$(BUILD_PROFILE)/fluvio-run" $(K8_CLUSTER)

fluvio_bin_musl:
	rustup target add $(TARGET_MUSL)
	cargo build --bin fluvio-run $(RELEASE_FLAG) --target $(TARGET_MUSL)

make publish_fluvio_image:
	curl \
	-X POST \
	-H "Accept: application/vnd.github.v3+json" \
	-H "Authorization: $(GITHUB_ACCESS_TOKEN)" \
	https://api.github.com/repos/infinyon/fluvio/actions/workflows/2333005/dispatches \
	-d '{"ref":"master"}'


.EXPORT_ALL_VARIABLES:
FLUVIO_BUILD_ZIG ?= zig
FLUVIO_BUILD_LLD ?= lld
CC_aarch64_unknown_linux_musl=$(PWD)/build-scripts/aarch64-linux-musl-zig-cc
CC_x86_64_unknown_linux_musl=$(PWD)/build-scripts/x86_64-linux-musl-zig-cc
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=$(PWD)/build-scripts/ld.lld
CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=$(PWD)/build-scripts/ld.lld
