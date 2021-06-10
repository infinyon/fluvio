VERSION := $(shell cat VERSION)
RUSTV?=stable
GITHUB_TAG=v$(VERSION)
GIT_COMMIT=$(shell git rev-parse HEAD)
DOCKER_TAG=$(VERSION)-$(GIT_COMMIT)
DOCKER_REGISTRY=infinyon
DOCKER_IMAGE=$(DOCKER_REGISTRY)/fluvio
TARGET_LINUX=x86_64-unknown-linux-musl
TARGET_DARWIN=x86_64-apple-darwin
CLI_BUILD=fluvio_cli
BUILD_PROFILE=$(if $(RELEASE),release,debug)
FLUVIO_BIN=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/fluvio,./target/$(BUILD_PROFILE)/fluvio)
RELEASE_FLAG=$(if $(RELEASE),--release,)
TARGET_FLAG=$(if $(TARGET),--target $(TARGET),)
VERBOSE_FLAG=$(if $(VERBOSE),--verbose,)
CLIENT_LOG=warn
SERVER_LOG=fluvio=debug
TEST_BIN=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/flv-test,./target/$(BUILD_PROFILE)/flv-test)
TEST_LOG=--client-log ${CLIENT_LOG} --server-log ${SERVER_LOG}
DEFAULT_ITERATION=1000
SPU_DELAY=5
SC_AUTH_CONFIG=./src/sc/test-data/auth_config
EXTRA_ARG=

# Test env
TEST_ENV_AUTH_POLICY=
TEST_ENV_FLV_SPU_DELAY=

# Test args
TEST_ARG_SPU=--spu 2
TEST_ARG_LOG=--client-log ${CLIENT_LOG} --server-log ${SERVER_LOG}
TEST_ARG_REPLICATION=-r 2
TEST_ARG_LOCAL=
TEST_ARG_DEVELOP=
TEST_ARG_TLS=
TEST_ARG_KEEP_CLUSTER=
TEST_ARG_AUTH_CONFIG_MAP=
TEST_ARG_SKIP_CHECKS=
TEST_ARG_EXTRA=
TEST_ARG_CONSUMER_WAIT=
TEST_ARG_PRODUCER_ITERATION=--producer-iteration=1000

export PATH := $(shell pwd)/target/$(BUILD_PROFILE):${PATH}


# install all tools required
install_tools_mac:
	brew install yq
	brew install helm

build_cli:
	cargo build --bin fluvio $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)

build_cluster: install_test_target
	cargo build --bin fluvio-run $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)

build_test:	build_cluster build_cli
	cargo build --bin flv-test $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)

install_test_target:
ifdef TARGET
	rustup target add $(TARGET)
endif


#
# List of smoke test steps.  This is used by CI
#

smoke-test-base: test-setup smoke-test-base-nobuild
smoke-test-base-nobuild: test-setup-nobuild
	# Set ENV
	AUTH_POLICY=$(TEST_ENV_AUTH_POLICY) \
	FLV_SPU_DELAY=$(TEST_ENV_FLV_SPU_DELAY) \
		$(TEST_BIN) smoke \
			${TEST_ARG_SPU} \
			${TEST_ARG_LOG} \
			${TEST_ARG_TLS} \
			${TEST_ARG_LOCAL} \
			${TEST_ARG_DEVELOP} \
			${TEST_ARG_REPLICATION} \
			${TEST_ARG_KEEP_CLUSTER} \
			${TEST_ARG_AUTH_CONFIG_MAP} \
			${TEST_ARG_SKIP_CHECKS} \
			${TEST_ARG_EXTRA} \
			-- \
			${TEST_ARG_CONSUMER_WAIT} \
			${TEST_ARG_PRODUCER_ITERATION}

smoke-test-nobuild: TEST_ARG_LOCAL=--local
smoke-test-nobuild: TEST_ARG_SKIP_CHECKS=--skip-checks
smoke-test-nobuild: smoke-test-base-nobuild
smoke-test: test-setup smoke-test-nobuild

smoke-test-stream-nobuild: TEST_ARG_SKIP_CHECKS+=--skip-checks
smoke-test-stream-nobuild: TEST_ARG_CONSUMER_WAIT+=--consumer-wait=true
smoke-test-stream-nobuild: smoke-test-base-nobuild
smoke-test-stream: test-setup smoke-test-stream-nobuild

smoke-test-tls-nobuild: TEST_ARG_TLS=--tls
smoke-test-tls-nobuild: TEST_ARG_LOCAL=--local
smoke-test-tls-nobuild: smoke-test-base-nobuild
smoke-test-tls: test-setup smoke-test-tls-nobuild

smoke-test-tls-policy-nobuild: TEST_ENV_AUTH_POLICY=$(SC_AUTH_CONFIG)/policy.json X509_AUTH_SCOPES=$(SC_AUTH_CONFIG)/scopes.json
smoke-test-tls-policy-nobuild: TEST_ENV_FLV_SPU_DELAY=$(SPU_DELAY)
smoke-test-tls-policy-nobuild: TEST_ARG_TLS=--tls
smoke-test-tls-policy-nobuild: TEST_ARG_LOCAL=--local
smoke-test-tls-policy-nobuild: TEST_ARG_SKIP_CHECKS=--skip-checks
smoke-test-tls-policy-nobuild: TEST_ARG_KEEP_CLUSTER=--keep-cluster
smoke-test-tls-policy-nobuild: smoke-test-base-nobuild
smoke-test-tls-policy: test-setup smoke-test-tls-policy-nobuild

# test rbac with ROOT user
smoke-test-tls-root: smoke-test-tls-policy test-permission-user1
smoke-test-tls-root-nobuild: smoke-test-tls-policy-nobuild test-permission-user1

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

smoke-test-k8-nobuild: TEST_ARG_DEVELOP=--develop
smoke-test-k8-nobuild: TEST_ARG_SKIP_CHECKS=--skip-checks
smoke-test-k8-nobuild: smoke-test-base-nobuild
smoke-test-k8: test-setup smoke-test-k8-nobuild

smoke-test-k8-tls-nobuild: TEST_ARG_TLS=--tls
smoke-test-k8-tls-nobuild: TEST_ARG_DEVELOP=--develop
smoke-test-k8-tls-nobuild: TEST_ARG_SKIP_CHECKS=--skip-checks
smoke-test-k8-tls-nobuild: smoke-test-base-nobuild
smoke-test-k8-tls: test-setup smoke-test-k8-tls-nobuild

smoke-test-k8-tls-policy-nobuild-setup:
	kubectl delete configmap authorization --ignore-not-found
	kubectl create configmap authorization --from-file=POLICY=${SC_AUTH_CONFIG}/policy.json --from-file=SCOPES=${SC_AUTH_CONFIG}/scopes.json
smoke-test-k8-tls-policy-nobuild: TEST_ENV_FLV_SPU_DELAY=$(SPU_DELAY)
smoke-test-k8-tls-policy-nobuild: TEST_ARG_TLS=--tls
smoke-test-k8-tls-policy-nobuild: TEST_ARG_DEVELOP=--develop
smoke-test-k8-tls-policy-nobuild: TEST_ARG_AUTH_CONFIG_MAP=--authorization-config-map authorization
smoke-test-k8-tls-policy-nobuild: TEST_ARG_SKIP_CHECKS=--skip-checks
smoke-test-k8-tls-policy-nobuild: TEST_ARG_AUTH_CONFIG_MAP=--authorization-config-map authorization
smoke-test-k8-tls-policy-nobuild: TEST_ARG_KEEP_CLUSTER=--keep-cluster
smoke-test-k8-tls-policy-nobuild: smoke-test-k8-tls-policy-nobuild-setup smoke-test-base-nobuild
smoke-test-k8-tls-policy: test-setup minikube_image smoke-test-k8-tls-policy-nobuild

test-permission-k8:	SC_HOST=$(shell kubectl get node -o json | jq '.items[].status.addresses[0].address' | tr -d '"' )
test-permission-k8:	SC_PORT=$(shell kubectl get svc fluvio-sc-public -o json | jq '.spec.ports[0].nodePort' )
test-permission-k8:	test-permission-user1

smoke-test-k8-tls-root: smoke-test-k8-tls-policy test-permission-k8
smoke-test-k8-tls-root-nobuild: smoke-test-k8-tls-policy-nobuild test-permission-k8

# test rbac
#
#
#

test-rbac:
	AUTH_POLICY=$(POLICY_FILE) X509_AUTH_SCOPES=$(SCOPE) make smoke-test-tls DEFAULT_LOG=fluvio=debug

test-setup: build_test test-setup-nobuild
test-setup-nobuild:
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
check-clippy: install-clippy
	cargo +$(RUSTV) check --all --all-targets --all-features --tests $(VERBOSE_FLAG)
	cargo +$(RUSTV) clippy --all --all-targets --all-features --tests $(VERBOSE_FLAG) -- -D warnings -A clippy::upper_case_acronyms

build_smartstreams:
	make -C src/smartstream/examples build

run-all-unit-test: build_smartstreams
	cargo test --lib --all-features $(RELEASE_FLAG) $(TARGET_FLAG)
	cargo test -p fluvio-storage $(RELEASE_FLAG) $(TARGET_FLAG)
	make test-all -C src/protocol	

run-unstable-test:	build_smartstreams
	cargo test --lib --all-features $(RELEASE_FLAG) $(TARGET_FLAG) -- --ignored

run-all-doc-test:
	cargo test --all-features --doc $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)

install_musl:
	rustup target add ${TARGET_LINUX}

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

# Build docker image in minikube environment
minikube_image: MINIKUBE_FLAG=minikube
minikube_image: fluvio_image

# Build docker image for Fluvio.
# In development, we tag the image with just the git commit.
# In further releases, we re-tag the image as needed.
fluvio_image: fluvio_bin_linux
	echo "Building Fluvio musl image with tag: $(GIT_COMMIT)"
	k8-util/docker/build.sh $(GIT_COMMIT) "./target/x86_64-unknown-linux-musl/$(BUILD_PROFILE)/fluvio-run" $(MINIKUBE_FLAG)

fluvio_bin_linux: install_musl
	export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=x86_64-linux-musl-gcc && \
	export TARGET_CC=x86_64-linux-musl-gcc && \
	cargo build --bin fluvio-run $(RELEASE_FLAG) --target $(TARGET_LINUX)

make publish_fluvio_image:
	curl \
	-X POST \
	-H "Accept: application/vnd.github.v3+json" \
	-H "Authorization: $(GITHUB_ACCESS_TOKEN)" \
	https://api.github.com/repos/infinyon/fluvio/actions/workflows/2333005/dispatches \
	-d '{"ref":"master"}'


#
# Helm actions
#

helm-install-plugin:
	helm plugin install https://github.com/chartmuseum/helm-push.git


helm-login:
	helm repo remove fluvio
	helm repo add fluvio https://gitops:$(HELM_PASSWORD)@charts.fluvio.io

helm-publish-sys:
	helm push k8-util/helm/fluvio-sys --version="$(VERSION)" --force fluvio

helm-publish-app:
	helm push k8-util/helm/fluvio-app --version="$(VERSION)" --force fluvio



#
# Github release actions
#



GITHUB_USER=infinyon
GITHUB_REPO=fluvio
CLI_BINARY=fluvio
BUILD_OUTPUT=/tmp

release_github:	build-cli-darwin build-cli-linux create-gh-release upload-gh-darwin upload-gh-linux

build-cli-darwin:
	rustup target add $(TARGET_DARWIN)
	cargo build --release --bin fluvio --target $(TARGET_DARWIN)

build-cli-linux:
	rustup target add $(TARGET_LINUX)
	cargo build --release --bin fluvio --target $(TARGET_LINUX)



create-gh-release:
	github-release release \
		--user ${GITHUB_USER} \
		--repo ${GITHUB_REPO} \
		--tag ${GITHUB_TAG} \
		--name "${GITHUB_TAG}" \
		--description "${GITHUB_TAG}"

upload-gh-darwin:
	github-release upload \
		--user ${GITHUB_USER} \
		--repo ${GITHUB_REPO} \
		--tag ${GITHUB_TAG} \
		--name "fluvio-$(GITHUB_TAG)-$(TARGET_DARWIN)" \
		--file target/$(TARGET_DARWIN)/release/fluvio

upload-gh-linux:
	github-release upload \
		--user ${GITHUB_USER} \
		--repo ${GITHUB_REPO} \
		--tag ${GITHUB_TAG} \
		--name "fluvio-$(GITHUB_TAG)-$(TARGET_LINUX)" \
		--file target/$(TARGET_LINUX)/release/fluvio



delete-gh-release:
	github-release delete \
	--user ${GITHUB_USER} \
	--repo ${GITHUB_REPO} \
	--tag ${GITHUB_TAG}
