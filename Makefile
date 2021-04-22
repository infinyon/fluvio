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
DEFAULT_SPU=1
REPL=1
DEFAULT_ITERATION=1000
SPU_DELAY=5
SC_AUTH_CONFIG=./src/sc/test-data/auth_config
SKIP_CHECK=--skip-checks
EXTRA_ARG=

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

smoke-test:	test-clean-up	build_test
	$(TEST_BIN) smoke --spu ${DEFAULT_SPU} --local ${TEST_LOG} -r ${REPL} ${SKIP_CHECK} ${EXTRA_ARG} -- --producer-iteration=${DEFAULT_ITERATION}

smoke-test-stream:	test-clean-up	build_test
	$(TEST_BIN) smoke --spu ${DEFAULT_SPU} --local ${TEST_LOG} ${SKIP_CHECK} -- --consumer-wait=true --producer-iteration=${DEFAULT_ITERATION}

smoke-test-tls:	test-clean-up build_test
	$(TEST_BIN) smoke --spu ${DEFAULT_SPU} --tls --local ${TEST_LOG} ${SKIP_CHECK} -- --producer-iteration=${DEFAULT_ITERATION}

smoke-test-tls-policy:	test-clean-up build_test
	AUTH_POLICY=$(SC_AUTH_CONFIG)/policy.json X509_AUTH_SCOPES=$(SC_AUTH_CONFIG)/scopes.json  \
	FLV_SPU_DELAY=$(SPU_DELAY) \
	$(TEST_BIN) smoke --spu ${DEFAULT_SPU} --tls --local ${TEST_LOG} ${SKIP_CHECK} --keep-cluster -- --producer-iteration=${DEFAULT_ITERATION}

# test rbac with ROOT user
smoke-test-tls-root:	smoke-test-tls-policy test-permission-user1

# test rbac with user1 who doesn't have topic creation permission
# assumes cluster is set
SC_HOST=localhost
test-permission-user1:
	rm -f /tmp/topic.err
	- $(FLUVIO_BIN) --cluster ${SC_HOST}:9003 \
		--tls --enable-client-cert --domain fluvio.local \
		--ca-cert tls/certs/ca.crt --client-cert tls/certs/client-user1.crt --client-key tls/certs/client-user1.key \
		 topic create test3 2> /tmp/topic.err
	grep -q permission /tmp/topic.err

k8-setup:
	$(FLUVIO_BIN) cluster start --setup --develop
#	$(FLUVIO_BIN) cluster check --pre-install


smoke-test-k8:	test-clean-up minikube_image
	$(TEST_BIN)	smoke --spu ${DEFAULT_SPU} --develop ${TEST_LOG} ${SKIP_CHECK} -- --producer-iteration=${DEFAULT_ITERATION}

smoke-test-k8-tls:	test-clean-up minikube_image
	$(TEST_BIN) smoke --spu ${DEFAULT_SPU} --tls --develop ${TEST_LOG} ${SKIP_CHECK} -- --producer-iteration=${DEFAULT_ITERATION}

smoke-test-k8-tls-policy:	test-clean-up minikube_image
	kubectl create configmap authorization --from-file=POLICY=${SC_AUTH_CONFIG}/policy.json --from-file=SCOPES=${SC_AUTH_CONFIG}/scopes.json
	FLV_SPU_DELAY=$(SPU_DELAY) \
	$(TEST_BIN) \
		smoke \
		--spu ${DEFAULT_SPU} \
		--tls \
		--develop \
		${TEST_LOG} \
		--authorization-config-map authorization \
		${SKIP_CHECK} \
		--keep-cluster \
		-- \
		--producer-iteration=${DEFAULT_ITERATION}

test-permission-k8:	SC_HOST=$(shell kubectl get svc fluvio-sc-public -o json | jq '.status.loadBalancer.ingress[0].ip' | tr -d '"' )
test-permission-k8:	test-permission-user1

smoke-test-k8-tls-root:	smoke-test-k8-tls-policy test-permission-k8

# test rbac
#
#
#

test-rbac:
	AUTH_POLICY=$(POLICY_FILE) X509_AUTH_SCOPES=$(SCOPE) make smoke-test-tls DEFAULT_LOG=fluvio=debug


test-clean-up:	build_test
ifeq ($(UNINSTALL),noclean)
	echo "no clean"
else
	echo "clean up previous installation"
	$(FLUVIO_BIN) cluster delete
	$(FLUVIO_BIN) cluster delete --local
	kubectl delete configmap authorization --ignore-not-found
endif



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
	cargo +$(RUSTV) clippy --all --all-targets --all-features --tests -- -D warnings -A clippy::upper_case_acronyms

build-all-test:
	cargo build --lib --tests --all-features $(RELEASE_FLAG) $(TARGET_FLAG) $(VERBOSE_FLAG)

check-all-test:
	cargo check --lib --tests --all-features $(TARGET_FLAG) $(VERBOSE_FLAG)

test_tls_multiplex:
	cd src/socket; cargo test test_multiplexing_native_tls

build_filter_wasm:
	rustup target add wasm32-unknown-unknown 
	make -C smart_filter build_test

run-all-unit-test: test_tls_multiplex build_filter_wasm
	cargo test --lib --all-features $(RELEASE_FLAG) $(TARGET_FLAG)
	cargo test -p fluvio-storage $(RELEASE_FLAG) $(TARGET_FLAG)

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
