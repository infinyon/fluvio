VERSION := $(shell cat VERSION)
RUSTV=stable
DOCKER_TAG=$(VERSION)
GITHUB_TAG=v$(VERSION)
GIT_COMMIT=$(shell git rev-parse HEAD)
DOCKER_REGISTRY=infinyon
DOCKER_IMAGE=$(DOCKER_REGISTRY)/fluvio
TARGET_LINUX=x86_64-unknown-linux-musl
TARGET_DARWIN=x86_64-apple-darwin
CLI_BUILD=fluvio_cli
FLUVIO_BIN=./target/debug/fluvio
TEST_BIN=FLV_CMD=true RUST_LOG=debug ./target/debug/flv-test
DEFAULT_SPU=1
DEFAULT_ITERATION=5
DEFAULT_LOG=info
SC_AUTH_CONFIG=./src/sc/test-data/auth_config
SPU_DELAY=20

# install all tools required
install_tools_mac:
	brew install yq
	brew install helm

build:
	cargo build

#
# List of smoke test steps.  This is used by CI
#

smoke-test:	test-clean-up
	$(TEST_BIN) --spu ${DEFAULT_SPU} --produce-iteration ${DEFAULT_ITERATION} --local --rust-log ${DEFAULT_LOG}

smoke-test-tls:	test-clean-up
	$(TEST_BIN) --spu ${DEFAULT_SPU} --produce-iteration ${DEFAULT_ITERATION} --tls --local --rust-log ${DEFAULT_LOG}

# test rbac with ROOT user
smoke-test-tls-root:	test-clean-up
	AUTH_POLICY=$(SC_AUTH_CONFIG)/policy.json X509_AUTH_SCOPES=$(SC_AUTH_CONFIG)/scopes.json  \
	FLV_SPU_DELAY=$(SPU_DELAY) \
	$(TEST_BIN) --spu ${DEFAULT_SPU} --produce-iteration ${DEFAULT_ITERATION} --tls --local --rust-log ${DEFAULT_LOG}

# test rbac with user1 who doesn't have topic creation permission
# assumes cluster is set
test-permission-user1-local:
	rm -f /tmp/topic.err
	- $(FLUVIO_BIN) topic create test3 --cluster localhost:9003 --tls --enable-client-cert \
		 --domain fluvio.local --ca-cert tls/certs/ca.crt \
		 --client-cert tls/certs/client-user1.crt --client-key tls/certs/client-user1.key  2> /tmp/topic.err
	grep -q permission /tmp/topic.err
	

smoke-test-k8:	test-clean-up minikube_image
	$(TEST_BIN)	--spu ${DEFAULT_SPU} --produce-iteration ${DEFAULT_ITERATION} --develop --rust-log ${DEFAULT_LOG}

smoke-test-k8-tls:	test-clean-up minikube_image
	$(TEST_BIN) --spu ${DEFAULT_SPU} --produce-iteration ${DEFAULT_ITERATION} --tls --develop --rust-log ${DEFAULT_LOG}

smoke-test-k8-tls-root:	test-clean-up minikube_image
	kubectl create configmap authorization --from-file=POLICY=${SC_AUTH_CONFIG}/policy.json --from-file=SCOPES=${SC_AUTH_CONFIG}/scopes.json
	FLV_SPU_DELAY=$(SPU_DELAY) \
	$(TEST_BIN) \
		--spu ${DEFAULT_SPU} \
		--produce-iteration ${DEFAULT_ITERATION} \
		--tls \
		--develop \
		--rust-log ${DEFAULT_LOG} \
		--authorization-config-map authorization


# test rbac
#
#
#

test-rbac:
	AUTH_POLICY=$(POLICY_FILE) X509_AUTH_SCOPES=$(SCOPE) make smoke-test-tls DEFAULT_LOG=fluvio=debug



test-clean-up:
	$(FLUVIO_BIN) cluster uninstall
	$(FLUVIO_BIN) cluster uninstall --local
	kubectl delete configmap authorization --ignore-not-found

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

check-clippy:	install-clippy
	cargo +$(RUSTV) clippy --all-targets  -- -D warnings
	cd src/client; cargo +$(RUSTV) clippy --all-targets  -- -D warnings



run-all-unit-test:
	cargo test --all

install_musl:
	rustup target add ${TARGET_LINUX}

clean_build:
	rm -rf /tmp/cli-*



release:	update_version release_image helm_publish_app publish_cli

# This needed to be run every time we increment VERSION
update_version:
	cp VERSION	src/cli/src


# need to bump up version
publish_cli:
	cd src/cli;cargo publish



# 
# Docker actions
#
release_image:	RELEASE=true
release_image:	fluvio_image
	docker tag $(DOCKER_IMAGE):$(GIT_COMMIT) $(DOCKER_IMAGE):$(VERSION)
	docker push $(DOCKER_IMAGE):$(VERSION)


latest_image:	RELEASE=true
latest_image:	fluvio_image
	docker tag $(DOCKER_IMAGE):$(GIT_COMMIT) $(DOCKER_IMAGE):latest
	docker push $(DOCKER_IMAGE):latest


nightly_image:	RELEASE=true
nightly_image:	fluvio_image
	docker tag $(DOCKER_IMAGE):$(GIT_COMMIT) $(DOCKER_IMAGE):nightly
	docker push $(DOCKER_IMAGE):nightly

# publish docker image to minikube environment
minikube_image:	MINIKUBE_DOCKER_ENV=true
minikube_image:	fluvio_image

# build docker image for fluvio using release mode
# this will tag with current git tag
fluvio_image: CARGO_PROFILE=$(if $(RELEASE),release,debug)
fluvio_image: fluvio_bin_linux
	echo "Building Fluvio musl image with version: $(VERSION)"
	export CARGO_PROFILE=$(if $(RELEASE),release,debug); \
	export MINIKUBE_DOCKER_ENV=$(MINIKUBE_DOCKER_ENV); \
	export DOCKER_TAG=$(GIT_COMMIT); \
	k8-util/docker/build.sh


fluvio_bin_linux: RELEASE_FLAG=$(if $(RELEASE),--release,)
fluvio_bin_linux: install_musl
	cd src/cli; cargo build $(RELEASE_FLAG) --no-default-features  \
		--features cluster_components_rustls --bin fluvio --target $(TARGET_LINUX)

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

helm-publish-app:
	helm push k8-util/helm/fluvio-app  --version="$(VERSION)" --force fluvio



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
