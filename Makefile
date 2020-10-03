VERSION := $(shell cat VERSION)
RUSTV=stable
DOCKER_TAG=$(VERSION)
GITHUB_USER=infinyon
GITHUB_REPO=fluvio
GITHUB_TAG=$(VERSION)
GIT_COMMIT=$(shell git rev-parse HEAD)
DOCKER_REGISTRY=infinyon
TARGET_LINUX=x86_64-unknown-linux-musl
TARGET_DARWIN=x86_64-apple-darwin
CLI_BUILD=fluvio_cli
FLUVIO_BIN=./target/debug/fluvio
TEST_BIN=FLV_CMD=true ./target/debug/flv-test
DEFAULT_SPU=1
DEFAULT_ITERATION=1
DEFAULT_LOG=info

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
	$(TEST_BIN) --spu ${DEFAULT_SPU} --produce-iteration ${DEFAULT_ITERATION} --local-driver --log-dir /tmp --rust-log ${DEFAULT_LOG}

smoke-test-tls:	test-clean-up
	$(TEST_BIN) --spu ${DEFAULT_SPU} --produce-iteration ${DEFAULT_ITERATION} --tls --local-driver --log-dir /tmp --rust-log ${DEFAULT_LOG}

smoke-test-k8:	test-clean-up minikube_image
	$(TEST_BIN)	--spu ${DEFAULT_SPU} --produce-iteration ${DEFAULT_ITERATION} --develop --rust-log ${DEFAULT_LOG}

smoke-test-k8-tls:	test-clean-up minikube_image
	$(TEST_BIN) --spu ${DEFAULT_SPU} --produce-iteration ${DEFAULT_ITERATION} --tls --develop --rust-log ${DEFAULT_LOG}

test-clean-up:
	$(FLUVIO_BIN) cluster uninstall
	$(FLUVIO_BIN) cluster uninstall --local


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
	cargo +$(RUSTV) clippy --all-targets --all-features -- -D warnings


run-all-unit-test:
	cargo test --all --all-features

install_musl:
	rustup target add ${TARGET_LINUX}

clean_build:
	rm -rf /tmp/cli-*


#
# List of steps for creating fluvio binary
# create binaries for CLI
release_cli:
	rustup target add ${BUILD_TARGET}
	cd src/cli;cargo build --release --bin fluvio --target ${BUILD_TARGET}
	mkdir -p /tmp/$(CLI_BUILD)_${BUILD_TARGET}
	cp target/${BUILD_TARGET}/release/fluvio /tmp/$(CLI_BUILD)_${BUILD_TARGET}
	cd /tmp;tar -czvf cli-${BUILD_TARGET}-release.tar.gz $(CLI_BUILD)_${BUILD_TARGET};rm -rf $(CLI_BUILD)_${BUILD_TARGET}


release_cli_darwin: BUILD_TARGET=${TARGET_DARWIN}
release_cli_darwin:	release_cli
	
release_cli_linux:	BUILD_TARGET=${TARGET_LINUX}
release_cli_linux:	release_cli

# create docker image
release_image:	RELEASE=true
release_image:	fluvio_image
	docker tag infinyon/fluvio:$(GIT_COMMIT) infinyon/fluvio:$(VERSION)
	docker push infinyon/fluvio:$(VERSION)

minikube_image:	MINIKUBE_DOCKER_ENV=true
minikube_image:	fluvio_image

fluvio_image: CARGO_PROFILE=$(if $(RELEASE),release,debug)
fluvio_image: fluvio_bin_linux
	echo "Building Fluvio image with version: $(VERSION)"
	export CARGO_PROFILE=$(if $(RELEASE),release,debug); \
	export MINIKUBE_DOCKER_ENV=$(MINIKUBE_DOCKER_ENV); \
	export DOCKER_TAG=$(GIT_COMMIT); \
	k8-util/docker/build.sh


fluvio_bin_linux: RELEASE_FLAG=$(if $(RELEASE),--release,)
fluvio_bin_linux: install_musl
	cargo build $(RELEASE_FLAG) --bin fluvio --target $(TARGET_LINUX)

make publish_fluvio_image: 
	curl \
	-X POST \
	-H "Accept: application/vnd.github.v3+json" \
	-H "Authorization: $(GITHUB_ACCESS_TOKEN)" \
	https://api.github.com/repos/infinyon/fluvio/actions/workflows/2333005/dispatches \
	-d '{"ref":"master"}'


# create releases
# release CLI can be downloaded from https://github.com/aktau/github-release/releases
create_release:
	github-release release \
		--user ${GITHUB_USER} \
		--repo ${GITHUB_REPO} \
		--tag ${GITHUB_TAG} \
		--name "${GITHUB_TAG}" \
		--description "${GITHUB_TAG}"


upload_release:	release_cli_darwin release_cli_linux
	github-release upload \
		--user ${GITHUB_USER} \
		--repo ${GITHUB_REPO} \
		--tag ${GITHUB_TAG} \
		--name "cli-${TARGET_DARWIN}-release.tar.gz" \
		--file /tmp/cli-${TARGET_DARWIN}-release.tar.gz 
	github-release upload \
		--user ${GITHUB_USER} \
		--repo ${GITHUB_REPO} \
		--tag ${GITHUB_TAG} \
		--name "cli-${TARGET_LINUX}-release.tar.gz" \
		--file /tmp/cli-${TARGET_LINUX}-release.tar.gz


delete_release:
	github-release delete \
	--user ${GITHUB_USER} \
	--repo ${GITHUB_REPO} \
	--tag ${GITHUB_TAG}

# helm targets
helm_package:
	make -C k8-util/helm package-core
