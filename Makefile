VERSION := $(shell cat VERSION)
DOCKER_VERSION = $(VERSION)
TOOLCHAIN = "./rust-toolchain"
RUSTV = $(shell cat ${TOOLCHAIN})
RUST_DOCKER_IMAGE=fluvio/rust-tool:${RUSTV}
CARGO_BUILD=build --release
BIN_NAME=release
MAKE_CMD=build
GITHUB_USER=infinyon
GITHUB_REPO=fluvio
GITHUB_TAG=$(VERSION)
DOCKER_REGISTRY=infinyon
TARGET_LINUX=x86_64-unknown-linux-musl
TARGET_DARWIN=x86_64-apple-darwin
CLI_BUILD=fluvio_cli
FLUVIO_BIN=./target/debug/fluvio
TEST_BIN=FLV_CMD=true ./target/debug/flv-test

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
	$(TEST_BIN) --local-driver --log-dir /tmp

smoke-test-tls:	build test-clean-up
	$(TEST_BIN) --tls --local-driver --log-dir /tmp --rust-log flv=debug

smoke-test-k8:	build test-clean-up minikube_image
	$(TEST_BIN)	--develop --log-dir /tmp

smoke-test-k8-tls:	build test-clean-up minikube_image
	$(TEST_BIN) --tls --develop --log-dir /tmp

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

check-clippy:
	cargo +$(RUSTV) clippy --all-targets --all-features -- -D warnings


run-all-unit-test:
	cargo test --all

install_musl:
	rustup target add ${TARGET_LINUX}

clean_build:
	rm -rf /tmp/cli-*


#
# List of steps for creating fluvio binary
# create binaries for CLI
release_cli:
	rustup target add ${BUILD_TARGET}
	cd src/cli;cargo build --release --bin fluvio --features cluster_components --target ${BUILD_TARGET}
	mkdir -p /tmp/$(CLI_BUILD)_${BUILD_TARGET}
	cp target/${BUILD_TARGET}/release/fluvio /tmp/$(CLI_BUILD)_${BUILD_TARGET}
	cd /tmp;tar -czvf cli-${BUILD_TARGET}-release.tar.gz $(CLI_BUILD)_${BUILD_TARGET};rm -rf $(CLI_BUILD)_${BUILD_TARGET}


release_cli_darwin: BUILD_TARGET=${TARGET_DARWIN}
release_cli_darwin:	release_cli
	
release_cli_linux:	BUILD_TARGET=${TARGET_LINUX}
release_cli_linux:	release_cli

all_image:	linux-spu-server spu_image linux-sc-server sc_image

# create docker images for release
release_image:	MAKE_CMD=push
release_image:	all_image

release_image_chart_latest: DOCKER_VERSION=$(VERSION)-latest
release_image_chart_latest: release_image

release_image_ver_latest:	DOCKER_VERSION=latest
release_image_ver_latest:	release_image

release_image_latest:	release_image_chart_latest release_image_ver_latest


develop_image:	VERSION=$(shell git log -1 --pretty=format:"%H")
develop_image: 	all_image
develop_image:	CARGO_BUILD=build
develop_image:	BIN_NAME=debug

local_image:	develop_image
local_image:	DOCKER_REGISTRY=localhost:5000/infinyon

minikube_image:	local_image
minikube_image:	MAKE_CMD=minikube


aws_dev_image:	develop_image
aws_dev_image:	MAKE_CMD=push
aws_dev_image:	DOCKER_REGISTRY=$(AWS_ECR)

linux-sc-server:	install_musl
	cargo $(CARGO_BUILD) --bin sc-k8-server  --target ${TARGET_LINUX}

linux-spu-server:	install_musl
	cargo $(CARGO_BUILD) --bin spu-server  --target ${TARGET_LINUX}


spu_image:	linux-spu-server
	echo "Building SPU image with version: ${DOCKER_VERSION}"
	make build BIN_NAME=$(BIN_NAME) $(MAKE_CMD) VERSION=${DOCKER_VERSION} REGISTRY=${DOCKER_REGISTRY} -C k8-util/docker/spu

sc_image:	linux-spu-server
	echo "Building SC image with version: ${DOCKER_VERSION}"
	make build BIN_NAME=$(BIN_NAME) $(MAKE_CMD) VERSION=${DOCKER_VERSION} REGISTRY=${DOCKER_REGISTRY} -C k8-util/docker/sc

fluvio_cli: install_musl
	cd src/cli;cargo build --release --bin fluvio --features cluster_components --target ${TARGET_LINUX}

fluvio_image: fluvio_cli
	echo "Building Fluvio image with version: ${VERSION}"
	make build BIN_NAME=$(BIN_NAME) $(MAKE_CMD) VERSION=${VERSION} REGISTRY=${DOCKER_REGISTRY} -C k8-util/docker/fluvio

fluvio_image_latest: VERSION=latest
fluvio_image_latest: MAKE_CMD=push
fluvio_image_latest: fluvio_image

make publish_fluvio_image: 
	curl \
	-X POST \
	-H "Accept: application/vnd.github.v3+json" \
	-H "Authorization: $(GITHUB_ACCESS_TOKEN)" \
	https://api.github.com/repos/infinyon/fluvio/actions/workflows/2333005/dispatches \
	-d '{"ref":"master"}'



cargo_cache_dir:
	mkdir -p .docker-cargo


# run test in docker
docker_linux_test:	cargo_cache_dir
	 docker run --rm --volume ${PWD}:/src --workdir /src  \
	 	-e USER -e CARGO_HOME=/src/.docker-cargo \
		-e CARGO_TARGET_DIR=/src/target-docker \
	  	${RUST_DOCKER_IMAGE} cargo test


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
