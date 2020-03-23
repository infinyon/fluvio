VERSION := $(shell cat VERSION)
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

build:
	cargo build

integration-test:	build
	RUST_LOG=flv_integration=debug ./target/debug/flv-integration-test


run-all-unit-test:
	cargo test --all

install_musl:
	rustup target add ${TARGET_LINUX}

clean_build:
	rm -rf /tmp/cli-*

# create binaries for CLI
release_cli_darwin:	
	cargo build --release --bin fluvio  --target ${TARGET_DARWIN}
	mkdir -p /tmp/$(CLI_BUILD)_${TARGET_DARWIN}
	cp target/${TARGET_DARWIN}/release/fluvio /tmp/$(CLI_BUILD)_${TARGET_DARWIN}
	cd /tmp;tar -czvf cli-${TARGET_DARWIN}-release.tar.gz $(CLI_BUILD)_${TARGET_DARWIN};rm -rf $(CLI_BUILD)_${TARGET_DARWIN}

release_cli_linux:
	cargo build --release --bin fluvio  --target ${TARGET_LINUX}
	mkdir -p /tmp/$(CLI_BUILD)_${TARGET_LINUX}
	cp target/${TARGET_LINUX}/release/fluvio /tmp/$(CLI_BUILD)_${TARGET_LINUX}
	cd /tmp;tar -czvf cli-${TARGET_LINUX}-release.tar.gz $(CLI_BUILD)_${TARGET_LINUX};rm -rf $(CLI_BUILD)_${TARGET_LINUX}


all_image:	linux-spu-server spu_image linux-sc-server sc_image

# create docker images for release
release_image:	MAKE_CMD=push
release_image:	all_image

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


spu_image:	 linux-spu-server
	echo "Building SPU image with version: ${VERSION}"
	make build BIN_NAME=$(BIN_NAME) $(MAKE_CMD) VERSION=${VERSION} REGISTRY=${DOCKER_REGISTRY} -C k8-util/docker/spu

sc_image:	install_musl linux-spu-server
	echo "Building SC image with version: ${VERSION}"
	make build BIN_NAME=$(BIN_NAME) $(MAKE_CMD) VERSION=${VERSION} REGISTRY=${DOCKER_REGISTRY} -C k8-util/docker/sc
	

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
	cd k8-util/helm; make package-core


## Helper targets to compile specific crate


build-sc-test:
	cd src/sc-server;cargo test --no-run
			
			
build-spu-test:
	cd src/spu-server;cargo test --no-run

build-storage-test:
	cd src/storage;cargo test --no-run

build-internal-test:
	cd src/internal-api;cargo test --no-run	

		
build-k8client:
	cd src/k8-client;cargo build


test-spu:
	cd src/spu-server;cargo test

test-spu-offset:
	cd src/spu-server;RUST_LOG=spu_server=trace cargo test flv_offset_fetch_test	

test-sc-connection:
	cd src/sc-server;RUST_LOG=sc_server=trace cargo test connection_test

test-sc-partition:
	cd src/sc-server;RUST_LOG=sc_server=trace cargo test partition_test

test-sc-controller:
	cd src/sc-server; cargo test test_controller_basic		

test-sc:
	cd src/sc-core;cargo test	


test-sc-k8:
	cd src/sc-k8;cargo test

test-storage:
	cd src/storage;cargo test

test-internal-api:
	cd src/api/internal-api;cargo test

test-cli:
	cd src/cli;cargo test

test-helper:
	cd src/future-helper;cargo test

test-aio:
	cd src/flv-future-aio = { version = "0.1.0" };cargo test

test-kfsocket:
	cd src/kf-socket;cargo test

test-kfservice:
	cd src/kf-service;cargo test

test-k8client:
	cd src/k8-client;cargo test

test-k8metadata:
	cd src/k8-metadata;cargo test	

test-k8config:
	cd src/k8-config;cargo test

test-kf-protocol:
	cd src/kf-protocol;cargo test

test-client:
	cd src/client;cargo test




.PHONY:	build test-helper teste-aio test-kfsocket test-kfservice test-k8client test-k8config integration-test
