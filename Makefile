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
FLUVIO_BIN=./target/debug/fluvio

# install all tools required
install_tools:
	brew install yq
	brew install helm

build:
	cargo build

# run local smoke test
smoke-test:	build test-clean-up
	FLV_CMD=true ./target/debug/flv-test --local-driver --log-dir /tmp

smoke-test-tls:	build test-clean-up
	FLV_CMD=true ./target/debug/flv-test --tls --local-driver --log-dir /tmp --rust-log flv=debug

smoke-test-k8:	build test-clean-up minikube_image
	FLV_CMD=true ./target/debug/flv-test --develop --log-dir /tmp

smoke-test-k8-tls:	build test-clean-up minikube_image
	FLV_CMD=true ./target/debug/flv-test --tls --develop --log-dir /tmp

test-clean-up:
	$(FLUVIO_BIN) cluster uninstall
	$(FLUVIO_BIN) cluster uninstall --local

install-fmt:
	rustup component add rustfmt --toolchain $(RUSTV)

check-fmt:
	cargo +$(RUSTV) fmt -- --check

install-clippy:
	rustup component add clippy --toolchain $(RUSTV)

check-clippy:
	cargo +$(RUSTV) clippy --all-targets --all-features -- -D warnings

# create secret for k8 in development mode
k8-create-secret:
	kubectl delete secret fluvio-ca --ignore-not-found=true
	kubectl delete secret fluvio-tls --ignore-not-found=true
	kubectl create secret generic fluvio-ca --from-file tls/certs/ca.crt
	kubectl create secret tls fluvio-tls --cert tls/certs/server.crt --key tls/certs/server.key

k8-tls-list:
	$(FLUVIO_BIN) topic list --tls --enable-client-cert --ca-cert tls/certs/ca.crt --client-cert tls/certs/client.crt --client-key tls/certs/client.key --sc fluvio.local:9003

# set k8 profile using tls
k8-set-k8-profile-tls:
	$(FLUVIO_BIN) profile set-k8-profile --tls --domain fluvio.local --enable-client-cert --ca-cert tls/certs/ca.crt --client-cert tls/certs/client.crt --client-key tls/certs/client.key

run-all-unit-test:
	cargo test --all

install_musl:
	rustup target add ${TARGET_LINUX}

clean_build:
	rm -rf /tmp/cli-*

# create binaries for CLI
release_cli_darwin: 
	cd src/cli;cargo build --release --bin fluvio --features cluster_components --target ${TARGET_DARWIN}
	mkdir -p /tmp/$(CLI_BUILD)_${TARGET_DARWIN}
	cp target/${TARGET_DARWIN}/release/fluvio /tmp/$(CLI_BUILD)_${TARGET_DARWIN}
	cd /tmp;tar -czvf cli-${TARGET_DARWIN}-release.tar.gz $(CLI_BUILD)_${TARGET_DARWIN};rm -rf $(CLI_BUILD)_${TARGET_DARWIN}

release_cli_linux:
	cd src/cli;cargo build --release --bin fluvio --features cluster_components --target ${TARGET_LINUX}
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


spu_image:	linux-spu-server
	echo "Building SPU image with version: ${VERSION}"
	make build BIN_NAME=$(BIN_NAME) $(MAKE_CMD) VERSION=${VERSION} REGISTRY=${DOCKER_REGISTRY} -C k8-util/docker/spu

sc_image:	linux-spu-server
	echo "Building SC image with version: ${VERSION}"
	make build BIN_NAME=$(BIN_NAME) $(MAKE_CMD) VERSION=${VERSION} REGISTRY=${DOCKER_REGISTRY} -C k8-util/docker/sc

fluvio_image: all_image
	echo "Building Fluvio image with version: ${VERSION}"
	make push BIN_NAME=$(BIN_NAME) $(MAKE_CMD) VERSION=${VERSION} REGISTRY=${DOCKER_REGISTRY} -C k8-util/docker/fluvio

fluvio_image_nightly: VERSION=nightly
fluvio_image_nightly: fluvio_image

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


# install using local helm chart and current code
helm_minikube_dev_install:	minikube_image	
	cd k8-util/helm; make install_minikube_dev


helm_uninstall_dev:
	helm uninstall fluvio


test-smoke:
	make -C tests smoke-test

install-local-tls:
	$(FLUVIO_BIN) cluster install --local  \
		--tls --server-cert tls/certs/server.crt --server-key tls/certs/server.key \
		--ca-cert tls/certs/ca.crt --client-cert tls/certs/client.crt	\
		--client-key tls/certs/client.key --domain fluvio.local
	
uninstall-local:
	$(FLUVIO_BIN) cluster uninstall --local

install-k8-tls:
	$(FLUVIO_BIN) cluster install --develop \
		--tls --server-cert tls/certs/server.crt --server-key tls/certs/server.key \
		--ca-cert tls/certs/ca.crt --client-cert tls/certs/client.crt	\
		--client-key tls/certs/client.key --domain fluvio.local

uninstall-k8:
	$(FLUVIO_BIN) cluster uninstall