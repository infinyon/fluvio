NAME = fluvio/rust-tool
TOOLCHAIN = "../../../rust-toolchain"
RUSTV = $(shell cat ${TOOLCHAIN})
TAG = ${RUSTV}
all:    build push

build:
	docker build --build-arg RUSTV=$(RUSTV)  -t $(NAME):$(TAG)  .

push:
	docker push $(NAME):$(TAG)