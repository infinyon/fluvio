NAME = $(REGISTRY)/fluvio
TAG = $(VERSION)
BIN_DIR = ../../../target/x86_64-unknown-linux-musl/$(BIN_NAME)

all: push

copy_binaries:
	mkdir -p target
	cp	${BIN_DIR}/fluvio target

build:	copy_binaries
	docker build -f Dockerfile.fluvio  -t $(NAME):$(TAG)  ./target

push:	build
	docker push $(NAME):$(TAG)

minikube:	push
	eval $$(minikube docker-env); \
	docker pull $(NAME):$(TAG);