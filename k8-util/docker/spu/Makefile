NAME = $(REGISTRY)/fluvio-spu
TAG = $(VERSION)
BIN_DIR = ../../../target/x86_64-unknown-linux-musl/$(BIN_NAME)

all: push

copy_binaries:
	mkdir -p target
	cp	${BIN_DIR}/fluvio-spu target

build:	copy_binaries
	docker build -f Dockerfile.spu -t $(NAME):$(TAG) ./target

push:	build
	docker push $(NAME):$(TAG)

minikube:	copy_binaries
	eval $$(minikube docker-env); docker build -f Dockerfile.spu  -t $(NAME):$(TAG)  ./target