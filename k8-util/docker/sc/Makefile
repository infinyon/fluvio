NAME = $(REGISTRY)/fluvio-sc
TAG = $(VERSION)
BIN_DIR = ../../../target/x86_64-unknown-linux-musl/$(BIN_NAME)

all:	build

copy_binaries:
	mkdir -p target
	cp	${BIN_DIR}/fluvio-sc-k8 target

build:	copy_binaries
	docker build -f Dockerfile.sc -t $(NAME):$(TAG) ./target
	
push:	build
	docker push $(NAME):$(TAG)

minikube:	copy_binaries
	eval $$(minikube docker-env); docker build -f Dockerfile.sc  -t $(NAME):$(TAG)  ./target
	