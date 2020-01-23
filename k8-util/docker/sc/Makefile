NAME = $(REGISTRY)/fluvio-sc
TAG = $(VERSION)
BIN_DIR = ../../../target/x86_64-unknown-linux-musl/$(BIN_NAME)

all:	build


copy_binaries:
	mkdir -p target
	cp	${BIN_DIR}/sc-k8-server target

build:	copy_binaries
	docker build -f Dockerfile.sc  -t $(NAME):$(TAG)  ./target
	
push:	build
	docker push $(NAME):$(TAG)

minikube:	push
	eval $$(minikube docker-env); \
	docker pull $(NAME):$(TAG);
