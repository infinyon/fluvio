NAME = infinyon/fluvio-spu
TAG = 0.2.0-alpha.1
BIN_DIR = ../../../target/x86_64-unknown-linux-musl/$(BIN_NAME)

all: push

copy_binaries:
	mkdir -p target
	cp	${BIN_DIR}/spu-server target

build:	copy_binaries
	docker build -f Dockerfile.spu  -t $(NAME):$(TAG)  ./target


push_registry:
	docker tag $(NAME):$(TAG) localhost:5000/$(NAME):$(TAG)
	docker push localhost:5000/$(NAME):$(TAG)

pull_minikube:
	eval $$(minikube docker-env); \
	docker pull localhost:5000/$(NAME):$(TAG); \
	docker tag localhost:5000/$(NAME):$(TAG) $(NAME):$(TAG)

push:	build push_registry pull_minikube

push_release:	build
	docker push $(NAME):$(TAG)
