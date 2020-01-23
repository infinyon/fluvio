VERSION := $(shell cat ../../VERSION)
GIT_VERSION=$(shell git log -1 --pretty=format:"%H")
REGISTRY=infinyon
CLOUD=minikube
SC_LOG="RUST_LOG=kf=trace,sc=trace,flv=trace"

dry-run:
	helm install --debug --dry-run goodly-guppy ./fluvio --set fluvioVersion=$(GIT_VERSION)

dry-run-aws:
	helm install --debug --dry-run goodly-guppy ./fluvio --set cloud=aws

install-test:
	helm install test ./fluvio --set fluvioVersion=$(GIT_VERSION) \
		 --set registry=$(REGISTRY) --set cloud=$(CLOUD)

install_minikube:	REGISTRY=localhost:5000/infinyon
install_minikube:	install-test

install_aws:	install-test
install_aws:	CLOUD=aws
install_aws:	REGISTRY=$(AWS_ECR)


uninstall-test:
	helm uninstall test