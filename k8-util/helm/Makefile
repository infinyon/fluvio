MAKE_DIR = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))
VERSION := $(shell cat $(MAKE_DIR)/../../VERSION)
GIT_VERSION=$(shell git log -1 --pretty=format:"%H")
REGISTRY=infinyon
CLOUD=minikube
SC_LOG=info
IMAGE_POLICY=IfNotPresent
NAME=fluvio
NS=default
TEST_LOG_LEVEL=debug
TEST_NS=default
TEST_NAME=fluviotest
FLUVIO_VERSION=$(VERSION)
CORE_VERSION=$(shell yq read fluvio-core/Chart.yaml version)
SYS_VERSION=$(shell yq read fluvio-sys/Chart.yaml version)
CHART_REPO=../../../infinyon.github.io
CHART_SRC=./fluvio-core
LOCAL_RGISTRY=localhost:5000/infinyon

dry-run:
	helm install --debug --dry-run goodly-guppy ./fluvio --set appVersion=$(GIT_VERSION)

dry-run-aws:
	helm install --debug --dry-run goodly-guppy ./fluvio --set cloud=aws

update_version:
	yq w -i fluvio-core/Chart.yaml version $(VERSION)
	yq w -i fluvio-core/values.yaml fluvioVersion $(VERSION)

check_version:
	if [[ $(CORE_VERSION) == $(SYS_VERSION) && $(CORE_VERSION) == $(VERSION)-latest ]]; \
	then exit 0; \
	else echo "versions do not match $(VERSION)"; exit 1; \
	fi

install:	update_version
	helm install $(NAME) $(CHART_SRC) -n $(NS) \
		--set fluvioVersion=$(FLUVIO_VERSION) \
		--set registry=$(REGISTRY)	\
		--set cloud=$(CLOUD) \
		--set scLog=$(SC_LOG) \
		--set imagePolicy=$(IMAGE_POLICY)


test-install:	SC_LOG="kf\=$(TEST_LOG_LEVEL)\,sc\=$(TEST_LOG_LEVEL)\,flv\=$(TEST_LOG_LEVEL)\,k8\=$(TEST_LOG_LEVEL)"
test-install:	IMAGE_POLICY=Always
test-install:	install
test-install:	NAME=$(TEST_NAME)
test-install:	NS=$(TEST_NS)
test-install:	FLUVIO_VERSION=$(GIT_VERSION)

install_mk_test:	REGISTRY=$(LOCAL_RGISTRY)
install_mk_test:	test-install
install_mk_test:	CLOUD=minikube

install_sys_minikube:	CLOUD=minikube
install_sys_minikube:	CHART_SRC=./fluvio-sys
install_sys_minikube:	NAME=fluvio-sys
install_sys_minikube:	install


install_minikube:	CLOUD=minikube
install_minikube:	install

install_minikube_dev:	CLOUD=minikube
install_minikube_dev:	install
install_minikube_dev:	FLUVIO_VERSION=$(GIT_VERSION)
install_minikube_dev:	REGISTRY=$(LOCAL_RGISTRY)

install_minikube_test:	CLOUD=minikube
install_minikube_test:	NS=test
install_minikube_test:	install

test_aws_install:	CLOUD=aws
test_aws_install:	REGISTRY=$(AWS_ECR)
test_aws_install:	test-install

aws_install:	CLOUD=aws
aws_install:	install

release_install:
	helm install $(NAME) ./fluvio \
		--set cloud=aws

test_uninstall:	uninstall
test_uninstall: NAME=$(TEST_NAME)
test_uninstall:	NS=$(TEST_NS)

uninstall:
	helm uninstall $(NAME) -n $(NS)


package-core:	cleanup	update_version
	echo "Installing $(VERSION) core helm package"
	helm package ./fluvio-core
	mv fluvio-core-$(VERSION).tgz $(CHART_REPO)/charts
	cd $(CHART_REPO); helm repo index charts

package-sys:	cleanup
	helm package ./fluvio-sys
	mv fluvio-sys-$(SYS_VERSION).tgz $(CHART_REPO)/charts
	cd $(CHART_REPO); helm repo index charts

cleanup:
	rm -rf *.tgz


add_repo:
	 helm repo add fluvio https://infinyon.github.io/charts
	 helm repo update

install_repo_minikube_sys:
	helm install fluvio/fluvio-sys --generate-name --set cloud=minikube

install_repo_minikube_core:
	helm install fluvio/fluvio-core --generate-name --set cloud=minikube


install_repo_minikube_test: SC_LOG="kf\=$(TEST_LOG_LEVEL)\,sc\=$(TEST_LOG_LEVEL)\,flv\=$(TEST_LOG_LEVEL)\,k8\=$(TEST_LOG_LEVEL)"
install_repo_minikube_test:
	helm install fluvio/fluvio --generate-name --set cloud=minikube --set scLog=$(SC_LOG)