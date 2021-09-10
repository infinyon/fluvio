MAKE_DIR = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))
VERSION := $(shell cat $(MAKE_DIR)/../../VERSION)
APP_VERSION = $(VERSION)
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
CORE_VERSION=$(shell yq read fluvio-app/Chart.yaml version)
SYS_VERSION=$(shell yq read fluvio-sys/Chart.yaml version)
CHART_SRC=./fluvio-app

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

install_mk_test:	test-install
install_mk_test:	CLOUD=minikube

# install sys chart for local development
install_sys_dev:	
	helm install fluvio-sys ./fluvio-sys -n $(NS) --set cloud=$(CLOUD)


install_minikube:	CLOUD=minikube
install_minikube:	install

install_minikube_dev:	CLOUD=minikube
install_minikube_dev:	install
install_minikube_dev:	FLUVIO_VERSION=$(GIT_VERSION)

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

cleanup:
	rm -rf *.tgz


add_repo:
	 helm repo add fluvio https://charts.fluvio.io
	 helm repo update

install_repo_minikube_sys:
	helm install fluvio/fluvio-sys --generate-name --set cloud=minikube

install_repo_minikube_core:
	helm install fluvio/fluvio-core --generate-name --set cloud=minikube


install_repo_minikube_test: SC_LOG="kf\=$(TEST_LOG_LEVEL)\,sc\=$(TEST_LOG_LEVEL)\,flv\=$(TEST_LOG_LEVEL)\,k8\=$(TEST_LOG_LEVEL)"
install_repo_minikube_test:
	helm install fluvio/fluvio --generate-name --set cloud=minikube --set scLog=$(SC_LOG)


helm-install-plugin:
	helm plugin install https://github.com/chartmuseum/helm-push.git


helm-login:
	helm repo remove fluvio
	helm repo add fluvio https://gitops:$(HELM_PASSWORD)@charts.fluvio.io

helm-publish-sys:
	helm push k8-util/helm/fluvio-sys --version="$(VERSION)" --force fluvio

helm-publish-app:
	helm push k8-util/helm/fluvio-app --version="$(VERSION)" --force fluvio


.PHONY: target_directory

sys_charts:
	mkdir -p pkg_sys

app_charts:
	mkdir -p app_sys

clean:
	rm -rf pkg_sys;
	rm -rf pkg_app;

pkg_sys:	fluvio-sys/*.* ../../VERSION
	rm -rf pkg_sys;
	mkdir -p pkg_sys;
	helm package ./fluvio-sys --app-version $(APP_VERSION) --version $(APP_VERSION) -d pkg_sys

pkg_app:	fluvio-app/*.* ../../VERSION
	rm -rf pkg_app
	mkdir -p pkg_app
	helm package ./fluvio-app --app-version $(APP_VERSION) --version $(APP_VERSION) -d pkg_app

package:	pkg_sys pkg_app