# Probably move these up to the base makefile?
GIT_COMMIT_SHA=$(shell git rev-parse HEAD)
VERSION?=$(shell cat VERSION)
TAG?=$(VERSION)-$(GIT_COMMIT_SHA)

DOCKER_USERNAME?=test-docker-user
DOCKER_PASSWORD?=test-docker-pass

GH_TOKEN?=test-github-token


FLUVIO_BIN?=$(HOME)/.fluvio/bin/fluvio

DIRNAME?=
TARGET?=
PACKAGE?=
ARTIFACT?=

PUBLISH_BINARIES=fluvio fluvio-run fluvio-channel fluvio-test smdk

## TODO: Remember to remove `echo` from calls before merging

#### Testing only

get-version:
	echo $(VERSION)

get-tag:
	echo $(TAG)

clean-publish:
	rm --verbose --force *.zip *.tgz *.exe
	rm --verbose --force --recursive fluvio-* fluvio.*

#### End testing


# Login to Docker Hub
docker-hub-login:
	echo docker login --username=$(DOCKER_USERNAME) --password=$(DOCKER_PASSWORD)
#	docker login --username=$(DOCKER_USERNAME) --password=$(DOCKER_PASSWORD)


# Get Fluvio VERSION from Github, provided a given git SHA

# Create latest development Fluvio image
docker-create-manifest-dev: docker-hub-login
	echo docker manifest create "docker.io/infinyon/fluvio:latest" \
	  "docker.io/infinyon/fluvio:$(TAG)-amd64" \
	  "docker.io/infinyon/fluvio:$(TAG)-arm64v8"

# Push docker manifest
docker-push-manifest-dev: docker-create-manifest-dev
	echo docker manifest push "docker.io/infinyon/fluvio:$(TAG)"


# Expects GH_TOKEN env var to have token
#gh-cli-login-token:
#	gh auth login --with-token

# Uses $(VERSION)
install-fluvio-curl:
	curl -fsS https://packages.fluvio.io/v1/install.sh | bash


install-fluvio-stable: VERSION=stable
install-fluvio-stable: install-fluvio-curl

install-fluvio-latest: VERSION=latest
install-fluvio-latest: install-fluvio-curl

# Tag images


# Target that logs in to GH CLI. Assuming a token is provided, unless explicitly skipped

# Target that install fluvio-package
install-fluvio-package:
	$(FLUVIO_BIN) install fluvio-package

# Target that downloads the GH dev release
# Requires GH_TOKEN set
download-fluvio-release-dev:
	gh release download dev -R infinyon/fluvio --skip-existing


# Publish artifacts from GH Releases to Fluvio Packages
#
# Artifacts from GH Releases look like this:
#
# ./
#   ARTIFACT-TARGET.zip, such as:
#   fluvio-x86_64-unknown-linux-musl.zip
#   fluvio-aarch64-unknown-linux-musl.zip
#   fluvio-x86_64-apple-darwin.zip
#
# Here, we extract each zip into dirs with the same name.
# Then, we get the TARGET from the `.target` file inside.
#
# ./
#   ARTIFACT-TARGET.zip
#   ARTIFACT-TARGET/
#     ARTIFACT
#     .target
#   fluvio-x86_64-unknown-linux-musl.zip
#   fluvio-x86_64-unknown-linux-musl/
#     fluvio
#     .target
unzip-gh-release-artifacts: download-fluvio-release-dev
	echo "unzip stuff"
	echo $(foreach bin, $(wildcard *.zip), \
		$(shell \
			export DIRNAME=$(basename $(bin)); \
			echo Unzipping $(bin) to $$DIRNAME; \
			unzip -u -d $$DIRNAME $(bin); \
		) \
	)

#		$(shell \
#			unzip $f -d $(basename $f); \
#			cat $(basename $f)/.target
#		) \
#	)

publish-artifacts: unzip-gh-release-artifacts
	echo "package stuff"
	$(foreach bin, $(wildcard *.zip), \
		export DIRNAME=$(basename $(bin)); \
		export TARGET=$(shell cat $(basename $(bin))/.target); \
		export PACKAGE=$(subst -$$TARGET,,$$DIRNAME); \
		export ARTIFACT=$(abspath $$DIRNAME/$$PACKAGE); \
		echo $(FLUVIO_BIN) package publish \
			--package=$$PACKAGE \
			--version=$(VERSION) \
			--target=$$TARGET \
			$$ARTIFACT \
	)

#	for GH_ARTIFACT in *.zip; do \
#		echo filename: $(basename $$GH_ARTIFACT); \
#	done

# Moved this from cargo-make. It was called "bump-fluvio-latest" and only run in CI

# Version convention is different here. Notice the `+`
bump-fluvio-latest: TAG=$(VERSION)+$(GIT_COMMIT_SHA)
bump-fluvio-latest: install-fluvio-latest install-fluvio-package
	echo $(FLUVIO_BIN) package publish bump latest $(TAG)
	echo $(foreach bin, $(PUBLISH_BINARIES), \
		$(shell echo $(FLUVIO_BIN) package tag $(bin):$(TAG) --allow-missing-targets --tag=latest --force) \
	)
#	$(FLUVIO_BIN) package publish tag  fluvio:$(TAG) --tag=latest --force
#	$(FLUVIO_BIN) package publish tag  fluvio-run:$(TAG) --allow-missing-targets --tag=latest --force
#	$(FLUVIO_BIN) package publish tag  fluvio-channel:$(TAG) --allow-missing-targets --tag=latest --force
#	$(FLUVIO_BIN) package publish tag  fluvio-test:$(TAG) --allow-missing-targets --tag=latest --force
#	$(FLUVIO_BIN) package publish tag  smdk:$(TAG) --allow-missing-targets --tag=latest --force
#${HOME}/.fluvio/extensions/fluvio-package ${FLUVIO_PUBLISH_TEST} \
#    bump latest "$(cat VERSION)+$(git rev-parse HEAD)"
#
#${HOME}/.fluvio/extensions/fluvio-package ${FLUVIO_PUBLISH_TEST} \
#    tag "fluvio:$(cat VERSION)+$(git rev-parse HEAD)" --tag=latest --force
#${HOME}/.fluvio/extensions/fluvio-package ${FLUVIO_PUBLISH_TEST} \
#    tag "fluvio-run:$(cat VERSION)+$(git rev-parse HEAD)" --allow-missing-targets --tag=latest --force
#${HOME}/.fluvio/extensions/fluvio-package ${FLUVIO_PUBLISH_TEST} \
#    tag "fluvio-channel:$(cat VERSION)+$(git rev-parse HEAD)" --allow-missing-targets --tag=latest --force
