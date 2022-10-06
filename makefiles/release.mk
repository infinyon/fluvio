# Use the binary name produced by cargo
PUBLISH_BINARIES=fluvio fluvio-run fluvio-channel fluvio-test smdk

# CI has to set RELEASE=true to run commands that update public
RELEASE?=false
ifneq ($(RELEASE),true)
DRY_RUN_ECHO=echo
#$(info Dry run mode - No public changes)
else
# When this is blank, commands will affect the public releases
DRY_RUN_ECHO=
#$(info Live mode - Public changes possible)
endif

GIT_COMMIT_SHA=$(shell git rev-parse HEAD)
STABLE_VERSION_TAG?=stable
REPO_VERSION?=$(shell cat VERSION)
DEV_VERSION_TAG?=$(REPO_VERSION)-$(GIT_COMMIT_SHA)
CHANNEL_TAG?=stable

# VERSION is used mostly by build, so here we
# use CHANNEL_TAG to override value of VERSION for CI
ifeq ($(CHANNEL_TAG), stable)
VERSION:=$(STABLE_VERSION_TAG)
#$(info Working with channel $(CHANNEL_TAG) version: $(VERSION))
else ifeq ($(CHANNEL_TAG), latest)
VERSION:=$(DEV_VERSION_TAG)
#$(info Working with channel $(CHANNEL_TAG) version: $(VERSION))
else
VERSION:=$(CHANNEL_TAG)
endif
#$(info Working with version: $(VERSION))

DOCKER_USERNAME?=test-docker-user
DOCKER_PASSWORD?=test-docker-pass
DOCKER_IMAGE_TAG?=$(REPO_VERSION)
#$(info Docker image tag: $(DOCKER_IMAGE_TAG))

GH_TOKEN?=
GH_RELEASE_TAG?=dev

# Allow using local `gh` auth token for local testing
ifeq ($(CI), true)
ifndef GH_TOKEN
$(error GH_TOKEN required in CI)
endif
endif

DIRNAME?=
TARGET?=
PACKAGE?=
ARTIFACT?=

#### Testing only

get-version:
	echo $(VERSION)

get-tag:
	echo $(DEV_VERSION_TAG)

clean-publish:
	rm --verbose --force *.zip *.tgz *.exe
	rm --verbose --force --recursive fluvio-* fluvio.*
	rm --verbose --force /tmp/release_notes /tmp/cd_dev_latest.txt

#fix-latest-channel:
#	# Find the last git sha from master
#	# Re-set all tags to use the version and sha from that commit
#### End testing


# Login to Docker Hub
docker-hub-login:
	$(DRY_RUN_ECHO) docker login --username=$(DOCKER_USERNAME) --password=$(DOCKER_PASSWORD)

docker-hub-check-image-exists:
	if [ $(lastword $(shell docker pull --quiet infinyon/fluvio:$(DOCKER_IMAGE_TAG); echo $$?)) -eq 0 ]; then \
		echo Image tag already exists; \
		exit 0; \
	else \
		echo Image tag does not exist; \
		exit 1; \
	fi

# Get Fluvio VERSION from Github, provided a given git SHA
docker-create-manifest: docker-hub-login
	$(DRY_RUN_ECHO) docker manifest create "docker.io/infinyon/fluvio:$(DOCKER_IMAGE_TAG)" \
		"docker.io/infinyon/fluvio:$(DEV_VERSION_TAG)-amd64" \
		"docker.io/infinyon/fluvio:$(DEV_VERSION_TAG)-arm64v8"

docker-push-manifest: docker-create-manifest
	$(DRY_RUN_ECHO) docker manifest push "docker.io/infinyon/fluvio:$(DOCKER_IMAGE_TAG)"

# Create latest development Fluvio image
docker-create-manifest-dev: DOCKER_IMAGE_TAG=latest
docker-create-manifest-dev: docker-hub-login docker-create-manifest

# Push docker manifest
docker-push-manifest-dev: DOCKER_IMAGE_TAG=latest
docker-push-manifest-dev: docker-create-manifest-dev docker-push-manifest

# Uses $(VERSION)
curl-install-fluvio:
	curl -fsS https://packages.fluvio.io/v1/install.sh | bash

install-fluvio-stable: VERSION=stable
install-fluvio-stable: curl-install-fluvio

install-fluvio-latest: VERSION=latest
install-fluvio-latest: curl-install-fluvio

install-fluvio-package: FLUVIO_BIN=$(HOME)/.fluvio/bin/fluvio
install-fluvio-package:
	$(FLUVIO_BIN) install fluvio-package

# Requires GH_TOKEN set or `gh auth login`
download-fluvio-release:
	gh release download $(GH_RELEASE_TAG) -R infinyon/fluvio --skip-existing

unzip-gh-release-artifacts: download-fluvio-release
	@echo "unzip stuff"
	@$(foreach bin, $(wildcard *.zip), \
		printf "\n"; \
		export DIRNAME=$(basename $(bin)); \
		unzip -u -d $$DIRNAME $(bin); \
	)

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
publish-artifacts: PUBLIC_VERSION=$(subst -,+,$(VERSION))
publish-artifacts: install-fluvio-package unzip-gh-release-artifacts
	@echo "package stuff"
	$(foreach bin, $(wildcard *.zip), \
		printf "\n"; \
		export DIRNAME=$(basename $(bin)); \
		export TARGET=$(shell cat $(basename $(bin))/.target); \
		export PACKAGE=$(subst -$(shell cat $(basename $(bin))/.target), ,$(basename $(bin))); \
		export ARTIFACT=$(abspath $$DIRNAME/$$PACKAGE); \
		$(DRY_RUN_ECHO) $(FLUVIO_BIN) package publish \
			--package=$(subst .exe, ,$(subst -$(shell cat $(basename $(bin))/.target), ,$(basename $(bin)))) \
			--version=$(PUBLIC_VERSION) \
			--target=$$TARGET \
			$$ARTIFACT; \
	)


publish-artifacts-stable: VERSION=$(REPO_VERSION)
publish-artifacts-stable: publish-artifacts

publish-artifacts-dev: VERSION=$(DEV_VERSION_TAG)
publish-artifacts-dev: publish-artifacts

# Need to ensure that version is always a semver
# Version convention is different here. Notice the `+`
bump-fluvio: FLUVIO_BIN=$(HOME)/.fluvio/bin/fluvio
bump-fluvio: PUBLIC_VERSION?=$(subst -,+,$(VERSION))
bump-fluvio: install-fluvio-package
	$(DRY_RUN_ECHO) $(FLUVIO_BIN) package publish bump $(CHANNEL_TAG) $(PUBLIC_VERSION)
	@$(foreach bin, $(PUBLISH_BINARIES), \
		printf "\n"; \
		$(DRY_RUN_ECHO) $(FLUVIO_BIN) package tag $(bin):$(PUBLIC_VERSION) --allow-missing-targets --tag=$(CHANNEL_TAG) --force; \
	)

bump-fluvio-stable: CHANNEL_TAG=stable
bump-fluvio-stable: VERSION=$(REPO_VERSION)
bump-fluvio-stable: install-fluvio-stable bump-fluvio

bump-fluvio-latest: CHANNEL_TAG=latest
bump-fluvio-latest: VERSION=$(subst -,+,$(DEV_VERSION_TAG))
bump-fluvio-latest: install-fluvio-latest bump-fluvio

update-public-installer-script-s3:
	$(DRY_RUN_ECHO) aws s3 cp ./install.sh s3://packages.fluvio.io/v1/install.sh --acl public-read

latest-cd-dev-status:
	gh api /repos/{owner}/{repo}/actions/workflows/cd_dev.yaml/runs | jq .workflow_runs[0] > /tmp/cd_dev_latest.txt
	@echo "Latest CD_Dev run: $$( cat /tmp/cd_dev_latest.txt | jq .html_url | tr -d '"')"

	@if [ $$(cat /tmp/cd_dev_latest.txt | jq .conclusion | tr -d '"') = success ]; then \
		echo ✅ Most recent CD_Dev run passed; \
		exit 0; \
	else \
		echo ❌ Most recent CD_Dev run failed; \
		exit 1; \
	fi

build-release-notes:
	rm --verbose --force /tmp/release_notes
	touch /tmp/release_notes
	echo "# Release Notes" >> /tmp/release_notes
	export VERSION=$(shell cat VERSION)
	cat CHANGELOG.md | sed -e '/./{H;$$!d;}' -e "x;/##\ Platform\ Version\ $$VERSION/"'!d;' >> /tmp/release_notes

	# Replace UNRELEASED w/ date YYYY-MM-dd
	export TZ=":America/Los_Angeles"
	cat /tmp/release_notes | sed -i "s/UNRELEASED/$(shell date +%F)/" /tmp/release_notes

	# Print the release notes to stdout
	cat /tmp/release_notes

create-gh-release: download-fluvio-release build-release-notes
	$(DRY_RUN_ECHO) gh release create -R infinyon/fluvio \
		--title="v$(VERSION)" \
		-F /tmp/release_notes \
		"v$(VERSION)" \
		$(wildcard *.zip *.tgz)