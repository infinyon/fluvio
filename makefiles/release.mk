# Use the binary name produced by cargo
PUBLISH_BINARIES=fluvio-test
#PUBLISH_BINARIES=fluvio fluvio-run fluvio-channel fluvio-test smdk

# CI has to set RELEASE=true to run commands that update public
RELEASE?=false
ifneq ($(RELEASE),true)
DRY_RUN_ECHO=echo
$(info Dry run mode - No public changes)
else
DRY_RUN_ECHO=blahbreak
$(info Live mode - Public changes possible)
endif

# Probably move these up to the base makefile?
GIT_COMMIT_SHA=$(shell git rev-parse HEAD)
STABLE_VERSION_TAG?=$(shell cat VERSION)
DEV_VERSION_TAG?=$(STABLE_VERSION_TAG)-$(GIT_COMMIT_SHA)
CHANNEL_TAG?=latest

# CHANNEL_TAG overrides VERSION
ifeq ($(CHANNEL_TAG), stable)
VERSION:=$(STABLE_VERSION_TAG)
$(info Working with channel $(CHANNEL_TAG) version: $(VERSION))
else ifeq ($(CHANNEL_TAG), latest)
VERSION:=$(DEV_VERSION_TAG)
$(info Working with channel $(CHANNEL_TAG) version: $(VERSION))
else
VERSION:=$(CHANNEL_TAG)
endif
$(info Working with version: $(VERSION))


DOCKER_USERNAME?=test-docker-user
DOCKER_PASSWORD?=test-docker-pass
DOCKER_IMAGE_TAG?=$(VERSION)
$(info Docker image tag: $(DOCKER_IMAGE_TAG))

GH_TOKEN?=
GH_RELEASE_TAG?=dev

# Allow using local `gh` auth token for local testing
ifeq ($(CI), true)
ifndef GH_TOKEN
$(error GH_TOKEN required in CI)
endif
endif

FLUVIO_BIN?=$(HOME)/.fluvio/bin/fluvio

DIRNAME?=
TARGET?=
PACKAGE?=
ARTIFACT?=

## TODO: Remember to remove `echo` from calls before merging

#### Testing only

get-version:
	echo $(VERSION)

get-tag:
	echo $(DEV_VERSION_TAG)

clean-publish:
	rm --verbose --force *.zip *.tgz *.exe
	rm --verbose --force --recursive fluvio-* fluvio.*
	rm --verbose --force /tmp/release_notes /tmp/fluvio_image_exists
	rm --verbose --force --recursive ./tmp-release


fix-latest-channel:
	# Find the last git sha from master
	# Re-set all tags to use the version and sha from that commit
#### End testing


# Login to Docker Hub
docker-hub-login:
	$(DRY_RUN_ECHO) docker login --username=$(DOCKER_USERNAME) --password=$(DOCKER_PASSWORD)
#	docker login --username=$(DOCKER_USERNAME) --password=$(DOCKER_PASSWORD)

docker-hub-check-image-exists:
	$(info Checking on if infinyon/fluvio:$(DOCKER_IMAGE_TAG) exists in Docker Hub);
	ifeq ($(shell docker pull --quiet infinyon/fluvio:$(DOCKER_IMAGE_TAG) > /dev/null 2>&1; echo $$?), 0)
	$(shell echo true > /tmp/fluvio_image_exists);
	$(info Image tag already exists);
	else
	$(shell echo false > /tmp/fluvio_image_exists);
	$(error Image tag does not exist);
	endif


# Get Fluvio VERSION from Github, provided a given git SHA
docker-create-manifest: docker-hub-login
	$(DRY_RUN_ECHO) docker manifest create "docker.io/infinyon/fluvio:$(DOCKER_IMAGE_TAG)" \
		"docker.io/infinyon/fluvio:$(DEV_VERSION_TAG)-amd64" \
		"docker.io/infinyon/fluvio:$(DEV_VERSION_TAG)-arm64v8"

docker-push-manifest: docker-create-manifest-dev
	$(DRY_RUN_ECHO) docker manifest push "docker.io/infinyon/fluvio:$(DOCKER_IMAGE_TAG)"

# Create latest development Fluvio image
docker-create-manifest-dev: DOCKER_IMAGE_TAG=latest
docker-create-manifest-dev: docker-hub-login
#	$(DRY_RUN_ECHO) docker manifest create "docker.io/infinyon/fluvio:latest" \
#		"docker.io/infinyon/fluvio:$(DEV_VERSION_TAG)-amd64" \
#		"docker.io/infinyon/fluvio:$(DEV_VERSION_TAG)-arm64v8"

# Push docker manifest
docker-push-manifest-dev: DOCKER_IMAGE_TAG=$(DEV_VERSION_TAG)
docker-push-manifest-dev: docker-create-manifest-dev
#	$(DRY_RUN_ECHO) docker manifest push "docker.io/infinyon/fluvio:$(DEV_VERSION_TAG)"


# Expects GH_TOKEN env var to have token
#gh-cli-login-token:
#	gh auth login --with-token

# Uses $(VERSION)
curl-install-fluvio:
	curl -fsS https://packages.fluvio.io/v1/install.sh | bash


install-fluvio-stable: VERSION=stable
install-fluvio-stable: curl-install-fluvio

install-fluvio-latest: VERSION=latest
install-fluvio-latest: curl-install-fluvio

# Tag images


# Target that logs in to GH CLI. Assuming a token is provided, unless explicitly skipped

# Target that install fluvio-package
install-fluvio-package:
	$(FLUVIO_BIN) install fluvio-package

# Target that downloads the GH dev release
# Requires GH_TOKEN set
DOWNLOAD_FLUVIO_RELEASE_CMD = gh release download $(GH_RELEASE_TAG) -R infinyon/fluvio --skip-existing
# Obviously don't commit this
download-fluvio-release:
	$(call DOWNLOAD_FLUVIO_RELEASE_CMD)


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
unzip-gh-release-artifacts: download-fluvio-release
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
		$(DRY_RUN_ECHO) $(FLUVIO_BIN) package publish \
			--package=$$PACKAGE \
			--version=$(GH_RELEASE_TAG) \
			--target=$$TARGET \
			$$ARTIFACT \
	)

publish-artifacts-dev: GH_RELEASE_TAG=dev
publish-artifacts-dev: publish-artifacts

#	for GH_ARTIFACT in *.zip; do \
#		echo filename: $(basename $$GH_ARTIFACT); \
#	done

# Moved this from cargo-make. It was called "bump-fluvio-latest" and only run in CI

# Version convention is different here. Notice the `+`
#bump-fluvio-latest: DEV_VERSION_TAG=$(VERSION)+$(GIT_COMMIT_SHA)

bump-fluvio: PUBLIC_VERSION=$(subst -,+,$(VERSION))
bump-fluvio:
	@$(DRY_RUN_ECHO) $(FLUVIO_BIN) package publish bump $(CHANNEL_TAG) $(PUBLIC_VERSION)
	@$(DRY_RUN_ECHO) $(foreach bin, $(PUBLISH_BINARIES), \
		$(shell $(DRY_RUN_ECHO) $(FLUVIO_BIN) package tag $(bin):$(PUBLIC_VERSION) --allow-missing-targets --tag=$(CHANNEL_TAG) --force) \
	)

bump-fluvio-latest: VERSION=$(subst -, +, $(DEV_VERSION_TAG))
bump-fluvio-latest: install-fluvio-latest install-fluvio-package bump-fluvio
#	$(DRY_RUN_ECHO) $(FLUVIO_BIN) package publish bump latest $(DEV_VERSION_TAG_PLUS)
#	$(DRY_RUN_ECHO) $(foreach bin, $(PUBLISH_BINARIES), \
#		$(shell $(DRY_RUN_ECHO) $(FLUVIO_BIN) package tag $(bin):$(DEV_VERSION_TAG_PLUS) --allow-missing-targets --tag=latest --force) \
#	)
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

update-public-installer-script-s3:
	$(DRY_RUN_ECHO) aws s3 cp ./install.sh s3://packages.fluvio.io/v1/install.sh --acl public-read


# Obviously don't commit this
latest-cd-dev-status:
	gh api /repos/{owner}/{repo}/actions/workflows/cd_dev.yaml/runs | jq .workflow_runs[0] > /tmp/cd_dev_latest.txt
	echo "Latest CD_Dev run: $(shell cat /tmp/cd_dev_latest.txt | jq .html_url | tr -d '"')"
ifeq ($(shell cat /tmp/cd_dev_latest.txt | jq .conclusion | tr -d '"'), success)
	echo $(shell echo ✅ Most recent CD_Dev run passed)
	exit 0;
else
	echo $(shell echo ❌ Most recent CD_Dev run failed)
	exit 1;
endif

build-release-notes:
	rm --verbose /tmp/release_notes
	touch /tmp/release_notes
	echo "# Release Notes" >> /tmp/release_notes
	export VERSION=$(shell cat VERSION)
	cat CHANGELOG.md | sed -e '/./{H;$$!d;}' -e "x;/##\ Platform\ Version\ $$VERSION/"'!d;' >> /tmp/release_notes

	# Replace UNRELEASED w/ date YYYY-MM-dd
	export TZ=":America/Los_Angeles"
	cat /tmp/release_notes | sed -i "s/UNRELEASED/$(shell date +%F)/" /tmp/release_notes

	# Print the release notes to stdout
	cat /tmp/release_notes


create-gh-release: build-release-notes
	# Create temporary directory to download all artifacts
	mkdir -p tmp-release
	#$(shell cd tmp-release; $(DRY_RUN_ECHO) gh release download -R infinyon/fluvio dev)
	cd tmp-release; $(call DOWNLOAD_FLUVIO_RELEASE_CMD)

	$(DRY_RUN_ECHO) $(shell cd tmp-release; $(DRY_RUN_ECHO) gh release create -R infinyon/fluvio \
		--title="v$(VERSION)" \
		-F /tmp/release_notes \
		"v$(VERSION)" \
		$(wildcard tmp-release/*))