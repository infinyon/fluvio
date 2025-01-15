ORG=myorg
IMAGE=http-source
VERSION=0.3.8

copy-profile:
	fluvio profile export > fluvio_profile.toml

build: copy-profile
	docker build --progress=plain -t $(ORG)/$(IMAGE):$(VERSION) .

run: build
	docker run --network host --rm  $(ORG)/$(IMAGE):$(VERSION)