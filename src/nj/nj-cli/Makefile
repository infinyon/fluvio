MPATH = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))

install: build
	cargo install --path $(MPATH)

build:
	cargo build

