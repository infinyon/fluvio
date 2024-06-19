
SMDK_BIN ?= smdk
SMDK_OPT ?=

default: build

setup:
	rustup target add wasm32-wasi

build: setup
	${SMDK_BIN} build ${SMDK_OPT}

build-raw:
	cargo build --target wasm32-wasi

