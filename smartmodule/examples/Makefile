default: build

SMDK_BIN ?= smdk
SMDK_OPT ?=

SUBDIRS := $(filter-out target/., $(wildcard */.))

.PHONY: build $(SUBDIRS)

setup:
	rustup target add wasm32-wasip1
	@echo SMDK_BIN ${SMDK_BIN}

dirs:
	echo ${SUBDIRS}

build: setup $(SUBDIRS)

$(SUBDIRS):
	@echo Building smartmodule example $@
	cd $@ && ${SMDK_BIN} build ${SMDK_OPT}

build-raw: setup
	cargo build --release

clean:
	cargo clean
