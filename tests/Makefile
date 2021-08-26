flvt=../target/debug/fluvio-test
DRIVER=--local-driver
COUNT=3000

build:
	cargo build

test:	run-local run-k8

smoke-test:	build
	$(flvt) $(DRIVER)

run-stress:	build
	$(flvt) $(DRIVER) --produce-count	$(COUNT)
