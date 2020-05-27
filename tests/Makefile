flvt=../target/debug/flv-test

build:
	cargo build

test:	run-local run-k8

run-local:	build
	$(flvt) --local-driver

run-local-tols:	build
	$(flvt) --local-driver 

run-k8:	build
	$(flvt)




