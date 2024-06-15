FLUVIO_BIN=target/debug/fluvio
SPU=2
REPLICATION=2

fluvio:
	cargo build --bin fluvio

# make -f src/spu/Makefile test_replica
test_replica:	fluvio
	$(FLUVIO_BIN) cluster delete --force
	$(FLUVIO_BIN) cluster start --spu $(SPU) --local --develop --rust-log=fluvio_spu=debug
	$(FLUVIO_BIN) topic create test --replication $(REPLICATION)
	echo "hello world" | $(FLUVIO_BIN) produce test
	sleep 2
	$(FLUVIO_BIN) partition list




