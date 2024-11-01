BASE_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../)
FLVR = $(BASE_DIR)/target/debug/fluvio-run
FLV = $(BASE_DIR)/target/debug/fluvio
EDGE_PROFILE = edge-local
EDGE_CLUSTER_ADDR = 127.0.0.1:9203

setup-home:
	$(FLV) remote register edge1
	$(FLV) topic create edge-cmd-request  --mirror-apply mirror.json --home-to-remote
	$(FLV) topic create edge-cmd-reply  --mirror-apply mirror.json

export-home:
	$(FLV) remote export edge1  --file  edge/edge1-remote.json


start-edge-cluster: setup-remote start-sc-edge start-spu-edge setup-edge-cluster

setup-remote:	clean
	mkdir -p sc
	mkdir -p spu

start-sc-edge:
	RUST_LOG=fluvio_sc=info  nohup $(FLVR) sc --local sc --bind-public $(EDGE_CLUSTER_ADDR) --bind-private 127.0.0.1:9204 > sc/sc.log 2>&1 &

start-spu-edge:
	RUST_LOG=fluvio_spu=info nohup $(FLVR) spu -i 5001 -p 0.0.0.0:9210 -v 0.0.0.0:9211 --sc-addr 127.0.0.1:9204 --log-base-dir spu > spu/spu.log 2>&1 &

setup-edge-cluster:
	sleep 5
	$(FLV) --profile $(EDGE_PROFILE) cluster spu register --id 5001 --public-server 0.0.0.0:9210  --private-server spu:9211

create-edge-profile:
	$(FLV) profile add $(EDGE_PROFILE) $(EDGE_CLUSTER_ADDR) local

connect-edge:
	$(FLV) --profile $(EDGE_PROFILE) home connect --file edge1-remote.json

clean:
	rm -rf sc spu