BASE_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
TARGET=target
FLV=$(BASE_DIR)/$(TARGET)/debug/fluvio
TOPIC=boats
EDGE=boat1
UPSTREAM=host.orb.internal
ES="ssh orb"
LINUX_AA64_TARGET=target/aarch64-unknown-linux-musl
ORB_CMD="orb run"
TLS=--tls --domain fluvio.local --server-key $(BASE_DIR)/tls/certs/server.key --server-cert $(BASE_DIR)/tls/certs/server.crt --ca-cert $(BASE_DIR)/tls/certs/ca.crt --client-cert $(BASE_DIR)/tls/certs/client-root.crt --client-key $(BASE_DIR)/tls/certs/client-root.key
DEBUG=--rust-log fluvio=debug

clean:
	$(FLV) cluster delete

local-cluster: clean
	$(FLV) cluster start --local $(TLS) $(DEBUG)

upstream-setup:	local-cluster
	$(FLV) topic create $(TOPIC) --mirror-assignment mirror_assignment.json
	$(FLV) cluster remote-cluster register --type mirror-edge $(EDGE)
	$(FLV) cluster remote-cluster metadata export --topic $(TOPIC) \
	     --mirror $(EDGE) --upstream $(UPSTREAM) --file $(BASE_DIR)/$(EDGE).json


remote-cleanup: TARGET=$(LINUX_AA64_TARGET)
remote-cleanup:
	echo "using $(FLV)"
	orb run $(FLV) cluster shutdown --local
	orb run rm -rf /home/$(LOGNAME)/.fluvio
	-pkill -f "nohup orb"


remote-edge1: TARGET=$(LINUX_AA64_TARGET)
remote-edge1: remote-cleanup
	nohup orb run $(FLV) cluster start --read-only $(BASE_DIR)/$(EDGE).json --no-stop $(DEBUG) &

produce-edge1: TARGET=$(LINUX_AA64_TARGET)
produce-edge1:
	echo first | orb run $(FLV) produce $(TOPIC)
	echo second | orb run $(FLV) produce $(TOPIC)
