TEST_BIN?=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/fluvio-test,./target/$(BUILD_PROFILE)/fluvio-test)
CLIENT_LOG?=warn
SERVER_LOG?=info
DEFAULT_SPU?=2
REPL?=2
DEFAULT_ITERATION?=1000
SPU_DELAY?=5
SC_AUTH_CONFIG?=./crates/fluvio-sc/test-data/auth_config
EXTRA_ARG?=

# Test env
TEST_ENV_AUTH_POLICY=
TEST_ENV_FLV_SPU_DELAY=

# Test args
TEST_ARG_SPU=--spu ${DEFAULT_SPU}
TEST_ARG_LOG=--client-log ${CLIENT_LOG} --server-log ${SERVER_LOG}
TEST_ARG_REPLICATION=-r ${REPL}
TEST_ARG_DEVELOP=$(if $(IMAGE_VERSION),--image-version ${IMAGE_VERSION}, --develop)
TEST_ARG_SKIP_CHECKS=
TEST_ARG_EXTRA=
TEST_ARG_CONSUMER_WAIT=
TEST_ARG_PRODUCER_ITERATION=--producer-iteration=${DEFAULT_ITERATION}
TEST_ARG_CONNECTOR_CONFIG=
TEST_ARG_TABLE_FORMAT_CONFIG=
TEST_ARG_SKIP_TEST_CONNECTOR=$(if $(SKIP_TEST_CONNECTOR),--skip-test-connector,)
TEST_ARG_COMMON = ${TEST_ARG_SPU} \
                ${TEST_ARG_LOG} \
                ${TEST_ARG_REPLICATION} \
                ${TEST_ARG_DEVELOP} \
                ${TEST_ARG_EXTRA}



ifeq ($(UNINSTALL),noclean)
clean_cluster:
	echo "no clean"
else
clean_cluster:
	echo "clean up previous installation"
	$(FLUVIO_BIN) cluster delete
endif

test-setup:	build-test-ci clean_cluster



validate-test-harness: test-setup
	$(TEST_BIN) expected_pass ${TEST_ARG_EXTRA}
	$(TEST_BIN) expected_fail --expect-fail
	$(TEST_BIN) expected_fail_join_fail_first --expect-fail
	$(TEST_BIN) expected_fail_join_success_first --expect-fail
	$(TEST_BIN) expected_timeout --timeout 5sec --expect-timeout

validate-test-harness-local: TEST_ARG_EXTRA=--local  --cluster-start
validate-test-harness-local: validate-test-harness

# To run a smoke test locally: make smoke-test-local EXTRA_ARG=--cluster-start
smoke-test: test-setup
	# Set ENV
	$(TEST_ENV_AUTH_POLICY) \
	$(TEST_ENV_FLV_SPU_DELAY) \
		$(TEST_BIN) smoke \
			${TEST_ARG_COMMON} \
			-- \
			${TEST_ARG_CONSUMER_WAIT} \
			${TEST_ARG_PRODUCER_ITERATION} \
			${TEST_ARG_CONNECTOR_CONFIG} \
			${TEST_ARG_TABLE_FORMAT_CONFIG} \
			${TEST_ARG_SKIP_TEST_CONNECTOR}

smoke-test-local: TEST_ARG_EXTRA=--local  $(EXTRA_ARG)
smoke-test-local: smoke-test

smoke-test-stream: TEST_ARG_EXTRA=--local $(EXTRA_ARG)
smoke-test-stream: TEST_ARG_CONSUMER_WAIT=--consumer-wait=true
smoke-test-stream: smoke-test

smoke-test-tls: TEST_ARG_EXTRA=--tls --local $(EXTRA_ARG)
smoke-test-tls: smoke-test

smoke-test-tls-policy: TEST_ENV_AUTH_POLICY=AUTH_POLICY=$(SC_AUTH_CONFIG)/policy.json X509_AUTH_SCOPES=$(SC_AUTH_CONFIG)/scopes.json
smoke-test-tls-policy: TEST_ENV_FLV_SPU_DELAY=FLV_SPU_DELAY=$(SPU_DELAY)
smoke-test-tls-policy: TEST_ARG_EXTRA=--tls --local  $(EXTRA_ARG)
smoke-test-tls-policy: smoke-test

# test rbac with ROOT user
smoke-test-tls-root: smoke-test-tls-policy test-permission-user1


# delivery semantics
smoke-test-at-most-once: TEST_ARG_EXTRA=--producer-delivery-semantic at-most-once $(EXTRA_ARG)
smoke-test-at-most-once: smoke-test

# election test only runs on local
election-test: TEST_ARG_EXTRA=--local $(EXTRA_ARG)
election-test: test-setup
	$(TEST_BIN) election  ${TEST_ARG_COMMON}

multiple-partition-test: TEST_ARG_EXTRA=--local $(EXTRA_ARG)
multiple-partition-test: test-setup
	$(TEST_BIN) multiple_partition --partition 10 \


batch-failure-test: TEST_ARG_EXTRA=--local $(EXTRA_ARG)
batch-failure-test: FLV_SOCKET_WAIT=25
batch-failure-test: DEFAULT_SPU=1
batch-failure-test: REPL=1
batch-failure-test: test-setup
	$(TEST_BIN) produce_batch  ${TEST_ARG_COMMON}

batching-test: TEST_ARG_EXTRA=--local $(EXTRA_ARG)
batching-test: DEFAULT_SPU=1
batching-test: REPL=1
batching-test: test-setup
	$(TEST_BIN) batching  ${TEST_ARG_COMMON}

reconnection-test: TEST_ARG_EXTRA=--local $(EXTRA_ARG)
reconnection-test: DEFAULT_SPU=1
reconnection-test: REPL=1
reconnection-test: test-setup
	$(TEST_BIN) reconnection  ${TEST_ARG_COMMON}

# test rbac with user1 who doesn't have topic creation permission
# assumes cluster is set
SC_HOST=localhost
SC_PORT=9003
test-permission-user1:
	rm -f /tmp/topic.err
	- $(FLUVIO_BIN) --cluster ${SC_HOST}:${SC_PORT} \
		--tls --enable-client-cert --domain fluvio.local \
		--ca-cert tls/certs/ca.crt \
		--client-cert tls/certs/client-user1.crt \
		--client-key tls/certs/client-user1.key \
		 topic create test3 2> /tmp/topic.err
	grep -q permission /tmp/topic.err



# Kubernetes Tests

smoke-test-k8: TEST_ARG_EXTRA=$(EXTRA_ARG)
smoke-test-k8: TEST_ARG_TABLE_FORMAT_CONFIG=--table-format-config ./tests/test-table-format-config.yaml
smoke-test-k8: build_k8_image smoke-test

smoke-test-k8-tls: TEST_ARG_EXTRA=--tls $(EXTRA_ARG)
smoke-test-k8-tls: TEST_ARG_TABLE_FORMAT_CONFIG=--table-format-config ./tests/test-table-format-config.yaml
smoke-test-k8-tls: build_k8_image smoke-test

smoke-test-k8-tls-policy-setup:
	kubectl delete configmap authorization --ignore-not-found
	kubectl create configmap authorization --from-file=POLICY=${SC_AUTH_CONFIG}/policy.json --from-file=SCOPES=${SC_AUTH_CONFIG}/scopes.json
smoke-test-k8-tls-policy: TEST_ENV_FLV_SPU_DELAY=FLV_SPU_DELAY=$(SPU_DELAY)
smoke-test-k8-tls-policy: TEST_ARG_EXTRA=--tls --authorization-config-map authorization $(EXTRA_ARG)
smoke-test-k8-tls-policy: TEST_ARG_TABLE_FORMAT_CONFIG=--table-format-config ./tests/test-table-format-config.yaml
smoke-test-k8-tls-policy: build_k8_image smoke-test

test-permission-k8:	SC_HOST=$(shell kubectl get node -o json | jq '.items[].status.addresses[0].address' | tr -d '"' )
test-permission-k8:	SC_PORT=$(shell kubectl get svc fluvio-sc-public -o json | jq '.spec.ports[0].nodePort' )
test-permission-k8:	test-permission-user1

# run auth policy without setup, UNCLEAN must not be set
smoke-test-k8-tls-root-unclean: smoke-test-k8-tls-policy test-permission-k8

# run auth policy with setup
smoke-test-k8-tls-root: smoke-test-k8-tls-policy-setup smoke-test-k8-tls-policy  test-permission-k8

install-test-k8-port-forwarding: build_k8_image
install-test-k8-port-forwarding:
	$(FLUVIO_BIN) cluster start --develop --use-k8-port-forwarding

ifeq (${CI},true)
# In CI, we expect all artifacts to already be built and loaded for the script
upgrade-test:
	./tests/upgrade-test.sh
else
# When not in CI (i.e. development), load the dev k8 image before running test
upgrade-test: build-cli build_k8_image
	./tests/upgrade-test.sh
endif

# When running in development, might need to run `cargo clean` to ensure correct fluvio binary is used
validate-release-stable:
	./tests/fluvio-validate-release.sh $(VERSION) $(GIT_COMMIT)

ifeq (${CI},true)
# In CI, we expect all artifacts to already be built and loaded for the script
longevity-test:
	$(TEST_BIN) longevity --expect-timeout -- --runtime-seconds=1800 --producers 10 --consumers 10
else
# When not in CI (i.e. development), load the dev k8 image before running test
longevity-test: build-test
	$(TEST_BIN) longevity --expect-timeout -- $(VERBOSE_FLAG) --runtime-seconds=60
endif

cli-platform-cross-version-test:
	bats -t ./tests/cli/cli-platform-cross-version.bats

cli-fluvio-smoke:
	bats $(shell ls -1 ./tests/cli/fluvio_smoke_tests/*.bats | sort -R)
	bats ./tests/cli/fluvio_smoke_tests/non-concurrent/cluster-delete.bats

cli-smdk-smoke:
	bats $(shell ls -1 ./tests/cli/smdk_smoke_tests/*.bats | sort -R)

cli-cdk-smoke:
	bats $(shell ls -1 ./tests/cli/cdk_smoke_tests/*.bats | sort -R)

cli-basic-test:
	bats ./tests/cli/fluvio_smoke_tests/e2e-basic.bats

cli-smartmodule-all-test:
	bats ./tests/cli/fluvio_smoke_tests/e2e-smartmodule-basic.bats

cli-smartmodule-aggregate-test:
	bats -f aggregate ./tests/cli/fluvio_smoke_tests/e2e-smartmodule-basic.bats

cli-smartmodule-basic-test:
	bats ./tests/cli/fluvio_smoke_tests/smartmodule-basic.bats

cli-producer-smartmodule-test:
	bats ./tests/cli/fluvio_smoke_tests/producer-smartmodule.bats

stats-test:
	$(TEST_BIN) stats -- $(VERBOSE_FLAG) --tolerance=5

cli-smdk-basic-test:
	SMDK_BIN=$(shell readlink -f $(SMDK_BIN)) bats   ./tests/cli/smdk_smoke_tests/smdk-basic.bats

cli-cdk-basic-test:
	CDK_BIN=$(shell readlink -f $(CDK_BIN)) bats   ./tests/cli/cdk_smoke_tests/cdk-basic.bats

# test rbac
#
#
#

test-rbac:
	AUTH_POLICY=$(POLICY_FILE) X509_AUTH_SCOPES=$(SCOPE) make smoke-test-tls DEFAULT_LOG=fluvio=debug

# In CI mode, do not run any build steps
ifeq (${CI},true)
build-test-ci:
else
# When not in CI (i.e. development), need build cli and cluster
build-test-ci: build-test build-cli build-cluster
endif


FORCE_TEST_PANIC_FLAG=$(if $(FORCE_TEST_PANIC),--force-panic)

ifeq (${CI},true)
fluvio-test-self-check:
else
fluvio-test-self-check: build-test
endif
	$(TEST_BIN) self_check -- $(FORCE_TEST_PANIC_FLAG)
