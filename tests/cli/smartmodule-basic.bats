#!/usr/bin/env bats

load test_helper/tools_check.bash
load test_helper/setup_k8_cluster.bash

setup() {
    load test_helper/random_string.bash
    export SMARTMODULE_NAME=$RANDOM_STRING
}

# Create smartmodule
@test "Create smartmodule" {
    run $FLUVIO_BIN smartmodule create $SMARTMODULE_NAME
}

# Create smartmodule - Negative test
@test "Attempt to create a smartmodule with same name" {
    run -1 $FLUVIO_BIN smartmodule create $SMARTMODULE_NAME
}

# Describe smartmodule
@test "Describe smartmodule" {
    run $FLUVIO_BIN smartmodule describe $SMARTMODULE_NAME 
}

# List smartmodule
@test "List smartmodule" {
    run $FLUVIO_BIN smartmodule list
}

# Delete smartmodule
@test "Delete smartmodule" {
    run $FLUVIO_BIN smartmodule delete $SMARTMODULE_NAME
}

# Delete smartmodule - Negative test
@test "Attempt to delete a smartmodule that doesn't exist" {
    run -1 $FLUVIO_BIN smartmodule delete $SMARTMODULE_NAME
}