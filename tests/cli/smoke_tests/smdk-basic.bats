#!/usr/bin/env bats

SKIP_CLUSTER_START=true
export SKIP_CLUSTER_START

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    PROJECT_NAME_PREFIX="$(random_string)"
    export PROJECT_NAME_PREFIX
    TEST_DIR="$(mktemp -d -t smdk-test)"
    export TEST_DIR
}

### Using crates.io dependency for `fluvio-smartmodule`

@test "Generate and build filter - default case" {
    LABEL=default
    SM_TYPE=filter
    SM_PARAMS=false
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build map - default case" {
    LABEL=default
    SM_TYPE=map
    SM_PARAMS=false
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build array-map - default case" {
    LABEL=default
    SM_TYPE=array-map
    SM_PARAMS=false
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build filter-map - default case" {
    LABEL=default
    SM_TYPE=filter-map
    SM_PARAMS=false
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build aggregate - default case" {
    LABEL=default
    SM_TYPE=aggregate
    SM_PARAMS=false
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

### Using crates.io dependency for `fluvio-smartmodule` with init fn

@test "Generate and build filter - default with init" {
    LABEL=default-init
    SM_TYPE=filter
    SM_PARAMS=false
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build map - default with init" {
    LABEL=default-init
    SM_TYPE=map
    SM_PARAMS=false
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build array-map - default with init" {
    LABEL=default-init
    SM_TYPE=array-map
    SM_PARAMS=false
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build filter-map - default with init" {
    LABEL=default-init
    SM_TYPE=filter-map
    SM_PARAMS=false
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build aggregate - default with init" {
    LABEL=default-init
    SM_TYPE=aggregate
    SM_PARAMS=false
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

### Using crates.io dependency for `fluvio-smartmodule` with params

@test "Generate and build filter - default with params" {
    LABEL=default-params
    SM_TYPE=filter
    SM_PARAMS=true
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build map - default with params" {
    LABEL=default-params
    SM_TYPE=map
    SM_PARAMS=true
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build array-map - default with params" {
    LABEL=default-params
    SM_TYPE=array-map
    SM_PARAMS=true
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build filter-map - default with params" {
    LABEL=default-params
    SM_TYPE=filter-map
    SM_PARAMS=true
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build aggregate - default with params" {
    LABEL=default-params
    SM_TYPE=aggregate
    SM_PARAMS=true
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

### Using crates.io dependency for `fluvio-smartmodule` with init fn and params

@test "Generate and build filter - default with init + params" {
    LABEL=default-init-params
    SM_TYPE=filter
    SM_PARAMS=true
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build map - default with init + params" {
    LABEL=default-init-params
    SM_TYPE=map
    SM_PARAMS=true
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build array-map - default with init + params" {
    LABEL=default-init-params
    SM_TYPE=array-map
    SM_PARAMS=true
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build filter-map - default with init + params" {
    LABEL=default-init-params
    SM_TYPE=filter-map
    SM_PARAMS=true
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build aggregate - default with init + params" {
    LABEL=default-init-params
    SM_TYPE=aggregate
    SM_PARAMS=true
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

### Using git dependency (develop) for `fluvio-smartmodule`

@test "Generate and build filter - develop case" {
    LABEL=develop
    SM_TYPE=filter
    SM_PARAMS=false
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build map - develop case" {
    LABEL=develop
    SM_TYPE=map
    SM_PARAMS=false
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build array-map - develop case" {
    LABEL=develop
    SM_TYPE=array-map
    SM_PARAMS=false
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build filter-map - develop case" {
    LABEL=develop
    SM_TYPE=filter-map
    SM_PARAMS=false
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build aggregate - develop case" {
    LABEL=develop
    SM_TYPE=aggregate
    SM_PARAMS=false
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

### Using git dependency (develop) for `fluvio-smartmodule` with init fn

@test "Generate and build filter - develop with init" {
    LABEL=develop-init
    SM_TYPE=filter
    SM_PARAMS=false
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build map - develop with init" {
    LABEL=develop-init
    SM_TYPE=map
    SM_PARAMS=false
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build array-map - develop with init" {
    LABEL=develop-init
    SM_TYPE=array-map
    SM_PARAMS=false
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build filter-map - develop with init" {
    LABEL=develop-init
    SM_TYPE=filter-map
    SM_PARAMS=false
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build aggregate - develop with init" {
    LABEL=develop-init
    SM_TYPE=aggregate
    SM_PARAMS=false
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

### Using git dependency (develop) for `fluvio-smartmodule` with params

@test "Generate and build filter - develop with params" {
    LABEL=develop-params
    SM_TYPE=filter
    SM_PARAMS=true
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build map - develop with params" {
    LABEL=develop-params
    SM_TYPE=map
    SM_PARAMS=true
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build array-map - develop with params" {
    LABEL=develop-params
    SM_TYPE=array-map
    SM_PARAMS=true
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build filter-map - develop with params" {
    LABEL=develop-params
    SM_TYPE=filter-map
    SM_PARAMS=true
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build aggregate - develop with params" {
    LABEL=develop-params
    SM_TYPE=aggregate
    SM_PARAMS=true
    SM_INIT=false
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

### Using git dependency (develop) for `fluvio-smartmodule` with init fn and params

@test "Generate and build filter - develop with init + params" {
    LABEL=develop-init-params
    SM_TYPE=filter
    SM_PARAMS=true
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build map - develop with init + params" {
    LABEL=develop-init-params
    SM_TYPE=map
    SM_PARAMS=true
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build array-map - develop with init + params" {
    LABEL=develop-init-params
    SM_TYPE=array-map
    SM_PARAMS=true
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build filter-map - develop with init + params" {
    LABEL=develop-init-params
    SM_TYPE=filter-map
    SM_PARAMS=true
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build aggregate - develop with init + params" {
    LABEL=develop-init-params
    SM_TYPE=aggregate
    SM_PARAMS=true
    SM_INIT=true
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --init-fn $SM_INIT \
        --smart-module-params $SM_PARAMS \
        --smart-module-type $SM_TYPE \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}
