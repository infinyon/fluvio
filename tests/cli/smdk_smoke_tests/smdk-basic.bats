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
    TEST_DIR="$(mktemp -d -t smdk-test.XXXXX)"
    export TEST_DIR
}

### Using crates.io dependency for `fluvio-smartmodule`

@test "Generate and build filter - default case" {
    LABEL=default
    SM_TYPE=filter
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --no-params \
        --sm-type $SM_TYPE \
        --silent \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build map - default case" {
    LABEL=default
    SM_TYPE=map
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --no-params \
        --sm-type $SM_TYPE \
        --silent \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build array-map - default case" {
    LABEL=default
    SM_TYPE=array-map
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --no-params \
        --sm-type $SM_TYPE \
        --silent \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build filter-map - default case" {
    LABEL=default
    SM_TYPE=filter-map
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --no-params \
        --sm-type $SM_TYPE \
        --silent \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build aggregate - default case" {
    LABEL=default
    SM_TYPE=aggregate
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --no-params \
        --sm-type $SM_TYPE \
        --silent \
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
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --with-params \
        --sm-type $SM_TYPE \
        --silent \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build map - default with params" {
    LABEL=default-params
    SM_TYPE=map
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --with-params \
        --sm-type $SM_TYPE \
        --silent \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build array-map - default with params" {
    LABEL=default-params
    SM_TYPE=array-map
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --with-params \
        --sm-type $SM_TYPE \
        --silent \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build filter-map - default with params" {
    LABEL=default-params
    SM_TYPE=filter-map
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --with-params \
        --sm-type $SM_TYPE \
        --silent \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build aggregate - default with params" {
    LABEL=default-params
    SM_TYPE=aggregate
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --with-params \
        --sm-type $SM_TYPE \
        --silent \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

### Using git dependency (develop) for `fluvio-smartmodule`

@test "Generate and build filter - develop case" {
    skip "Re-enable after merge, due to rename of template variables"
    LABEL=develop
    SM_TYPE=filter
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --no-params \
        --sm-type $SM_TYPE \
        --silent \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build map - develop case" {
    skip "Re-enable after merge, due to rename of template variables"
    LABEL=develop
    SM_TYPE=map
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --no-params \
        --sm-type $SM_TYPE \
        --silent \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build array-map - develop case" {
    skip "Re-enable after merge, due to rename of template variables"
    LABEL=develop
    SM_TYPE=array-map
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --no-params \
        --sm-type $SM_TYPE \
        --silent \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build filter-map - develop case" {
    skip "Re-enable after merge, due to rename of template variables"
    LABEL=develop
    SM_TYPE=filter-map
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --no-params \
        --sm-type $SM_TYPE \
        --silent \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build aggregate - develop case" {
    skip "Re-enable after merge, due to rename of template variables"
    LABEL=develop
    SM_TYPE=aggregate
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --no-params \
        --sm-type $SM_TYPE \
        --silent \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

### Using git dependency (develop) for `fluvio-smartmodule` with params

@test "Generate and build filter - develop with params" {
    skip "Re-enable after merge, due to rename of template variables"
    LABEL=develop-params
    SM_TYPE=filter
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --with-params \
        --sm-type $SM_TYPE \
        --silent \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build map - develop with params" {
    skip "Re-enable after merge, due to rename of template variables"
    LABEL=develop-params
    SM_TYPE=map
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --with-params \
        --sm-type $SM_TYPE \
        --silent \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build array-map - develop with params" {
    skip "Re-enable after merge, due to rename of template variables"
    LABEL=develop-params
    SM_TYPE=array-map
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --with-params \
        --sm-type $SM_TYPE \
        --silent \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build filter-map - develop with params" {
    skip "Re-enable after merge, due to rename of template variables"
    LABEL=develop-params
    SM_TYPE=filter-map
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --with-params \
        --sm-type $SM_TYPE \
        --silent \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}

@test "Generate and build aggregate - develop with params" {
    skip "Re-enable after merge, due to rename of template variables"
    LABEL=develop-params
    SM_TYPE=aggregate
    cd $TEST_DIR
    run $SMDK_BIN generate \
        --with-params \
        --sm-type $SM_TYPE \
        --silent \
        --develop \
        $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    assert_success
    cd $LABEL-$SM_TYPE-$PROJECT_NAME_PREFIX
    run $SMDK_BIN build
    refute_output --partial "could not compile"
}
