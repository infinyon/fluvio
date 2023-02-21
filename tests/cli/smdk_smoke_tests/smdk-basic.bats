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

    SMDK_TEMPLATE_PATH_FLAG="--template-path $(pwd)/smartmodule"
    export SMDK_TEMPLATE_PATH_FLAG

    TESTING_GROUP_NAME_FLAG="--project-group=smdk-smoke-test-group"
    export TESTING_GROUP_NAME_FLAG

    # Create a workspace to facilitate dependency sharing between test cases SMs
    cd $TEST_DIR
    echo '[workspace]'            > Cargo.toml
    echo                         >> Cargo.toml
    echo 'members = ['           >> Cargo.toml
    echo ']'                     >> Cargo.toml
    echo                         >> Cargo.toml
    echo '[profile.release-lto]' >> Cargo.toml
    echo 'inherits = "release"'  >> Cargo.toml
    echo 'lto = true'            >> Cargo.toml
    echo 'strip = "symbols"'     >> Cargo.toml
    cd -
}

### Using crates.io dependency for `fluvio-smartmodule`

@test "Generate and test filter - (stable fluvio-smartmodule / no params)" {
    LABEL=default
    SMDK_SM_TYPE=filter
    PARAMS_FLAG=--no-params
    SM_CRATE_PATH_FLAG=
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"

    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success

    # Test
    run $SMDK_BIN test --text 'a'
    assert_output --partial "1 records outputed"
    assert_success
}

@test "Generate and test map - (stable fluvio-smartmodule / no params)" {
    LABEL=default
    SMDK_SM_TYPE=map
    PARAMS_FLAG=--no-params
    SM_CRATE_PATH_FLAG=
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success

    # Test
    run $SMDK_BIN test --text '2'
    assert_output --partial "1 records outputed"
    assert_output --partial "4"
    assert_success
}

@test "Generate and test array-map - (stable fluvio-smartmodule / no params)" {
    LABEL=default
    SMDK_SM_TYPE=array-map
    PARAMS_FLAG=--no-params
    SM_CRATE_PATH_FLAG=
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success    

    # Test
    run $SMDK_BIN test --text '["foo", "bar"]'
    assert_output --partial "2 records outputed"
    assert_output --partial "foo"
    assert_output --partial "bar"
    assert_success
}

@test "Generate and test filter-map - (stable fluvio-smartmodule / no params)" {
    LABEL=default
    SMDK_SM_TYPE=filter-map
    PARAMS_FLAG=--no-params
    SM_CRATE_PATH_FLAG=
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    cd $TEST_DIR
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success

    # Test
    run $SMDK_BIN test --text '2'
    assert_output --partial "1 records outputed"
    assert_output --partial "2"
    assert_success
}

@test "Generate and test aggregate - (stable fluvio-smartmodule / no params)" {
    LABEL=default
    SMDK_SM_TYPE=aggregate
    PARAMS_FLAG=--no-params
    SM_CRATE_PATH_FLAG=
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success

    # Test
    run $SMDK_BIN test --text '2'
    assert_output --partial "1 records outputed"
    assert_output --partial "2"
    assert_success
}

### Using crates.io dependency for `fluvio-smartmodule` with params

@test "Generate and test filter - (stable fluvio-smartmodule / with params)" {
    LABEL=default-params
    SMDK_SM_TYPE=filter
    PARAMS_FLAG=--with-params
    SM_CRATE_PATH_FLAG=
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success

    # Test
    run $SMDK_BIN test --text 'a' -e key=value
    assert_output --partial "1 records outputed"
    assert_success
}

@test "Generate and build map - (stable fluvio-smartmodule / with params)" {
    LABEL=default-params
    SMDK_SM_TYPE=map
    PARAMS_FLAG=--with-params
    SM_CRATE_PATH_FLAG=
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success
}

@test "Generate and build array-map - (stable fluvio-smartmodule / with params)" {
    LABEL=default-params
    SMDK_SM_TYPE=array-map
    PARAMS_FLAG=--with-params
    SM_CRATE_PATH_FLAG=
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success
}

@test "Generate and build filter-map - (stable fluvio-smartmodule / with params)" {
    LABEL=default-params
    SMDK_SM_TYPE=filter-map
    PARAMS_FLAG=--with-params
    SM_CRATE_PATH_FLAG=
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success
}

@test "Generate and build aggregate - (stable fluvio-smartmodule / with params)" {
    LABEL=default-params
    SMDK_SM_TYPE=aggregate
    PARAMS_FLAG=--with-params
    SM_CRATE_PATH_FLAG=
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success
}

#### Using current repo path for `fluvio-smartmodule`

@test "Generate and test filter - (current repo fluvio-smartmodule / no params)" {
    LABEL=repo
    SMDK_SM_TYPE=filter
    PARAMS_FLAG=--no-params
    SM_CRATE_PATH_FLAG="--sm-crate-path $(pwd)/crates/fluvio-smartmodule"
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success

    # Test
    run $SMDK_BIN test --text 'a'
    assert_output --partial "1 records outputed"
    assert_success
}

@test "Generate and test map - (current repo fluvio-smartmodule / no params)" {
    LABEL=repo
    SMDK_SM_TYPE=map
    PARAMS_FLAG=--no-params
    SM_CRATE_PATH_FLAG="--sm-crate-path $(pwd)/crates/fluvio-smartmodule"
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success

    # Test
    run $SMDK_BIN test --text '2'
    assert_output --partial "1 records outputed"
    assert_output --partial "4"
    assert_success
}

@test "Generate and test array-map - (current repo fluvio-smartmodule / no params)" {
    LABEL=repo
    SMDK_SM_TYPE=array-map
    PARAMS_FLAG=--no-params
    SM_CRATE_PATH_FLAG="--sm-crate-path $(pwd)/crates/fluvio-smartmodule"
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success

    # Test
    run $SMDK_BIN test --text '["foo", "bar"]'
    assert_output --partial "2 records outputed"
    assert_output --partial "foo"
    assert_output --partial "bar"
    assert_success
}

@test "Generate and test filter-map - (current repo fluvio-smartmodule / no params)" {
    LABEL=repo
    SMDK_SM_TYPE=filter-map
    PARAMS_FLAG=--no-params
    SM_CRATE_PATH_FLAG="--sm-crate-path $(pwd)/crates/fluvio-smartmodule"
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success

    # Test
    run $SMDK_BIN test --text '2'
    assert_output --partial "1 records outputed"
    assert_output --partial "2"
    assert_success
}

@test "Generate and test aggregate - (current repo fluvio-smartmodule / no params)" {
    LABEL=repo
    SMDK_SM_TYPE=aggregate
    PARAMS_FLAG=--no-params
    SM_CRATE_PATH_FLAG="--sm-crate-path $(pwd)/crates/fluvio-smartmodule"
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success

    # Test
    run $SMDK_BIN test --text '2'
    assert_output --partial "1 records outputed"
    assert_output --partial "2"
    assert_success
}

### Using current repo path for `fluvio-smartmodule` with params

@test "Generate and test filter - (current repo fluvio-smartmodule / with params)" {
    LABEL=repo-params
    SMDK_SM_TYPE=filter
    PARAMS_FLAG=--with-params
    SM_CRATE_PATH_FLAG="--sm-crate-path $(pwd)/crates/fluvio-smartmodule"
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success

    # Test
    run $SMDK_BIN test --text 'a' -e key=value
    assert_output --partial "1 records outputed"
    assert_success
}

@test "Generate and build map - (current repo fluvio-smartmodule / with params)" {
    LABEL=repo-params
    SMDK_SM_TYPE=map
    PARAMS_FLAG=--with-params
    SM_CRATE_PATH_FLAG="--sm-crate-path $(pwd)/crates/fluvio-smartmodule"
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success
}

@test "Generate and build array-map - (current repo fluvio-smartmodule / with params)" {
    LABEL=repo-params
    SMDK_SM_TYPE=array-map
    PARAMS_FLAG=--with-params
    SM_CRATE_PATH_FLAG="--sm-crate-path $(pwd)/crates/fluvio-smartmodule"
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success
}

@test "Generate and build filter-map - (current repo fluvio-smartmodule / with params)" {
    LABEL=repo-params
    SMDK_SM_TYPE=filter-map
    PARAMS_FLAG=--with-params
    SM_CRATE_PATH_FLAG="--sm-crate-path $(pwd)/crates/fluvio-smartmodule"
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success
}

@test "Generate and build aggregate - (current repo fluvio-smartmodule / with params)" {
    LABEL=repo-params
    SMDK_SM_TYPE=aggregate
    PARAMS_FLAG=--with-params
    SM_CRATE_PATH_FLAG="--sm-crate-path $(pwd)/crates/fluvio-smartmodule"
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
        $SMDK_TEMPLATE_PATH_FLAG \
        $SM_CRATE_PATH_FLAG \
        $TESTING_GROUP_NAME_FLAG \
        --sm-type $SMDK_SM_TYPE \
        --sm-public $SMDK_SM_PUBLIC \
        --silent \
        $SM_PACKAGE_NAME
    assert_success

    # Build
    cd $SM_PACKAGE_NAME
    run $SMDK_BIN build
    refute_output --partial "could not compile"
    
    # Load
    run $SMDK_BIN load
    assert_output --partial "Creating SmartModule: $SM_PACKAGE_NAME"
    assert_success
}
