#!/usr/bin/env bats

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
    echo 'resolver = "2"'        >> Cargo.toml
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

# Call `$SMDK_BIN` via stdin.
# To be able to `run` something that uses pipe, we first need to create a function.
# see: https://bats-core.readthedocs.io/en/stable/tutorial.html#dealing-with-output
smdk_via_stdin() {
    echo -n $1 | $SMDK_BIN test --stdin ${@:2}
}

### Using crates.io dependency for `fluvio-smartmodule`

@test "Clean" {
    LABEL=clean
    SMDK_SM_TYPE=filter
    PARAMS_FLAG=--no-params
    SM_CRATE_PATH_FLAG=
    SM_PACKAGE_NAME=$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX
    SMDK_SM_PUBLIC=false
    
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
    

    # Verify if target exists in the parent folder
    [ -d "../target" ]
    
    # Clean
    run $SMDK_BIN clean
    assert_success

    # Verify if target was removed from the parent folder
    [ ! -d "../target" ]    
}

@test "Package" {
    LABEL=package
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

    # Package without existing package-meta
    run $SMDK_BIN publish --pack
    assert_success

    # Package with package-meta created before
    run $SMDK_BIN publish --pack
    assert_success
}

@test "Package with README" {
    LABEL=package
    SMDK_SM_TYPE=filter
    PARAMS_FLAG=--no-params
    SM_CRATE_PATH_FLAG=
    SM_PACKAGE_NAME="$LABEL-$SMDK_SM_TYPE-$PROJECT_NAME_PREFIX-readme-tests"
    SMDK_SM_PUBLIC=false

    # Add SM to workspace
    cd $TEST_DIR
    sed -i -e $'/members/a\\\n    "'$SM_PACKAGE_NAME'",' Cargo.toml

    # Generate
    run $SMDK_BIN generate \
        $PARAMS_FLAG \
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

    # Remove README from Template on Purpose
    rm "$TEST_DIR/$SM_PACKAGE_NAME/README.md"

    # Validates SmartModule Exists
    run $SMDK_BIN publish --pack
    assert_output --partial 'Error: README file not found at "./README.md"'
    assert_failure

    # Packages with specified README
    echo "# My SmartModule" > "$TEST_DIR/$SM_PACKAGE_NAME/README.md"
    run $SMDK_BIN publish --pack
    assert_success

}

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
    run $SMDK_BIN test --verbose --text 'a'
    assert_output --partial "1 records outputted"
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
    run $SMDK_BIN test --verbose --text '2'
    assert_output --partial "1 records outputted"
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

    # Test with verbose
    run $SMDK_BIN test --verbose --text '["foo", "bar"]'
    assert_output --partial "2 records outputted"
    assert_output --partial "foo"
    assert_output --partial "bar"
    assert_success

     # Test without verbose
    run $SMDK_BIN test  --text '["foo", "bar"]'
    refute_output --partial "2 records outputted"
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
    run $SMDK_BIN test --verbose --text '2'
    assert_output --partial "1 records outputted"
    assert_output --partial "1"
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
    run smdk_via_stdin '2' --verbose
    assert_output --partial "1 records outputted"
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
    run $SMDK_BIN test --verbose --text 'a' -e key=value
    assert_output --partial "1 records outputted"
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
    run $SMDK_BIN test --verbose --text 'a'
    assert_output --partial "1 records outputted"
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
    run $SMDK_BIN test  --verbose  --text '2'
    assert_output --partial "1 records outputted"
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

    # Test with verbose
    run smdk_via_stdin '["foo", "bar"]' --verbose
    assert_output --partial "2 records outputted"
    assert_output --partial "foo"
    assert_output --partial "bar"
    assert_success

    # Test with without verbose
    run smdk_via_stdin '["foo", "bar"]'
    refute_output --partial "2 records outputted"
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
    run $SMDK_BIN test --verbose --text '2'
    assert_output --partial "1 records outputted"
    assert_output --partial "1"
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
    run $SMDK_BIN test --verbose --text '2'
    assert_output --partial "1 records outputted"
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
    run smdk_via_stdin 'a' -e key=value --verbose
    assert_output --partial "1 records outputted"
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

@test "Test Lookback" {
    # Test with smartmodule example with Lookback
    cd "$(pwd)/smartmodule/examples/filter_look_back/"

    # Build
    run $SMDK_BIN build
    refute_output --partial "could not compile"

    # Test
    run $SMDK_BIN test --text '111' --lookback-last '1' --record '222' --record '333'
    refute_output --partial "111"
    assert_success

    run $SMDK_BIN test --text '444' --lookback-last '1' --record '222' --record '333'
    assert_output --partial "444"
    assert_success
}

@test "Test output with visible keys" {
    # Test with smartmodule example with Lookback
    cd "$(pwd)/smartmodule/examples/filter_look_back/"

    # Build
    run $SMDK_BIN build
    refute_output --partial "could not compile"

    # Test
    run $SMDK_BIN test --text '444' --lookback-last '1' --record '222' --record '333' --key-value
    assert_output --partial "[null]"
    assert_success

    run $SMDK_BIN test --text '444' --lookback-last '1' --record '222' --record '333' --key-value my-key
    assert_output --partial "[my-key]"
    assert_success
}

@test "Test using SmartModuleRecord on fluvio-smartmodule-map-with-timestamp" {
    # Test with smartmodule example with timestamp
    cd "$(pwd)/smartmodule/examples/map_with_timestamp/"

    # Set Date Variables
    DATE_NOW_YEAR="$(date --utc +%Y)"
    DATE_NOW_MONTH="$(date --utc +%m)"
    DATE_NOW_DAY="$(date --utc +%d)"

    # Build
    run $SMDK_BIN build
    refute_output --partial "could not compile"

    # Test
    run $SMDK_BIN test --text 'foo'
    assert_output --partial "0:$DATE_NOW_YEAR-$DATE_NOW_MONTH-$DATE_NOW_DAY"
    assert_success
}

@test "Test using SmartModuleRecord on fluvio-smartmodule-aggregate-with-timestamp" {
    # Test with smartmodule example with timestamp
    cd "$(pwd)/smartmodule/examples/aggregate_with_timestamp/"

    # Set Date Variables
    DATE_NOW_YEAR="$(date --utc +%Y)"
    DATE_NOW_MONTH="$(date --utc +%m)"
    DATE_NOW_DAY="$(date --utc +%d)"

    # Build
    run $SMDK_BIN build
    refute_output --partial "could not compile"

    # Test
    run $SMDK_BIN test --text 'foo'
    assert_output --partial "foo_[$DATE_NOW_YEAR-$DATE_NOW_MONTH-$DATE_NOW_DAY"
    assert_success
}

@test "Test using SmartModuleRecord on fluvio-filter-look-back-with-timestamps" {
    # Test with smartmodule example with Lookback
    cd "$(pwd)/smartmodule/examples/filter_look_back_with_timestamps/"

    # Build
    run $SMDK_BIN build
    refute_output --partial "could not compile"

    # Test
    run $SMDK_BIN test --text '111' --lookback-last '1' --record '222' --record '333'
    refute_output --partial "111"
    assert_success

    run $SMDK_BIN test --text '444' --lookback-last '1' --record '222' --record '333'
    assert_output --partial "444"
    assert_success
}

@test "Test using SmartModuleRecord on fluvio-array-map-json-array-with-timestamp" {
    # Test with smartmodule example with Array Map with Timestamp
    cd "$(pwd)/smartmodule/examples/array_map_json_array_with_timestamp/"

    # Set Date Variables
    DATE_NOW_YEAR="$(date --utc +%Y)"
    DATE_NOW_MONTH="$(date --utc +%m)"
    DATE_NOW_DAY="$(date --utc +%d)"

    # Build
    run $SMDK_BIN build
    refute_output --partial "could not compile"

    # Test
    run $SMDK_BIN test --verbose --text '["Apple", "Banana", "Cranberry"]'
    assert_output --partial "3 records outputted"
    assert_output --partial "\"Apple\"_$DATE_NOW_YEAR-$DATE_NOW_MONTH-$DATE_NOW_DAY"
    assert_output --partial "\"Banana\"_$DATE_NOW_YEAR-$DATE_NOW_MONTH-$DATE_NOW_DAY"
    assert_output --partial "\"Cranberry\"_$DATE_NOW_YEAR-$DATE_NOW_MONTH-$DATE_NOW_DAY"
    assert_success
}
