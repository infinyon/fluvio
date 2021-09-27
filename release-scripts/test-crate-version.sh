#!/usr/bin/env bash

set -eu

PUBLISH_CRATES=(
    fluvio-protocol-core
    fluvio-smartstream-derive
    fluvio-types
    fluvio-protocol-derive
    fluvio-protocol-codec
    fluvio-protocol
    fluvio-dataplane-protocol
    fluvio-socket
    fluvio-stream-model
    fluvio-controlplane-metadata
    fluvio-spu-schema
    fluvio-sc-schema
    fluvio-smartstream
    fluvio
    fluvio-stream-dispatcher
    fluvio-package-index
    fluvio-extension-common
)

ALL_CRATE_CHECK_PASS=true
CHECK_CRATES=()
readonly VERBOSE=${VERBOSE:-false}

# Check if we have cargo-download in path
# If not, attempt to download it
function cargo_download_check() {
    if which cargo-download;
    then
        echo "🔧 cargo-download found"
    else
        echo "cargo-download not found"
        echo "Attempting to download"
        cargo install cargo-download
    fi
}

function download_crate() {
    CRATE_NAME=$1
    mkdir -p ./crates_io/"$CRATE_NAME"

    if [[ $VERBOSE == true ]];
    then
        cargo download -x "$CRATE_NAME" -o ./crates_io/"$CRATE_NAME" --verbose
    else
        cargo download -x "$CRATE_NAME" -o ./crates_io/"$CRATE_NAME" --quiet
    fi
    
}


function compare_crates_src() {
    CRATE_NAME=$1

    if [[ $VERBOSE == true ]];
    then
        diff -bur ./crates/"$CRATE_NAME"/src ./crates_io/"$CRATE_NAME"/src;
    else
        # Don't print the diff
        diff -burq ./crates/"$CRATE_NAME"/src ./crates_io/"$CRATE_NAME"/src;
    fi
    
}

function compare_crates_version() {
    CRATE_NAME=$1

    CRATESIO_VERSION=$(grep -e "^version" ./crates_io/"$CRATE_NAME"/Cargo.toml | head -1)
    REPO_VERSION=$(grep -e "^version" ./crates/"$CRATE_NAME"/Cargo.toml | head -1)

    if [[ "$CRATESIO_VERSION" == "$REPO_VERSION" ]];
    then
        return 0
    else
        return 1
    fi
}

function compare_crates_content() {
    CRATE_NAME=$1

     if [[ $VERBOSE == true ]];
    then
        diff ./crates/"$CRATE_NAME"/Cargo.toml ./crates_io/"$CRATE_NAME"/Cargo.toml.orig;
    else
        # Don't print the diff
        diff -q ./crates/"$CRATE_NAME"/Cargo.toml ./crates_io/"$CRATE_NAME"/Cargo.toml.orig;
    fi
}



# ✅ If src matches and version matches (This is the most common success case)
# ✅ If src does not match and version does not match
# ❌ If src matches and version does not match (This is highly unlikely)
# ❌ If src does not match and version does match (This is the most common fail case)
# TODO: Add Cargo.toml comparisons
function check_crate() {
    SRC_MATCH=$1
    VERSION_MATCH=$2
    CARGO_TOML_MATCH=$3
    CRATE_NAME=$4

    # No changes between repo and crates.io
    if [[ "$SRC_MATCH" == true && "$VERSION_MATCH" == true ]];
    then
        echo "🟢 Repo code does not differ from crates.io"

        # TODO: Need a Cargo.toml comparison here
        # Crates.io does some re-writing of the Cargo.toml, so `diff` unreliable
    fi

    # Code changes found, but versions don't match
    # It's assumed that the repo is bumped appropriately
    if [[ "$SRC_MATCH" == false && "$VERSION_MATCH" == false ]];
    then
        echo "🟢 Repo code differs, but version has been updated too"
    fi

    # Code changes found, however versions match
    # It's assumed that the repo has increased version number
    if [[ "$SRC_MATCH" == false && "$VERSION_MATCH" == true ]];
    then
        echo "⛔ Repo code has changed but version needs to be bumped"
        CHECK_CRATES+=("$CRATE_NAME")
        ALL_CRATE_CHECK_PASS=false
    fi

    # No code changes found, but the versions are different
    # This is a weak test w/o Cargo.toml comparisons
    if [[ "$SRC_MATCH" == true && "$VERSION_MATCH" == false ]];
    then
        echo "🔴 Repo code has NOT changed, but versions don't match. Cargo.toml only changes?"
        CHECK_CRATES+=("$CRATE_NAME")
        ALL_CRATE_CHECK_PASS=false
    fi
}

function main() {

    if [[ $VERBOSE != false ]];
    then
        echo "VERBOSE MODE ON"
        set -x
    fi
    
    cargo_download_check;

    rm -rf ./crates_io
    mkdir -p ./crates_io;

    for crate in "${PUBLISH_CRATES[@]}" ; do
        echo "Checking: $crate"

        SRC_MATCH=false        
        VERSION_MATCH=false
        CARGO_TOML_MATCH=false

        download_crate "$crate";

        if compare_crates_src "$crate";
        then
            SRC_MATCH=true
        fi 
        
        
        if compare_crates_version "$crate";
        then
            VERSION_MATCH=true
        fi 

        # TODO: Add Cargo.toml compare
        #if compare_crates_content "$crate";
        #then
        #    CARGO_TOML_MATCH=true
        #fi 

        check_crate "$SRC_MATCH" "$VERSION_MATCH" "$CARGO_TOML_MATCH" "$crate";
    done

    echo
    echo "Results:"
    if [[ $ALL_CRATE_CHECK_PASS == true ]];
    then
        echo "✅ All crates appear to be ready for publishing"
        return 0
    else
        echo "❌ The following crates require attention:"
        printf '* %s\n' "${CHECK_CRATES[@]}"
        return 1
    fi
}

main;