#!/usr/bin/env bash

set -u

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
    #fluvio
    fluvio-stream-dispatcher
    fluvio-package-index
    fluvio-extension-common
)

ALL_CRATE_CHECK_PASS=true
CHECK_CRATES=()
readonly VERBOSE=${VERBOSE:-false}

# Check if we have cargo-download in path
# Attempt to download it
function cargo_download_check() {
    :
}

function download_crate() {
    CRATE_NAME=$1
    mkdir -p ./crates_io/$CRATE_NAME

    if [[ $VERBOSE == true ]];
    then
        cargo download -x  $CRATE_NAME -o ./crates_io/$CRATE_NAME --verbose
    else
        cargo download -x  $CRATE_NAME -o ./crates_io/$CRATE_NAME --quiet
    fi
    
}


function compare_crates_src() {
    CRATE_NAME=$1

    if [[ $VERBOSE == true ]];
    then
        diff -bur ./crates/$CRATE_NAME/src ./crates_io/$CRATE_NAME/src;
    else
        diff -burq ./crates/$CRATE_NAME/src ./crates_io/$CRATE_NAME/src;
    fi
    
}

function compare_crates_version() {
    CRATE_NAME=$1

    CRATESIO_VERSION=$(cat ./crates_io/$CRATE_NAME/Cargo.toml | grep -e "^version" | head -1)
    REPO_VERSION=$(cat ./crates/$CRATE_NAME/Cargo.toml | grep -e "^version" | head -1)

    if [[ $CRATESIO_VERSION == $REPO_VERSION ]];
    then
        return 0
    else
        return 1
    fi
}

# ✅ If src matches and version matches (This is the most common success case)
# ✅ If src does not match and version does not match
# ❌ If src matches and version does not match (This is highly unlikely)
# ❌ If src does not match and version does match (This is the most common fail case)
function does_repo_crate_version_need_bump() {
    SRC_MATCH=$1
    VERSION_MATCH=$2
    CRATE_NAME=$3

    if [[ "$SRC_MATCH" == true && "$VERSION_MATCH" == true ]];
    then
        echo "✅ Crate has not changed"
    fi

    if [[ "$SRC_MATCH" == false && "$VERSION_MATCH" == false ]];
    then
        echo "✅ Crate has already been bumped"
    fi

    if [[ "$SRC_MATCH" == false && "$VERSION_MATCH" == true ]];
    then
        echo "❌ Code has changed but version needs to be bumped"
        CHECK_CRATES+=("$CRATE_NAME")
        ALL_CRATE_CHECK_PASS=false
    fi

    if [[ "$SRC_MATCH" == true && "$VERSION_MATCH" == false ]];
    then
        echo "❌ Code has NOT changed, but versions don't match. Something is weird."
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

        download_crate $crate;


        compare_crates_src $crate;
        if [[ "$?" == 0 ]];
        then
            #echo "Crate src match"
            SRC_MATCH=true
        #else
        #    #echo "Crate src DOES NOT match"
        fi 
        
        compare_crates_version $crate;
        if [[ "$?" == 0 ]];
        then
            #echo "Crate version match"
            VERSION_MATCH=true
        #else
        #    #echo "Crate version DOES NOT match"
        fi 

        does_repo_crate_version_need_bump $SRC_MATCH $VERSION_MATCH $crate;
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