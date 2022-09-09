#!/usr/bin/env bash

set -eu

# Read in PUBLISH_CRATES var
source $(dirname -- ${BASH_SOURCE[0]})/publish-list

LOOP_AGAIN=false
ATTEMPTS=1
CRATES_UPLOADED=0
readonly MAX_ATTEMPTS=3
readonly VERBOSE=${VERBOSE:-false}

readonly CARGO_OUTPUT_TMP=$(mktemp)

function check_if_crate_uploaded() {

    local CRATE=$1;

    # Check for whether the crate was already uploaded to determine if we're good to move forward
    tail -1 "$CARGO_OUTPUT_TMP" | grep "already uploaded" > /dev/null

    # If exit code from `grep` is 0
    if [[ ${PIPESTATUS[1]} != 0 ]];
    then
        echo "$CRATE upload check failed. Will try again"
        LOOP_AGAIN=true;
    fi
}


# We're relying on CARGO_REGISTRY_TOKEN to be set in CI
function cargo_publish() {
    cargo publish 2>&1 | tee "$CARGO_OUTPUT_TMP"

    return "${PIPESTATUS[0]}"

}

function cargo_publish_all() {
    # We want to loop though each of the crates and attempt to publish.

    if [[ $ATTEMPTS -lt $((MAX_ATTEMPTS + 1)) ]];
    then
        for crate in "${PUBLISH_CRATES[@]}" ; do
            echo "$crate";
            pushd crates/"$crate";

            # Save the `cargo publish` in case we get a non-zero exit
            cargo_publish;
            result="$?";

            #echo "PUBLISH STEP HERE"
            #(exit 101)

            # cargo publish exit codes:
            if [[ "$result" != 0 ]];
            then
                check_if_crate_uploaded "$crate";
            else
                CRATES_UPLOADED=$((CRATES_UPLOADED+1));
            fi

            popd
        done
    else
        echo "❌ Max number of publish attempts reached"
        echo "❌ Max attempts: $MAX_ATTEMPTS"
        exit 1
    fi
}

function main() {

    if [[ $VERBOSE != false ]];
    then
        echo "VERBOSE MODE ON"
        set -x
    fi

    while cargo_publish_all; [[ $LOOP_AGAIN = true ]];
    do
        # Reset the loop flag
        LOOP_AGAIN=false;
        # Increment the attempts counter
        ATTEMPTS=$((ATTEMPTS+1));
    done

    echo "✅ Publish success after # Attempts: $ATTEMPTS"
    echo "✅ Crates uploaded: $CRATES_UPLOADED"
}

main;