#!/usr/bin/env bash

set -eu

# Read in PUBLISH_CRATES var
source $(dirname -- ${BASH_SOURCE[0]})/publish-list

CRATES_CHECKED=0
readonly VERBOSE=${VERBOSE:-false}

function cargo_publish_dry_run_all() {

    for crate in "${PUBLISH_CRATES[@]}" ; do
        echo "$crate";
        pushd crates/"$crate";

        # Using `cargo check` instead of `cargo publish --dry-run`
        # because dry-run will only utilize published dependencies.
        # So unpublished dependencies will cause test fail
        cargo check
        result="$?";

        # cargo publish exit codes:
        if [[ "$result" != 0 ]];
        then
            echo "❌ $crate check failed"
            exit 1
        else
            CRATES_CHECKED=$((CRATES_CHECKED+1));
        fi

        popd
    done

}

function main() {

    if [[ $VERBOSE != false ]];
    then
        echo "VERBOSE MODE ON"
        set -x
    fi

    cargo_publish_dry_run_all;
    
    echo "✅ # $CRATES_CHECKED crates checked"
}

main;