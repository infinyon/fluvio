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
        
        # Validate that dependencies with multiple locations defined have
        # a specific version pinned, to avoid failures at publish time (#2550)
        bad_dep=""
        deps_header=0
        while read -r line; do
            if [[ $deps_header -eq 0 ]] && \
               [[ $line =~ ^(\[((dependencies)|(.+\.dependencies))\])$ ]]; then
                # In a general dependencies section of Cargo.toml
                deps_header=1
            elif [[ $deps_header -eq 0 ]] && \
                 [[ $line =~ ^(\[dependencies\..*\])$ ]]; then
                # In a dedicated dependency section; version on future line
                deps_header=2
                bad_dep=$line
            elif [[ $deps_header -eq 1 ]] && [[ $line =~ ^(.*\{.*\}.*)$ ]] && \
                  [[ ! $line =~ ^.*workspace.*$ ]] && [[ ! $line =~ ^.*version.*$ ]]; then
                # Dependency has multiple locations defined but no version
                echo "❌ $crate has no version pinned for dependency: line $line"
                exit 1
            elif [[ $deps_header -eq 1 ]] && [[ $line =~ ^(.*\{.*)$ ]] && \
                 [[ ! $line =~ ^(.*version.*)$ ]]; then
                # Multi-line/dedicated dep without version in first line
                deps_header=2
                bad_dep=$line
            elif [[ $deps_header -eq 2 ]] && [[ $line =~ ^((.*\}.*)|())$ ]] && \
                 [[ ! $bad_dep =~ ^.*workspace.*$ ]] && [[ ! $line =~ ^(.*version.*)$ ]]; then
                # Multi-line/dedicated dep without version in any line
                echo "❌ $crate has no version pinned for dependency: bad $bad_dep"
                exit 1
            elif [[ $deps_header -eq 2 ]] && \
                 [[ $line =~ ^(.*version.*)$ ]]; then
                # Multi-line/location dep has a version defined in a later line
                deps_header=1
                bad_dep=""
            elif [[ $deps_header -eq 1 ]] && \
                 [[ $line =~ ^(\[.*\])$ || $line == "" ]]; then
                # Leaving a dependencies section
                deps_header=0
            fi
        done < Cargo.toml

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
