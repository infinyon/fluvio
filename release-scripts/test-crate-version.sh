#!/usr/bin/env bash

# Check all the crates we list in `publish-list`
# Test whether we should bump the version number
#
# Based on:
# - If source code has changed
# - If Cargo.toml has changed
#
# Supports new crates that haven't been initially published

set -eu

# Read in PUBLISH_CRATES var
# shellcheck source=/dev/null
source "$(dirname -- "${BASH_SOURCE[0]}")/publish-list"

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

# Check if we have toml2json in path
# If not, attempt to download it
function toml2json_check() {
    if which toml2json;
    then
        echo "🔧 toml2json found"
    else
        echo "toml2json not found"
        echo "Attempting to download"
        cargo install toml2json 
    fi
}

# Check if we have xsv in path
# If not, attempt to download it
function xsv_check() {
    if which xsv;
    then
        echo "🔧 xsv found"
    else
        echo "xsv not found"
        echo "Attempting to download"
        cargo install xsv 
    fi
}

# in the crates db snapshot list of published crates
function check_if_crate_published() {

    # Requires: curl, xsv (crate binary)
    CRATE_NAME=$1

    if xsv select name ./crates_io/db_snapshot/*/data/crates.csv | grep -E "^$CRATE_NAME$" > /dev/null;
    then
        return 0
    else
        return 1
    fi
}

function download_crates_io_data() {

    mkdir -p ./crates_io/db_snapshot;
    pushd ./crates_io/db_snapshot >/dev/null
    # Download crates.io (daily) data locally
    # From: https://crates.io/data-access
    # https://static.crates.io/db-dump.tar.gz

    # Extract
    wget --quiet https://static.crates.io/db-dump.tar.gz
    tar xzf db-dump.tar.gz
    rm db-dump.tar.gz

    popd >/dev/null
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
        DIFF_FLAGS="-bur"
    else
        # Don't print the diff
        DIFF_FLAGS="-burq"
    fi

    diff "$DIFF_FLAGS" ./crates/"$CRATE_NAME"/src ./crates_io/"$CRATE_NAME"/src;
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

# We want to catch Cargo.toml-only changes, like dependency updates
# and ensure that a version bump was also included in the update
function compare_crates_content() {
    CRATE_NAME=$1

    # Set the `diff` verbosity 
    if [[ $VERBOSE == true ]];
    then
        DIFF_FLAGS=""
    else
        DIFF_FLAGS="-q"
    fi

    if diff "$DIFF_FLAGS" ./crates/"$CRATE_NAME"/Cargo.toml ./crates_io/"$CRATE_NAME"/Cargo.toml.orig;
    then
        return 0
    else
        # Changes made to Cargo.toml. Let's identify where they are

        REPO_CARGO_JSON=$(mktemp)
        CRATESIO_CARGO_JSON=$(mktemp)

        CHANGE_FOUND=false

        toml2json ./crates/"$CRATE_NAME"/Cargo.toml | jq > "$REPO_CARGO_JSON"
        toml2json ./crates_io/"$CRATE_NAME"/Cargo.toml.orig | jq > "$CRATESIO_CARGO_JSON"

        # TODO: How to handle edgecase if new keys only exist in crates.io, bc we only 🚩 from the repo keys
        # We need to know if dependencies have been updated
        # Can we enumerate the top-level keys and compareo?q
        for cargo_keys in $(jq -r 'keys | @sh' "$REPO_CARGO_JSON" | xargs echo)
        do

            # Write repo value to temp file
            REPO_JSON_KV=$(mktemp)
            # Write crates_io value to temp file
            CRATESIO_JSON_KV=$(mktemp)

            # Compare sections and report to user

            #echo $cargo_keys
            jq ".[\"${cargo_keys}\"]" "$REPO_CARGO_JSON" > "$REPO_JSON_KV"
            jq ".[\"${cargo_keys}\"]" "$CRATESIO_CARGO_JSON" > "$CRATESIO_JSON_KV"

            if diff -q "$REPO_JSON_KV" "$CRATESIO_JSON_KV" >/dev/null;
            then
                :
                #echo "No changes in toml section: $cargo_keys"
            else
                # We raise a red flag, but this is only a problem is the version wasn't updated too
                echo "🚩 Changes FOUND in toml section: $cargo_keys"

                # Print the diff
                diff "$REPO_JSON_KV" "$CRATESIO_JSON_KV"
                
                CHANGE_FOUND=true

            fi

            # Cleanup
            rm -f "$REPO_JSON_KV" "$CRATESIO_JSON_KV"

        done

        # Cleanup
        rm -f "$REPO_CARGO_JSON" "$CRATESIO_CARGO_JSON"

        if [[ "$CHANGE_FOUND" == false ]];
        then
            echo "🚩 Changes were not found but they were expected 🚩"
            echo "🚩 Possible cause: top-level keys removed from repo Cargo.toml 🚩"
        fi

        return 1
    fi
}

# ✅ If src + version + Cargo.toml have no changes (This is the most common success case)
# ✅ If src + version both have changes
# ✅ If src has no changes but Cargo.toml + version both have changes
# ❌ If src has changes but version not updated
# ❌ If Cargo.toml has changes but version not updated
function check_crate() {
    SRC_MATCH=$1
    VERSION_MATCH=$2
    CARGO_TOML_MATCH=$3
    CRATE_NAME=$4

    # No changes between repo and crates.io
    if [[ "$SRC_MATCH" == true && "$VERSION_MATCH" == true && "$CARGO_TOML_MATCH" == true ]];
    then
        echo "🟢 Repo code does not differ from crates.io"
    fi

    # No code changes found. Cargo.toml updated and the versions are different
    if [[ "$SRC_MATCH" == true && "$VERSION_MATCH" == false && "$CARGO_TOML_MATCH" == false ]];
    then
        echo "🟢 Repo code has NOT changed. Cargo.toml changes found but version has been updated too."
    fi

    # Code changes found, but versions don't match
    # It's assumed that the repo is bumped appropriately
    if [[ "$SRC_MATCH" == false && "$VERSION_MATCH" == false ]];
    then
        echo "🟢 Repo code differs, but version has been updated too"
    fi

    # Code changes found, however versions match when it should be updated
    if [[ "$SRC_MATCH" == false && "$VERSION_MATCH" == true ]];
    then
        echo "⛔ Repo code has changed but version needs to be bumped"
        CHECK_CRATES+=("$CRATE_NAME")
        ALL_CRATE_CHECK_PASS=false
    fi

    # No code changes found. Cargo.toml changed found but the version wasn't updated
    if [[ "$VERSION_MATCH" == true && "$CARGO_TOML_MATCH" == false ]];
    then
        echo "⛔ Cargo.toml changes found but version needs to be bumped."
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

    # Tools check
    cargo_download_check;
    toml2json_check;
    xsv_check;

    # Cleanup previous runs
    rm -rf ./crates_io
    mkdir -p ./crates_io;

    # Download crates.io db
    download_crates_io_data;

    echo "Order of crates:"
    printf "* %s\n" "${PUBLISH_CRATES[@]}"

    for crate in "${PUBLISH_CRATES[@]}" ; do
        echo
        echo "================"
        echo "Checking: $crate"

        SRC_MATCH=false
        VERSION_MATCH=false
        CARGO_TOML_MATCH=false

        # Check if crate exists first
        if check_if_crate_published "$crate";
        then
            # if it does exist

            download_crate "$crate";

            if compare_crates_src "$crate";
            then
                SRC_MATCH=true
            fi


            if compare_crates_version "$crate";
            then
                VERSION_MATCH=true
            fi

            # We only need to compare Cargo.toml contents if there's a chance for Cargo.toml only changes 
            if [[ "$SRC_MATCH" == true ]] && [[ "$VERSION_MATCH" == true ]];
            then
                if compare_crates_content "$crate";
                then
                    CARGO_TOML_MATCH=true
                fi
            fi

            check_crate "$SRC_MATCH" "$VERSION_MATCH" "$CARGO_TOML_MATCH" "$crate";
        else
            # if it does not exist, 
            echo "🟡 Crate not found in crates.io (Possible cause: Not published yet?)"
        fi
    done

    echo
    echo "Results:"
    if [[ $ALL_CRATE_CHECK_PASS == true ]];
    then
        echo "✅ All crates appear to be ready for publishing"
        echo
        return 0
    else
        echo "❌ The following crates require attention:"
        printf '* %s\n' "${CHECK_CRATES[@]}" | sort -u
        echo
        return 1
    fi
}

main;