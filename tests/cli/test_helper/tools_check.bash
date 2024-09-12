# Resolve path to `fluvio` binary instead of expecting it in PATH
# Search order: $FLUVIO_BIN, in PATH, current directory, home directory
main() {
    # Take in override to test_helper directory
    TEST_HELPER_DIR=${TEST_HELPER_DIR:-./test_helper}
    export TEST_HELPER_DIR

    # BATS_TEST_RETRIES is set to default after bats started therefore we set it here
    BATS_TEST_RETRIES=${CLI_TEST_RETRIES:-0}
    export BATS_TEST_RETRIES

    check_load_bats_libraries;
    check_fluvio_bin_path;
    check_timeout_bin;
}

function check_fluvio_bin_path() {

    if [[ -n $FLUVIO_BIN ]]; then
        if [[ -n $DEBUG ]]; then
            echo "# DEBUG: found: FLUVIO_BIN was defined"
        fi
        _set_fluvio_bin_path_then_exit "$FLUVIO_BIN";
    elif which fluvio; then
        if [[ -n $DEBUG ]]; then
            echo "# DEBUG: found: fluvio in PATH"
        fi
        _set_fluvio_bin_path_then_exit "$(which fluvio)";
    elif test -f "$(pwd)/fluvio"; then
        if [[ -n $DEBUG ]]; then
            echo "# DEBUG: found: fluvio in current directory"
        fi
        _set_fluvio_bin_path_then_exit "$(pwd)/fluvio";
    elif test -f "$HOME/.fluvio/bin/fluvio"; then
        if [[ -n $DEBUG ]]; then
            echo "# DEBUG: found: fluvio in home directory"
        fi
        _set_fluvio_bin_path_then_exit "$HOME/.fluvio/bin/fluvio";
    fi
}

function _set_fluvio_bin_path_then_exit() {
    FLUVIO_BIN=$1
    export FLUVIO_BIN
    if [[ -n $DEBUG ]]; then
        echo "# DEBUG: Fluvio binary path: $FLUVIO_BIN"
    fi

}

function check_fluvio_cluster() {
    if [[ -n $DEBUG ]]; then
        echo "# DEBUG: Attempting to start cluster with fluvio bin @ $FLUVIO_BIN" >&3
    fi
    run "$FLUVIO_BIN" cluster start
}

# Make sure Bats-core helper libraries are installed
function check_load_bats_libraries() {
    # Look for bats-support, bats-assert, bats-file
    # If not there, try to clone it into place

    if ! test -d "$TEST_HELPER_DIR/bats-support"; then
        echo "# Installing bats-support in $TEST_HELPER_DIR"
        git clone https://github.com/bats-core/bats-support "$TEST_HELPER_DIR/bats-support"
    fi

    if ! test -d "$TEST_HELPER_DIR/bats-assert"; then
        echo "# Installing bats-assert in $TEST_HELPER_DIR"
        git clone https://github.com/bats-core/bats-assert "$TEST_HELPER_DIR/bats-assert"
    fi
}

function check_timeout_bin() {
    if ! which timeout; then
        echo "# \`timeout\` not in PATH" >&3

        if [[ $(uname) == "Darwin" ]]; then
            echo "# run \`brew install coreutils\` to install"
        fi

        false
    fi
}

function wait_for_line_in_file() {
    LINE="$1"
    FILE="$2"
    MAX_SECONDS="${3:-30}" # 30 seconds default value

    echo "Waiting for file $FILE containing $LINE"

    ELAPSED=0;
    until grep -q "$LINE" "$FILE"
    do
      sleep 1
      let ELAPSED=$ELAPSED+1
      if [[ $ELAPSED -ge MAX_SECONDS ]]
      then
        echo "timeout $MAX_SECONDS seconds elapsed"
        exit 1
      fi
    done
    echo "Done waiting for file $FILE containing $LINE"
}



main;
