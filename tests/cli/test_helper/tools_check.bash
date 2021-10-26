# Resolve path to `fluvio` binary instead of expecting it in PATH
# Search order: $FLUVIO_BIN, in PATH, current directory, home directory
main() {
    if [[ -n $FLUVIO_BIN ]]; then
        if [[ -n $DEBUG ]]; then
            echo "# DEBUG: found: FLUVIO_BIN was defined" >&3
        fi
        set_bin_path_then_exit "$FLUVIO_BIN";
    elif which fluvio; then
        if [[ -n $DEBUG ]]; then
            echo "# DEBUG: found: fluvio in PATH" >&3
        fi
        set_bin_path_then_exit "$(which fluvio)";
    elif test -f "$(pwd)/fluvio"; then
        if [[ -n $DEBUG ]]; then
            echo "# DEBUG: found: fluvio in current directory" >&3
        fi
        set_bin_path_then_exit "$(pwd)/fluvio";
    elif test -f "$HOME/.fluvio/bin/fluvio"; then
        if [[ -n $DEBUG ]]; then
            echo "# DEBUG: found: fluvio in home directory" >&3
        fi
        set_bin_path_then_exit "$HOME/.fluvio/bin/fluvio";
    fi
}

function set_bin_path_then_exit() {
    FLUVIO_BIN=$1
    export FLUVIO_BIN
    if [[ -n $DEBUG ]]; then
        echo "# DEBUG: Fluvio binary path: $FLUVIO_BIN" >&3
    fi

}

main;