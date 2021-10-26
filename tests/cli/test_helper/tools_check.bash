# Resolve path to `fluvio` binary instead of expecting it in PATH
# Search order: $FLUVIO_BIN, in PATH, current directory, home directory

function set_bin_path_then_exit() {
    #echo "# Fluvio binary path: $FLUVIO_BIN" >&3
    export FLUVIO_BIN
}

main() {
    if [[ -n $FLUVIO_BIN ]]; then
        #echo "# FLUVIO_BIN was defined" >&3
        FLUVIO_BIN="$FLUVIO_BIN"
        set_bin_path_then_exit;
    elif which fluvio; then
        #echo "# fluvio in PATH" >&3
        FLUVIO_BIN="$(which fluvio)"
        set_bin_path_then_exit;
    elif test -f "$(pwd)/fluvio"; then
        #echo "# fluvio in current directory" >&3
        FLUVIO_BIN="$(pwd)/fluvio"
        set_bin_path_then_exit;
    elif test -f "$HOME/.fluvio/bin/fluvio"; then
        #echo "# fluvio in home directory" >&3
        FLUVIO_BIN="$HOME/.fluvio/bin/fluvio"
        set_bin_path_then_exit;
    fi
}

main;