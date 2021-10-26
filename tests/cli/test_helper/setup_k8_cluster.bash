# TODO: Use FLUVIO_BIN env var instead of expecting `fluvio` in PATH.
# Search order: $FLUVIO_BIN, $PATH, $(pwd)/fluvio, $HOME/.fluvio/bin/fluvio

setup_file() {
    run fluvio cluster start
}