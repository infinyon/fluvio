# TODO: Use FLUVIO_BIN env var instead of expecting `fluvio` in PATH.
# Search order: $FLUVIO_BIN, $PATH, $(pwd)/fluvio, $HOME/.fluvio/bin/fluvio

setup() {
    FLUVIO_BIN=$(which fluvio)
    export FLUVIO_BIN
    # Print out `fluvio` path used in test
    echo "# Fluvio binary path: $FLUVIO_BIN" >&3
}