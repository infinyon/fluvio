# This should always be loaded after tools_check.bash

setup_file() {
    run "$FLUVIO_BIN" cluster start
}