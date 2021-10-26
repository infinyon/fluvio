# This should always be loaded after tools_check.bash

if [[ -n $DEBUG ]]; then
    echo "# DEBUG: Attempting to start cluster with fluvio bin @ $FLUVIO_BIN" >&3
fi
run "$FLUVIO_BIN" cluster start