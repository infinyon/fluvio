# Use the bats `-t` flag with DEBUG env var set (with a value)
#
# Example shell usage:
#
# `DEBUG=true bats -t *.bats`
#
function debug_msg() {
    MESSAGE="$*"
    MESSAGE_LEN="$(echo "$@" | wc | awk '{print $3}')"
    MESSAGE_B64="$(echo "$@" | base64)"

    if [[ -n $DEBUG ]]; then
        echo "# DEBUG: $MESSAGE" >&3
        echo "# DEBUG: len: $MESSAGE_LEN | base64: $MESSAGE_B64" >&3
    fi
}

# Note: We only generate strings w/ *lowercase* alphanum or '-'
# for topic naming compatibility
# We don't check for minimum length, but $DEFAULT_LEN min is 3.
# If less than 3 `shuf` will err when requested $BODY length less than zero
#
# Give a number as arg to control the length of the string generated
function random_string() {
    DEFAULT_LEN=7
    STRING_LEN=${1:-$DEFAULT_LEN}

    # Ensure we don't end up with a string that starts or ends with '-'
    # https://github.com/infinyon/fluvio/issues/1864

    HEAD=$(shuf -zer -n1 {a..z} {0..9} | tr -d '\0')
    BODY=$(shuf -zer -n"$(($STRING_LEN - 2))" {a..z} {0..9} "-" | tr -d '\0')
    FOOT=$(shuf -zer -n1 {a..z} {0..9} | tr -d '\0')

    RANDOM_STRING=$HEAD$BODY$FOOT

    if [[ -n $DEBUG ]]; then
        echo "# DEBUG: Random str (len: $STRING_LEN): $RANDOM_STRING" >&3
    fi

    export RANDOM_STRING
    echo "$RANDOM_STRING"
}

# 1. Uses the public installer script to install the `fluvio` cli,
# 2. Prints out version info
# 3. Starts a cluster
#
# Default version installed is `latest`.
#
# Give version as arg to control what version you download and install
function setup_fluvio_cluster() {
    CLUSTER_VERSION=${1:-latest}

    echo "# Installing cluster @ VERSION: $CLUSTER_VERSION" >&3
    $FLUVIO_BIN version >&3
    curl -fsS https://packages.fluvio.io/v1/install.sh | VERSION=$CLUSTER_VERSION bash
    echo "# Starting cluster @ VERSION: $CLUSTER_VERSION" >&3

    if [ "$CLUSTER_VERSION" = "latest" ]; then
        $FLUVIO_BIN cluster start --image-version latest
    else
        $FLUVIO_BIN cluster start
    fi
}

# 1. Uses the public installer script to install the `fluvio` cli
# 2. Prints version info
#
# Default version installed is `latest`.
#
# Give version as arg to control what version you download and install
function setup_fluvio_cli() {
    CLI_VERSION=${1:-latest}
    echo "Installing CLI @ VERSION: $CLI_VERSION" >&3
    curl -fsS https://packages.fluvio.io/v1/install.sh | VERSION=$CLI_VERSION bash
    $FLUVIO_BIN version >&3
}
