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
# We don't check for minimum length, but $DEFAULT_LEN >= 3
# or `shuf` will err when requested $BODY length less than zero
function random_string() {
    DEFAULT_LEN=7
    STRING_LEN=${1:-$DEFAULT_LEN}

    # Ensure we don't end up with a string that starts or ends with '-'
    # https://github.com/infinyon/fluvio/issues/1864

    HEAD=$(shuf -zer -n1 {a..z} {0..9})
    BODY=$(shuf -zer -n"$(($STRING_LEN-2))" {a..z} {0..9} "-")
    FOOT=$(shuf -zer -n1 {a..z} {0..9})

    RANDOM_STRING=$HEAD$BODY$FOOT

    if [[ -n $DEBUG ]]; then
        echo "# DEBUG: Random str (len: $STRING_LEN): $RANDOM_STRING" >&3
    fi

    export RANDOM_STRING
    echo "$RANDOM_STRING"
}