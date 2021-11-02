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
function random_string() {
    DEFAULT_LEN=7
    STRING_LEN=${1:-$DEFAULT_LEN}

    RANDOM_STRING=$(shuf -zer -n"$STRING_LEN" {a..z} {0..9} "-")

    if [[ -n $DEBUG ]]; then
        echo "# DEBUG: Random str (len: $STRING_LEN): $RANDOM_STRING" >&3
    fi

    export RANDOM_STRING
    echo "$RANDOM_STRING"
}