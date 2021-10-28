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