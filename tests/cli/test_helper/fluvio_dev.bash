# Use the bats `-t` flag with DEBUG env var set (with a value)
#
# Example shell usage:
#
# `DEBUG=true bats -t *.bats`
#
function debug_msg() {
    MESSAGE="$@"

    if [[ -n $DEBUG ]]; then
        echo "# DEBUG: $MESSAGE" >&3
    fi
}