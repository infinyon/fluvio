# Note: We should only generate strings w/ *lowercase* alphanum or '-'
function random_string() {
    DEFAULT_LEN=7
    STRING_LEN=${1:-$DEFAULT_LEN}

    RANDOM_STRING=$(shuf -zer -n"$STRING_LEN" {a..z} {0..9} "-")

    if [[ -n $DEBUG ]]; then
        echo "# DEBUG: Random str (len: $STRING_LEN): $RANDOM_STRING" >&3
    fi

    export RANDOM_STRING
    echo $RANDOM_STRING
}