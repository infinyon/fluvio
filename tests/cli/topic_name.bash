TOPIC_NAME_LEN=7

# Generate topic name once per test
if [ -z "$TOPIC_NAME" ]; then
    export TOPIC_NAME="`shuf -er -n$TOPIC_NAME_LEN  {A..Z} {a..z} {0..9} | tr -d '\0'`"
fi