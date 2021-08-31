# /bin/bash
# send out simple message
msg="$1"
port=${2:-9092}
xxd -r -p $msg - | nc -w 1 localhost $port |  xxd -p -c 4 
