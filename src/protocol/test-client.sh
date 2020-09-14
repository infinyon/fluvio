# /bin/bash
# send out simple message
msg="$1"
echo "$msg" | nc -w 1 localhost 9092