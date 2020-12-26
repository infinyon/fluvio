#/bin/bash
# generate large data size
set -e
fluvio topic create t1
fluvio produce t1 -r $(ls /var/log/journal/**/*.journal)