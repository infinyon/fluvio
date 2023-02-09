#!/bin/bash

#
# Upload a binary package to the hub
#
# Usage: upload-bpkg <binary> <opt:arch> <opt:channel>

# upload token needs to be provided in environment
bpkg_token=${BPKG_TOKEN}

set -eu

host='https://hub.infinyon.cloud'
binpath=$1
binfile=$(basename $1)
arch=${2:-'aarch64-apple-darwin'}
channel=${3:-'latest'}

url="${host}/hub/v0/bpkg-auth/${channel}/${arch}/${binfile}"

echo Upload file ${binpath}
echo Uploading to ${url}

http_status=$(curl -o /dev/null -s -w "%{response_code}" \
  ${url} \
  -X PUT \
  -H "Authorization: ${bpkg_token}" \
  -H 'Content-Type:' --data-binary "@${binpath}" \
  )
echo HTTP Response Status ${http_status}
if [ $http_status != "200" ]; then
	exit 1
fi
