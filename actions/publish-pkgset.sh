#!/usr/bin/env bash
set -u

echo Fluvio version: $FLUVIO_VERSION
echo Fluvio Cloud Version: $FLUVIO_CLOUD_VERSION
echo PackageSet Name: $PKGSET_NAME

curl -v -X "POST" "https://hub.infinyon.cloud/hub/v1/fvm/pkgset" \
     -H "Authorization: $BPKG_TOKEN" \
     -H 'Content-Type: application/json; charset=utf-8' \
     --data-binary @- << EOF
{
  "artifacts": [
    {
      "name": "fluvio",
      "version": "$FLUVIO_VERSION"
    },
    {
      "name": "fluvio-cloud",
      "version": "$FLUVIO_CLOUD_VERSION"
    },
    {
      "name": "fluvio-run",
      "version": "$FLUVIO_VERSION"
    },
    {
      "name": "cdk",
      "version": "$FLUVIO_VERSION"
    },
    {
      "name": "smdk",
      "version": "$FLUVIO_VERSION"
    }
  ],
  "pkgset": "$PKGSET_NAME"
}
EOF