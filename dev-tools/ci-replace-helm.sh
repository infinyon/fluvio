#!/bin/bash
set -e

declare -A PLATFORMS
PLATFORMS[ubuntu-latest]=linux-amd64
PLATFORMS[macOS-latest]=darwin-amd64

cd $(mktemp -d)
curl -sSL https://get.helm.sh/helm-${HELM_VERSION}-${PLATFORMS[$OS]}.tar.gz | tar zx
helm_bin=$(which helm)
sudo rm $helm_bin
sudo mv linux-amd64/helm $helm_bin
chmod +x $helm_bin