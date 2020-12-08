#!/bin/bash
set -e
echo "Operating System: $OS"
case $OS in
        'Linux') PLATFORM=linux-amd64
        ;;
        'ubuntu-latest') PLATFORM=linux-amd64
        ;;
        'macOS-latest') PLATFORM=darwin-amd64
        ;;
        *) echo "invalid os"
        exit
        ;;
esac

cd $(mktemp -d)
curl -sSL https://get.helm.sh/helm-${HELM_VERSION}-${PLATFORM}.tar.gz | tar zx
helm_bin=$(which helm)
sudo rm -f $helm_bin
sudo mv ${PLATFORM}/helm $helm_bin
chmod +x $helm_bin