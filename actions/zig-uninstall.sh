#!/bin/bash
# Install Zig
# This is optimized for installing on GitHub Actions
# To run in Apple silicon, `ARCH=aarch64 zig-install`
set -e
echo "removing zig"
sudo rm -rf /usr/local/bin/zig



