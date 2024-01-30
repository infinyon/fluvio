#!/bin/sh
# update CHANGELOG.md with unreleased changes since last tag
git cliff --unreleased -p CHANGELOG.md