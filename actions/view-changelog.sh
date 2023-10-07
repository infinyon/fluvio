#!/bin/sh
# run to view changelog updates
# run with argument "modify" to prepend changelog updates to CHANGELOG.md

set -e

MODIFY="${1:-no}"

LAST_TAG=$(gh release list --limit 1 --exclude-pre-releases | cut -w -f1)
CURR_TAG=$(cat VERSION)
echo git cliff ${LAST_TAG}..HEAD -t ${CURR_TAG}

if [ "$MODIFY" = "modify" ]; then
  git cliff ${LAST_TAG}..HEAD -t ${CURR_TAG} -p CHANGELOG.md
else
  git cliff ${LAST_TAG}..HEAD -t ${CURR_TAG} | less
fi
