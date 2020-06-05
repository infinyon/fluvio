#!/bin/bash
nohup hugo server --watch --verbose --buildDrafts --cleanDestinationDir --disableFastRender > /tmp/hugo.out 2> /tmp/hugo.out &
