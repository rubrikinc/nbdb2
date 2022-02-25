#!/bin/bash
echo "Tailing container $1 with $2 color scheme"
export GREP_COLOR=$2
docker logs $1 --follow 2>&1| sed 's/.*/'"$1"' -- &/' | grep --color=always ".*"
