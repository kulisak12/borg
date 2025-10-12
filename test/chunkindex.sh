#!/bin/bash

BORGTEST="$HOME/borgtest"
mkdir -p "$BORGTEST"
export BORG_REPO=ssh://localhost//"$BORGTEST/remote"
# you can also create a second repo for local testing
# export BORG_REPO="$BORGTEST/local"

# create a repository with one file
# use a small chunk size to create many chunks
borg repo-create --encryption=none
dd if=/dev/urandom of="$BORGTEST/file" bs=1024 count=10000
borg create --chunker-params=fixed,1024 test "$BORGTEST/file" # error
borg check # another way to trigger the error
