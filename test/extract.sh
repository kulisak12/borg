#!/bin/bash

BORGTEST="$HOME/borgtest"
mkdir -p "$BORGTEST"
export BORG_REPO="$BORGTEST/local"

# create a repository with one file
borg repo-create --encryption=none
dd if=/dev/urandom of="$BORGTEST/file" bs=1024 count=10000
borg create test "$BORGTEST/file"
