#!/bin/bash

# set vars
export URL=$1
export SUBDIR_1=$2
export SUBDIR_2=$3
export ARTDIR="./artifacts/$SUBDIR_1/$SUBDIR_2"

# create a directory to store artifacts if none exists
[ ! -d "$ARTDIR" ] && mkdir --parents $ARTDIR && echo "created artifacts directory at $ARTDIR"

# pull down artifacts from a specific job
wget --verbose --header "Circle-Token: $CIRCLE_USER_TOKEN" --directory-prefix="$ARTDIR" $URL
