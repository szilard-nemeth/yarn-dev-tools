#!/bin/bash

function clone-fetch-hadoop() {
  mkdir -p $REPOS_ROOT/apache
  cd $REPOS_ROOT/apache
  ls -la .
  git clone https://github.com/apache/hadoop.git

  set -e
  cd $REPOS_ROOT/apache/hadoop/
  git fetch origin

  curr_ref=$(git rev-parse HEAD)
  origin_trunk_curr_ref=$(git rev-parse $TRUNK_BRANCH)
  if [ "$curr_ref" != "$origin_trunk_curr_ref" ]; then
    echo "Resetting to $TRUNK_BRANCH..."
    git reset --hard $TRUNK_BRANCH
  fi

  set +e
}

#Setup vars
TRUNK_BRANCH="origin/trunk"
REPOS_ROOT="/home/cdsw/repos"

clone-fetch-hadoop

# Install python requirements
cd $REPOS_ROOT/snemeth/yarn-dev-tools
pip3 install -r requirements.txt --force-reinstall