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
    if [ -z ${TEST_EXEC_MODE+x} ]; then
      echo "Test exec mode not set, resetting to $CDPD_MASTER_BRANCH with git reset --hard..."
      git reset --hard $CDPD_MASTER_BRANCH
    else
      echo "Test exec mode set, resetting to $CDPD_MASTER_BRANCH with git reset..."
      if [[ -z $(git status -s) ]]; then
        echo "There are unstaged changes in repo `pwd`. Exiting"
        return 1
      fi
      git reset $CDPD_MASTER_BRANCH
    fi
  fi

  set +e
}

#Setup vars
TRUNK_BRANCH="origin/trunk"
REPOS_ROOT="/home/cdsw/repos"

clone-fetch-hadoop