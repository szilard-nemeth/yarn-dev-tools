#!/bin/bash

function clone-fetch-hadoop-downstream() {
  mkdir -p $REPOS_ROOT/cloudera
  cd $REPOS_ROOT/cloudera
  ls -la .
  git clone https://github.infra.cloudera.com/CDH/hadoop.git

  set -e
  cd $REPOS_ROOT/cloudera/hadoop/
  git fetch origin

  curr_ref=$(git rev-parse HEAD)
  orig_cdpdmaster_ref=$(git rev-parse $CDPD_MASTER_BRANCH)
  if [ "$curr_ref" != "$orig_cdpdmaster_ref" ]; then
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
STABLE_TAG="last-stable-branchcomparator"
MASTER_BRANCH="origin/master"
CDPD_MASTER_BRANCH="origin/cdpd-master"
CURR_BRANCH_YARN_DEV_TOOLS=$MASTER_BRANCH
CURR_BRANCH_HADOOP=$CDPD_MASTER_BRANCH

HOME_CDSW="/home/cdsw"
REPOS_ROOT="/home/cdsw/repos"

clone-fetch-hadoop-downstream