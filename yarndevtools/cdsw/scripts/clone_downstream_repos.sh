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
    echo "Resetting to $CDPD_MASTER_BRANCH..."
    git reset --hard $CDPD_MASTER_BRANCH
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

clone-fetch-yarndevtools
clone-fetch-hadoop-downstream

# Install python requirements with the latest version of the requirements file
##NOTE: yarndevtools will be installed as a python module so it won't fail with:
curl -o /tmp/requirements-cdsw.txt https://raw.githubusercontent.com/szilard-nemeth/yarn-dev-tools/master/yarndevtools/cdsw/requirements.txt
pip3 install -r /tmp/requirements-cdsw.txt --force-reinstall