#!/bin/bash

function clone-fetch-yarndevtools() {
  #set -e
  mkdir -p $REPOS_ROOT/snemeth
  mkdir -p $REPOS_ROOT/cloudera

  #Clone / Fetch yarn-dev-tools-mirror
  cd $REPOS_ROOT/snemeth
  git clone https://github.infra.cloudera.com/snemeth/yarn-dev-tools-mirror.git
  set -e
  cd $REPOS_ROOT/snemeth/yarn-dev-tools-mirror/
  git fetch --all --tags && git reset --hard $CURR_BRANCH_YARN_DEV_TOOLS
  cd ..
  rm -rf ./yarn-dev-tools
  mv yarn-dev-tools-mirror yarn-dev-tools
  set +e
}

function clone-fetch-hadoop() {
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


# This is already cloned + fetched by the CDSW script
#clone-fetch-yarndevtools
clone-fetch-hadoop

# Install python requirements
cd $REPOS_ROOT/snemeth/yarn-dev-tools
pip3 install -r requirements.txt --force-reinstall