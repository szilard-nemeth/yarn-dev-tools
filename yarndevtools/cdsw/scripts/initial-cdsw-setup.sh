#!/bin/bash
set -x

if [ $# -ne 2 ]; then
    echo "Usage: $0 <python module mode> <execution mode>"
    echo "Example: $0 global cloudera --> Uses 'global' module mode with execution mode: 'cloudera'"
    echo "Example: $0 user upstream --> Uses 'user' module mode with execution mode: 'upstream'"
    exit 1
fi

PYTHON_MODULE_MODE=""
if [[ "$1" == "global" ]]; then
  PYTHON_MODULE_MODE="global"
  shift
elif [[ "$1" == "user" ]]; then
  PYTHON_MODULE_MODE="user"
  shift
fi

EXEC_MODE=""
if [[ "$1" == "upstream" ]]; then
  EXEC_MODE="upstream"
  shift
elif [[ "$1" == "cloudera" ]]; then
  EXEC_MODE="cloudera"
  shift
fi

#Validations

(( "$PYTHON_MODULE_MODE" != "global" || "$PYTHON_MODULE_MODE" != "user" )) && echo "Python module mode should be either 'user' or 'global'!" && exit 1
(( "$EXEC_MODE" != "upstream" || "$EXEC_MODE" != "cloudera" )) && echo "Execution mode should be either 'upstream' or 'cloudera'!" && exit 1

echo "Python module mode: $PYTHON_MODULE_MODE"
echo "Execution mode: $EXEC_MODE"

echo "Downloading clone repository scripts..."
SCRIPTS_DIR="/home/cdsw/downloaded_scripts"
mkdir $SCRIPTS_DIR

#No errors allowed after this point!
set -e

curl -o $SCRIPTS_DIR/clone_downstream_repos.sh https://raw.githubusercontent.com/szilard-nemeth/yarn-dev-tools/master/yarndevtools/cdsw/scripts/clone_downstream_repos.sh
curl -o $SCRIPTS_DIR/clone_upstream_repos.sh https://raw.githubusercontent.com/szilard-nemeth/yarn-dev-tools/master/yarndevtools/cdsw/scripts/clone_upstream_repos.sh
chmod +x $SCRIPTS_DIR/clone_downstream_repos.sh
chmod +x $SCRIPTS_DIR/clone_upstream_repos.sh

# Always run clone_upstream_repos.sh
echo "Cloning upstream repos..."
$SCRIPTS_DIR/clone_upstream_repos.sh

# Only run clone_downstream_repos.sh if execution mode == "cloudera"
if [[ "$EXEC_MODE" == "cloudera" ]]; then
  echo "Cloning downstream repos..."
  $SCRIPTS_DIR/clone_downstream_repos.sh
fi

echo "Installing python requirements..."

if [[ "$EXEC_MODE" == "cloudera" ]]; then
  # TODO Duplicated commands: install-requirements.sh
  curl -o /tmp/requirements-cdsw.txt https://raw.githubusercontent.com/szilard-nemeth/yarn-dev-tools/master/yarndevtools/cdsw/requirements.txt
else
  curl -o /tmp/requirements-cdsw.txt https://raw.githubusercontent.com/szilard-nemeth/yarn-dev-tools/master/yarndevtools/cdsw/requirements-github.txt
fi
pip3 install -r /tmp/requirements-cdsw.txt --force-reinstall

GLOBAL_SITE_PACKAGES=$(python3 -c 'import site; print(site.getsitepackages()[0])')
USER_SITE_PACKAGES=$(python3 -m site --user-site)
echo "GLOBAL_SITE_PACKAGES: $GLOBAL_SITE_PACKAGES"
echo "USER_SITE_PACKAGES: $USER_SITE_PACKAGES"

echo "Listing: global python packages: $(ls -la $GLOBAL_SITE_PACKAGES)"
echo "Listing user python packages: $(ls -la $USER_SITE_PACKAGES)"
PYTHON_SITE=$USER_SITE_PACKAGES
if [[ "$PYTHON_MODULE_MODE" == "global" ]]; then
  PYTHON_SITE=$GLOBAL_SITE_PACKAGES
fi

#Set up some convenience variables
CDSW_PACKAGE_ROOT="$PYTHON_SITE/yarndevtools/cdsw"
JOBS_ROOT=/home/cdsw/jobs/
JOB_DS_BRANCHDIFF_REPORTING="downstream-branchdiff-reporting"
JOB_JIRA_UMBRELLA_CHECKER="jira-umbrella-checker"
JOB_UT_RESULT_AGGREGATOR="unit-test-result-aggregator"
JOB_UT_RESULT_REPORTER="unit-test-result-reporting"
CDSW_RUNNER_SCRIPT="cdsw_runner.py"

echo "Copying scripts to place..."
mkdir -p $JOBS_ROOT
mkdir -p $JOBS_ROOT/$JOB_DS_BRANCHDIFF_REPORTING
mkdir -p $JOBS_ROOT/$JOB_JIRA_UMBRELLA_CHECKER/
mkdir -p $JOBS_ROOT/$JOB_UT_RESULT_AGGREGATOR/
mkdir -p $JOBS_ROOT/$JOB_UT_RESULT_REPORTER/

cp $CDSW_PACKAGE_ROOT/$JOB_DS_BRANCHDIFF_REPORTING/$CDSW_RUNNER_SCRIPT $JOBS_ROOT/$JOB_DS_BRANCHDIFF_REPORTING/$CDSW_RUNNER_SCRIPT
cp $CDSW_PACKAGE_ROOT/$JOB_JIRA_UMBRELLA_CHECKER/$CDSW_RUNNER_SCRIPT $JOBS_ROOT/$JOB_JIRA_UMBRELLA_CHECKER/$CDSW_RUNNER_SCRIPT
cp $CDSW_PACKAGE_ROOT/$JOB_UT_RESULT_AGGREGATOR/$CDSW_RUNNER_SCRIPT $JOBS_ROOT/$JOB_UT_RESULT_AGGREGATOR/$CDSW_RUNNER_SCRIPT
cp $CDSW_PACKAGE_ROOT/$JOB_UT_RESULT_REPORTER/$CDSW_RUNNER_SCRIPT $JOBS_ROOT/$JOB_UT_RESULT_REPORTER/$CDSW_RUNNER_SCRIPT

echo "Adding execute flag for all cdsw_runner.py scripts..."
find /home/cdsw/jobs/ | grep cdsw_runner | xargs chmod +x
echo "Installed jobs:"
find /home/cdsw/jobs/ | grep cdsw_runner | xargs ls -la
set +x