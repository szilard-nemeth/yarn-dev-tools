#!/bin/bash
set -x
mkdir /tmp/yarn-cdsw-setup

echo "Downloading clone repository scripts..."
curl -o /tmp/yarn-cdsw-setup/clone_downstream_repos.sh https://raw.githubusercontent.com/szilard-nemeth/yarn-dev-tools/master/yarndevtools/cdsw/scripts/clone_downstream_repos.sh
curl -o /tmp/yarn-cdsw-setup/clone_upstream_repos.sh https://raw.githubusercontent.com/szilard-nemeth/yarn-dev-tools/master/yarndevtools/cdsw/scripts/clone_upstream_repos.sh

echo "Cloning upstream repos..."
/tmp/yarn-cdsw-setup/clone_upstream_repos.sh

echo "Cloning downstream repos..."
/tmp/yarn-cdsw-setup/clone_downstream_repos.sh

GLOBAL_SITE_PACKAGES=$(python3 -c 'import site; print(site.getsitepackages()[0])')
USER_SITE_PACKAGES=$(python3 -m site --user-site)

echo "Global python packages: $(ls -la $GLOBAL_SITE_PACKAGES)"
echo "User python packages: $(ls -la $USER_SITE_PACKAGES)"

#set up some convenience variables
CDSW_PACKAGE_ROOT="$USER_SITE_PACKAGES/yarndevtools/cdsw"
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

echo "Installed jobs:"
find /home/cdsw/jobs
set +x