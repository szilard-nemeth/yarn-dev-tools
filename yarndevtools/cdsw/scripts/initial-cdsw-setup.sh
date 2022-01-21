#!/bin/bash
set -x

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Received arguments: $@"
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
#No errors allowed in curl / chmod
REPOS_ROOT="/home/cdsw/repos/"
set -e
mkdir -p $REPOS_ROOT
cd $REPOS_ROOT

set +e
git clone https://github.com/szilard-nemeth/yarn-dev-tools.git

CDSW_ROOT="/home/cdsw/"
SCRIPTS_ROOT="$CDSW_ROOT/scripts"
mkdir -p $CDSW_ROOT
mkdir -p $SCRIPTS_ROOT
cp $REPOS_ROOT/yarn-dev-tools/yarndevtools/cdsw/scripts/*.sh $CDSW_ROOT/scripts
cp $REPOS_ROOT/yarn-dev-tools/yarndevtools/cdsw/start_job.py $CDSW_ROOT/scripts
cp -R $REPOS_ROOT/yarn-dev-tools/yarndevtools/cdsw/libreloader/ $CDSW_ROOT/scripts/libreloader
set -e

CLONE_DS_REPOS_SCRIPT_PATH="$SCRIPTS_ROOT/clone_downstream_repos.sh"
CLONE_US_REPOS_SCRIPT_PATH="$SCRIPTS_ROOT/clone_upstream_repos.sh"
INSTALL_REQUIREMENTS_SCRIPT_PATH="$SCRIPTS_ROOT/install-requirements.sh"
START_JOB_SCRIPT_PATH="$SCRIPTS_ROOT/start_job.py"


chmod +x $CLONE_DS_REPOS_SCRIPT_PATH
chmod +x $CLONE_US_REPOS_SCRIPT_PATH
chmod +x $INSTALL_REQUIREMENTS_SCRIPT_PATH
chmod +x $START_JOB_SCRIPT_PATH

set -e
#No errors allowed after this point!

# Always run clone_upstream_repos.sh
echo "Cloning upstream repos..."
$CLONE_US_REPOS_SCRIPT_PATH

# Only run clone_downstream_repos.sh if execution mode == "cloudera"
if [[ "$EXEC_MODE" == "cloudera" ]]; then
  echo "Cloning downstream repos..."
  $CLONE_DS_REPOS_SCRIPT_PATH
fi

. $INSTALL_REQUIREMENTS_SCRIPT_PATH $EXEC_MODE

# =================================================================
# Set up python package root
# =================================================================
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


# =================================================================
# COPY JOB CONFIGURATIONS TO THEIR PLACE
# !!! FROM THIS POINT, USE ALL FILES FROM THE PYTHON MODULE !!!
# =================================================================

#Set up some convenience variables
CDSW_PACKAGE_ROOT="$PYTHON_SITE/yarndevtools/cdsw"
CDSW_PACKAGE_ROOT_JOB_CONFIGS="$CDSW_PACKAGE_ROOT/job_configs"
JOBS_ROOT="$CDSW_ROOT/jobs/"
CDSW_RUNNER_SCRIPT_PATH="$SCRIPTS_ROOT/cdsw_runner.py"

# IMPORTANT: CDSW is able to launch linked scripts, but cannot modify and save the job's form because it thinks
# the linked script is not there.
echo "Copying scripts to place..."
rm -rf $JOBS_ROOT
mkdir -p $JOBS_ROOT
cp $CDSW_PACKAGE_ROOT_JOB_CONFIGS/*.py $JOBS_ROOT/

echo "Copying cdsw_runner.py into place..."
cp "$CDSW_PACKAGE_ROOT/common/cdsw_runner.py" "$CDSW_RUNNER_SCRIPT_PATH"

echo "Installed jobs:"
find $JOBS_ROOT | xargs ls -la
set +x

echo "Start jobs script path:"
echo $START_JOB_SCRIPT_PATH

echo "CDSW runner script path:"
echo $CDSW_RUNNER_SCRIPT_PATH