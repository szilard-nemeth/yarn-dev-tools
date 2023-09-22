#!/bin/bash


function uninstall-yarndevtools {
  echo "Uninstalling package: yarn-dev-tools"
  set +e
  pip3 -V
  pip3 show yarn-dev-tools
  pip3 uninstall -y yarn-dev-tools
}

function remove-deps {
  # It can happen that latest yarndevtools cannot be installed because of an older version of google-api-wrapper
  # Let's remove that as well
  # Example error from pip:
  # ERROR: Cannot install yarn-dev-tools and yarn-dev-tools==1.1.13 because these package versions have conflicting dependencies.
    #
    #The conflict is caused by:
    #    yarn-dev-tools 1.1.13 depends on python-common-lib==1.0.8
    #    google-api-wrapper2 1.0.4 depends on python-common-lib==1.0.4
    #
    #To fix this you could try to:
    #1. loosen the range of package versions you've specified
    #2. remove package versions to allow pip attempt to solve the dependency conflict
  echo "Uninstalling package: google-api-wrapper2"
  pip3 uninstall -y google-api-wrapper2

  echo "Uninstalling package: python-common-lib"
  pip3 uninstall -y python-common-lib
}

function manually-delete-yarndevtools {
  # Manually delete yarndev* dirs to overcome CDSW NFS issues
  # There are a ton of stale yarndevtools dirs in site-packages
  # Example dir listing
  # /home/cdsw/.local/lib/python3.8/site-packages/~-.%4evtools/yarn_dev_tools.py
  #/home/cdsw/.local/lib/python3.8/site-packages/~=%%7evtools/yarn_dev_tools.py
  #/home/cdsw/.local/lib/python3.8/site-packages/~=4ndevtools/yarn_dev_tools.py

  GLOBAL_SITE_PACKAGES=$(python3 -c 'import site; print(site.getsitepackages()[0])')
  USER_SITE_PACKAGES=$(python3 -m site --user-site)
  echo "GLOBAL_SITE_PACKAGES: $GLOBAL_SITE_PACKAGES"
  echo "USER_SITE_PACKAGES: $USER_SITE_PACKAGES"
  echo "Removing yarndevtools package remainders..."
  find $USER_SITE_PACKAGES -iname "yarn_dev_tools.py" | xargs dirname | xargs rm -rf
}

function install-yarndevtools {
  echo "Installing package: yarn-dev-tools"
  echo "Detected env var YARNDEVTOOLS_VERSION=$YARNDEVTOOLS_VERSION"
  if [ -z ${YARNDEVTOOLS_VERSION+x} ]; then
    echo "YARNDEVTOOLS_VERSION env var not set!"
    echo "To use the latest packaged version from pypi, set env var as YARNDEVTOOLS_VERSION=latest"
    echo "To use the latest packaged version from the repo (https://github.com/szilard-nemeth/yarn-dev-tools), set env var as YARNDEVTOOLS_VERSION=repo"
    echo "Exiting..."
    exit 2
  fi

  if [[ ${YARNDEVTOOLS_VERSION} == 'latest' ]]; then
    pip3 install yarn-dev-tools --force-reinstall
  elif [[ ${YARNDEVTOOLS_VERSION} == 'repo' ]]; then
    git_branch="cloudera-mirror-version" # harcode this for now
    # pip3 install git+ssh://git@github.com/szilard-nemeth/yarn-dev-tools.git
    # pip install git+https://github.com/<username>/<repo>.git@<branchname>
    pip3 install git+https://github.com/szilard-nemeth/yarn-dev-tools.git@$git_branch
  else
    pip3 install yarn-dev-tools=="$YARNDEVTOOLS_VERSION" --force-reinstall
  fi
  pip3 show yarn-dev-tools
}

function parse-args {
  echo $@
  if [ $# -ne 1 ]; then
      echo "Usage: $0 <execution mode>"
      echo "Example: $0 cloudera --> Uses execution mode: 'cloudera'"
      echo "Example: $0 upstream --> Uses execution mode: 'upstream'"
      exit 1
  fi
  EXEC_MODE="$1"
}



##################################################################################
set -x
parse-args "$@"
uninstall-yarndevtools
remove-deps
manually-delete-yarndevtools

set -e
install-yarndevtools