#!/bin/bash
set -x

# Latest version
yarndevtools_pip_version_spec="yarn-dev-tools"
if [[ ! -z "$YARNDEVTOOLS_MODULE_VERSION" ]]; then
  echo "Recognized YARNDEVTOOLS_MODULE_VERSION=$YARNDEVTOOLS_MODULE_VERSION"
  yarndevtools_pip_version_spec="yarn-dev-tools==$YARNDEVTOOLS_MODULE_VERSION"
fi

echo "Using module version for yarndevtools: $yarndevtools_pip_version_spec"



echo "Uninstalling package: yarn-dev-tools"
set +e
pip3 -V
pip3 show yarn-dev-tools
pip3 uninstall -y yarn-dev-tools

set -e
echo $@
if [ $# -ne 1 ]; then
    echo "Usage: $0 <execution mode>"
    echo "Example: $0 cloudera --> Uses execution mode: 'cloudera'"
    echo "Example: $0 upstream --> Uses execution mode: 'upstream'"
    exit 1
fi

EXEC_MODE="$1"
echo "Installing package: yarn-dev-tools"
pip3 install $yarndevtools_pip_version_spec --force-reinstall
pip3 show yarn-dev-tools