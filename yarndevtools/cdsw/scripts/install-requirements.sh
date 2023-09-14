#!/bin/bash
set -x

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

YARNDEVTOOLS_VERSION="1.1.9"
echo "Installing package: yarn-dev-tools"
pip3 install yarn-dev-tools==$YARNDEVTOOLS_VERSION --force-reinstall
pip3 show yarn-dev-tools