#!/bin/bash
set -x

#echo "Uninstalling package: 'yarn-dev-tools'"
pip3 show yarn-dev-tools
pip3 uninstall -y yarn-dev-tools
pip3 uninstall -y python-commons

if [ $# -ne 1 ]; then
    echo "Usage: $0 <execution mode>"
    echo "Example: $0 cloudera --> Uses execution mode: 'cloudera'"
    echo "Example: $0 upstream --> Uses execution mode: 'upstream'"
    exit 1
fi

EXEC_MODE="$1"

if [[ "$EXEC_MODE" == "cloudera" ]]; then
  curl -o /tmp/requirements-cdsw.txt https://raw.githubusercontent.com/szilard-nemeth/yarn-dev-tools/master/yarndevtools/cdsw/requirements.txt
else
  curl -o /tmp/requirements-cdsw.txt https://raw.githubusercontent.com/szilard-nemeth/yarn-dev-tools/master/yarndevtools/cdsw/requirements-github.txt
fi
echo "Installing python requirements..."
pip3 install -r /tmp/requirements-cdsw.txt --upgrade --force-reinstall