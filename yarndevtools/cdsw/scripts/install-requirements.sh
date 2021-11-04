#!/bin/bash

echo "Uninstalling package: 'yarn-dev-tools'"
pip3 show yarn-dev-tools
pip3 uninstall -y yarn-dev-tools

# Install python requirements with the latest version of the requirements file
##NOTE: yarndevtools will be installed as a python module so it won't fail with:
curl -o /tmp/requirements-cdsw.txt https://raw.githubusercontent.com/szilard-nemeth/yarn-dev-tools/master/yarndevtools/cdsw/requirements.txt
pip3 install -r /tmp/requirements-cdsw.txt --upgrade