#!/bin/bash

# Install python requirements with the latest version of the requirements file
##NOTE: yarndevtools will be installed as a python module so it won't fail with:
curl -o /tmp/requirements-cdsw.txt https://raw.githubusercontent.com/szilard-nemeth/yarn-dev-tools/master/yarndevtools/cdsw/requirements.txt
pip3 install -r /tmp/requirements-cdsw.txt --upgrade