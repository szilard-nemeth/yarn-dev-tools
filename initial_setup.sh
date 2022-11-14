#!/usr/bin/env bash

function setup-vars() {
    # HADOOP_DEV_DIR and CLOUDERA_HADOOP_ROOT need to be defined in the environment
    export UPSTREAM_HADOOP_DIR=${HADOOP_DEV_DIR}
    export DOWNSTREAM_HADOOP_DIR=${CLOUDERA_HADOOP_ROOT}

    # Replace this with the dir of your choice
    export YARNDEVTOOLS_ROOT="$HOME/.yarndevtools"
}

function setup-yarndevtools-package {
    set -x
    YARNDEVTOOLS_VERSION="1.1.2"
    ORIG_PYTHONPATH=$PYTHONPATH
    unset PYTHONPATH

    # local target_dir=$(mktemp -d -t yarndevtools)
    local target_dir="$YARNDEVTOOLS_ROOT"
    echo "Using target dir for yarndevtools: $target_dir"
    mkdir -p $target_dir
    cd $target_dir

    python3 -m venv venv
    echo "Activating virtualenv..."
    source venv/bin/activate
    pip3 install -I yarn-dev-tools==$YARNDEVTOOLS_VERSION
    local site_packages=$(pip3 list -v 2>/dev/null | grep "yarn-dev-tools" | tr -s ' ' | cut -d' ' -f3)
    echo "yarn-dev-tools is installed to: $site_packages"

    # cleanup
    echo "Deactivating virtualenv..."
    deactivate
    PYTHONPATH=$ORIG_PYTHONPATH
    set +x
}

setup-vars
setup-yarndevtools-package
