#!/bin/bash

#set -e
mkdir -p ~/repos/snemeth
mkdir -p ~/repos/cloudera
cd ~/repos/snemeth
git clone https://github.infra.cloudera.com/snemeth/linux-env-mirror.git
git fetch --all && git --hard reset origin/master

cd ~/repos/cloudera
git clone https://github.infra.cloudera.com/CDH/hadoop.git
git fetch --all && git --hard reset origin/cdpd-master

cd ~/repos/snemeth/linux-env-mirror/workplace-specific/cloudera/scripts/yarn/python/
pip3 install -r requirements.txt
pip3 install 3to2 --user
cd ~/repos/snemeth/linux-env-mirror/workplace-specific/cloudera/scripts/yarn/python/yarndevfunc

# Hacks to convert back Python3 to Python2 & Add future-fstrings
3to2 . --no-diffs -w
pip install future-fstrings
#https://stackoverflow.com/a/9612560/1106893
find . -type f -iname "*\.py" -print0 | while read -d $'\0' file
do
  #https://stackoverflow.com/a/46182112/1106893
  (echo "# -*- coding: future_fstrings -*-" && cat $file) > /tmp/$file && mv /tmp/$file $file
  chmod +x $file
done