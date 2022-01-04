[![CI for YARN dev tools (pip)](https://github.com/szilard-nemeth/yarn-dev-tools/actions/workflows/ci.yml/badge.svg)](https://github.com/szilard-nemeth/yarn-dev-tools/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/szilard-nemeth/yarn-dev-tools/branch/master/graph/badge.svg?token=OQD6FIFF7I)](https://codecov.io/gh/szilard-nemeth/yarn-dev-tools)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![GitHub language count](https://img.shields.io/github/languages/count/szilard-nemeth/yarn-dev-tools)


# YARN-dev tools

This project contains various developer helper scripts in order to simplify everyday tasks related to Git and Apache Hadoop YARN development.

### Getting started / Setup

You need to have python 3.8 and pip installed.
Run make from this directory and all python dependencies will be installed required by the project.


## Running the tests

TODO

## Main dependencies

* [gitpython](https://gitpython.readthedocs.io/en/stable/) - GitPython is a python library used to interact with git repositories, high-level like git-porcelain, or low-level like git-plumbing.
* [tabulate](https://pypi.org/project/tabulate/) - python-tabulate: Pretty-print tabular data in Python, a library and a command-line utility.
* [bs4](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) - Beautiful Soup is a Python library for pulling data out of HTML and XML files.
## Contributing

TODO 

## Authors

* **Szilard Nemeth** - *Initial work* - [Szilard Nemeth](https://github.com/szilard-nemeth)

## License

TODO 

## Acknowledgments

TODO


## Example commands


## Setup of precommit

Configure precommit as described in this blogpost: https://ljvmiranda921.github.io/notebook/2018/06/21/precommits-using-black-and-flake8/
Commands:
1. Install precommit: `pip install pre-commit`
2. Make sure to add pre-commit to your path. For example, on a Mac system, pre-commit is installed here: 
   `$HOME/Library/Python/3.8/bin/pre-commit`.
2. Execute `pre-commit install` to install git hooks in your `.git/` directory.

## Troubleshooting

### Installation issues
In case you're facing a similar issue:
```
An error has occurred: InvalidManifestError: 
=====> /<userhome>/.cache/pre-commit/repoBP08UH/.pre-commit-hooks.yaml does not exist
Check the log at /<userhome>/.cache/pre-commit/pre-commit.log
```
, please run: `pre-commit autoupdate`
More info here: https://github.com/pre-commit/pre-commit/issues/577

## Setting up handy aliases to use YARN-dev tools

There's only 1 prerequisite step to install python-commons which is a dependency of yarn-dev-tools.
The project root contains a requirements.txt file that has all the dependencies listed, including this.
Simply go to the root of this project and execute: 
```
pip3 install .
```

After this, you are ready to set up some aliases. In my system, I have these: 
https://github.com/szilard-nemeth/linux-env/blob/badf82f11f08d77cafbdbb19a1e2da83f392b1e4/workplace-specific/cloudera/scripts/yarn/yarn-dev-tools.sh#L73-L101

where: 
- SYSTEM_PYTHON_EXECUTABLE should be set to "/usr/local/bin/python3": 
```
➜ ls -la /usr/local/bin/python3
lrwxr-xr-x  1 snemeth  admin  38 Jun 14 23:43 /usr/local/bin/python3 -> ../Cellar/python@3.9/3.9.5/bin/python3
```
- VENV should be set to a virtualenv where yarndevtools is installed to. On my system it is set to "/Users/snemeth/development/my-repos/linux-env/venv"
- HADOOP_DEV_DIR should be set to the upstream Hadoop repo root, e.g.: "/Users/snemeth/development/apache/hadoop/"
- CLOUDERA_HADOOP_ROOT should be set to the downstream Hadoop repo root, e.g.: "/Users/snemeth/development/cloudera/hadoop/"
The latter 2 environment variables is better to be added to your bashrc file to keep them between the shells.


### Examples for YARN backporter
To backport YARN-6221 to 2 branches, run these commands:
```
yarn-backport YARN-6221 COMPX-6664 cdpd-master
yarn-backport YARN-6221 COMPX-6664 CDH-7.1-maint --no-fetch
```
The second parameter is the downstream jira ID.
The third parameter is the downstream branch.
The `--no-fetch` option is a means to skip git fetch on both repos.

Finally, I set up two aliases for pushing the changes to the downstream repo:
```
alias git-push-to-cdpdmaster="git push <REMOTE> HEAD:refs/for/cdpd-master%<REVIEWER_LIST>"
alias git-push-to-cdh71maint="git push <REMOTE> HEAD:refs/for/CDH-7.1-maint%<REVIEWER_LIST>"
```
where REVIEWER_LIST is in this format: "r=user1,r=user2,r=user3,..."

## CDSW Initial setup
1. Upload the initial setup scripts to the CDSW files, to the root directory (/home/cdsw)
- [initial-cdsw-setup.sh](yarndevtools/cdsw/scripts/initial-cdsw-setup.sh)
- [install-requirements.sh](yarndevtools/cdsw/scripts/install-requirements.sh)

2. Create a new CDSW session.
Wait for the session to be launched and open up a terminal by Clicking "Terminal access" on the top menu bar.


3. Execute this command:
```
~/initial-cdsw-setup.sh user cloudera
```


The script performs the following actions: 
1. Downloads the scripts that are cloning the upstream and downstream Hadoop repositories + installing yarndevtools itself as a python module.
The download location is: `/home/cdsw/scripts`
Please note that the files will be downloaded from the internal Cloudera mirror of this repository!
- [clone_downstream_repos.sh](yarndevtools/cdsw/scripts/clone_downstream_repos.sh)
- [clone_upstream_repos.sh](yarndevtools/cdsw/scripts/clone_upstream_repos.sh)

3. Executes the script described in step 2. 
This can take some time, especially cloning Hadoop.
Note: The individual CDSW jobs should make sure for themselves to clone the repositories.

4. Copies the cdsw_runner.py scripts for all jobs to `/home/cdsw/jobs`

5. All you have to do in CDSW is to set up the projects and their starter scripts like this:

| Project                                                       | Starter script location         | Arguments for script          |
|---------------------------------------------------------------|---------------------------------|-------------------------------|
| Jira umbrella checker reporting                               | scripts/start_job.py            | jira-umbrella-checker         |
| Unit test result aggregator                                   | scripts/start_job.py            | unit-test-result-aggregator   |
| Unit test result reporting                                    | scripts/start_job.py            | unit-test-result-reporting    |
| Branch comparator (Formerly: Downstream branchdiff reporting) | scripts/start_job.py            | branch-comparator             |
| Review sheet backport updater                                 | scripts/start_job.py | review-sheet-backport-updater |
| Reviewsync                                                    | scripts/start_job.py | reviewsync                    |