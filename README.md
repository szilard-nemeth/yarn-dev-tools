[![CI for YARN dev tools (pip)](https://github.com/szilard-nemeth/yarn-dev-tools/actions/workflows/ci.yml/badge.svg)](https://github.com/szilard-nemeth/yarn-dev-tools/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/szilard-nemeth/yarn-dev-tools/branch/master/graph/badge.svg?token=OQD6FIFF7I)](https://codecov.io/gh/szilard-nemeth/yarn-dev-tools)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![GitHub language count](https://img.shields.io/github/languages/count/szilard-nemeth/yarn-dev-tools)


# YARN-dev-tools

This project contains various developer helper scripts in order to simplify every day tasks related to Apache Hadoop YARN development.

## Main dependencies

* [gitpython](https://gitpython.readthedocs.io/en/stable/) - GitPython is a python library used to interact with git repositories, high-level like git-porcelain, or low-level like git-plumbing.
* [tabulate](https://pypi.org/project/tabulate/) - python-tabulate: Pretty-print tabular data in Python, a library and a command-line utility.
* [bs4](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) - Beautiful Soup is a Python library for pulling data out of HTML and XML files.

* TODO: Missing dependencies

## Contributing

TODO 

## Authors

* **Szilard Nemeth** - *Initial work* - [Szilard Nemeth](https://github.com/szilard-nemeth)

## License

TODO 

## Acknowledgments

TODO

# Getting started

In order to use this tool, you need to have at least Python 3.8 installed.

## Use yarn-dev-tools from package (Recommended)
If you don't want to tinker with the source code, you can download [yarn-dev-tools](https://pypi.org/project/yarn-dev-tools/#history) from PyPi as well.
This is probably the easiest way to use it.
You don't need to install anything manually as I created a [script](initial_setup.sh) that performs the installation automatically.
The script has a `setup-vars` function at the beginning that defines some environment variables:

These are the following:
- `YARNDEVTOOLS_ROOT`: Specifies the directory where the Python virtualenv will be created and yarn-dev-tools will be installed to this virtualenv.
- `HADOOP_DEV_DIR` Should be set to the upstream Hadoop repository root, e.g.: "~/development/apache/hadoop/"
- `CLOUDERA_HADOOP_ROOT` Should be set to the downstream Hadoop repository root, e.g.: "~/development/cloudera/hadoop/"

The latter two environment variables is better to be added to your bashrc / zshrc file (depending on what shell you are using) to keep them between the shells.

## Use yarn-dev-tools from source
If you want to use yarn-dev-tools from source, first you need to install its dependencies.
The project root contains a pyproject.toml file that has all the dependencies listed.
The project uses Poetry to resolve the dependencies so you need to [install poetry](https://python-poetry.org/docs/#installation) as well.
Simply go to the root of this project and execute `poetry install --without localdev`.
Alternatively, you can run `make` from the root of the project.

## Setting up handy aliases to use yarn-dev-tools
If you completed the installation (either by source or by package), you may want to define some shell aliases to use the tool more easily.
In my system, I have [these](
https://github.com/szilard-nemeth/linux-env/blob/master/workplace-specific/cloudera/scripts/yarn/setup-yarn-dev-tools-aliases.sh).
Please make sure to source this script so that the command 'yarndevtools' will be available since it's defined as a function.
It is important to specify `HADOOP_DEV_DIR` and `CLOUDERA_HADOOP_ROOT` as mentioned above, before sourcing the script.

After these steps, you will have a basic set of aliases that is enough to get you started.


# Setting up yarn-dev-tools with Cloudera CDSW

## Initial setup
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
The download location is: `/home/cdsw/scripts`<br>
Please note that the files will be downloaded from the GitHub master branch of this repository!
- [clone_downstream_repos.sh](yarndevtools/cdsw/scripts/clone_downstream_repos.sh)
- [clone_upstream_repos.sh](yarndevtools/cdsw/scripts/clone_upstream_repos.sh)

2. Executes the script described in step 2. 
This can take some time, especially cloning Hadoop.
Note: The individual CDSW jobs should make sure for themselves to clone the repositories.

3. Copies the [python-based job configs](yarndevtools/cdsw/job_configs) for all jobs to `/home/cdsw/jobs`

4. All you have to do in CDSW is to set up the projects and their starter scripts like this:

| Project                       | Starter script location | Arguments for script          |
|-------------------------------|-------------------------|-------------------------------|
| Jira umbrella data fetcher    | scripts/start_job.py    | jira-umbrella-data-fetcher    |
| Unit test result aggregator   | scripts/start_job.py    | unit-test-result-aggregator   |
| Unit test result fetcher      | scripts/start_job.py    | unit-test-result-fetcher      |
| Branch comparator             | scripts/start_job.py    | branch-comparator             |
| Review sheet backport updater | scripts/start_job.py    | review-sheet-backport-updater |
| Reviewsync                    | scripts/start_job.py    | reviewsync                    |

## CDSW environment variables

### Common environment variables for CDSW jobs
All common environment variables are used from a class called [CdswEnvVar](https://github.com/szilard-nemeth/yarn-dev-tools/blob/b484daffde3c6f70dc3dab71f92150738855d668/yarndevtools/cdsw/constants.py#L15-L32)

| Name                               | Level         | Mandatory? | Default value                         | Description                                                                                                                                                                                                                                       |
|------------------------------------|---------------|------------|:--------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| MAIL_ACC_USER                      | Project       | Yes        | N/A                                   | Username for the Gmail account that is being used for sending emails                                                                                                                                                                              |
| MAIL_ACC_PASSWORD                  | Project       | Yes        | N/A                                   | Password for the Gmail account that is being used for sending emails                                                                                                                                                                              |
| MAIL_RECIPIENTS                    | Project or Job | No         | yarn_eng_bp@cloudera.com              | Comma separated email addresses to send emails to. If not specified, the YARN mailing list is the default: yarn_eng_bp@cloudera.com<br/> Can be specified on Job-level, too                                                                       |
| ENABLE_GOOGLE_DRIVE_INTEGRATION    | Project or Job | No         | True                                  | Whether to enable Google Drive integration for saving result files.                                                                                                                                                                               |
| DEBUG_ENABLED                      | Project or Job | No         | Job-level default                     | Whether to enable debug mode for yarndevtools commands. Adds the `--debug` switch to CLI commands. Accepted values: True, False                                                                                                                   |
| OVERRIDE_SCRIPT_BASEDIR            | Project       | No         | N/A                                   | Option to change the scripts dir for CDSW jobs. Do not modify unless absolutely necessary!                                                                                                                                                        |
| ENABLE_LOGGER_HANDLER_SANITY_CHECK | Project or Job | No         | True                                  | Whether to enable sanity checking the number of loggers after first logger initialization. Can be disabled if errors come up during logger setup.                                                                                                 |
| CLOUDERA_HADOOP_ROOT               | Project       | Yes        | <CDSW_BASEDIR>/repos/cloudera/hadoop/ | Downstream repository path for Hadoop. [Auto set for CDSW](https://github.com/szilard-nemeth/yarn-dev-tools/blob/5c1f23a0bf74c46b76efe3739920fd299fc9d6c6/yarndevtools/cdsw/cdsw_common.py#L134-L139)                                             |
| HADOOP_DEV_DIR                     | Project       | Yes        | <CDSW_BASEDIR>/repos/apache/hadoop/   | Upstream repository path for Hadoop. [Auto set for CDSW](https://github.com/szilard-nemeth/yarn-dev-tools/blob/5c1f23a0bf74c46b76efe3739920fd299fc9d6c6/yarndevtools/cdsw/cdsw_common.py#L134-L139)                                               |
| PYTHONPATH                         | Project       | No         | $PYTHONPATH:/home/cdsw/scripts        | Tweaked PYTHONPATH, to correctly reload python dependencies. Do not modify unless absolutely necessary!                                                                                                                                           |
| TEST_EXECUTION_MODE                         | Project       | No         | cloudera                              | Test execution mode. Can take values of `TestExecMode` enum. For CDSW, it should be always set to `TestExecMode.CLOUDERA`                                                                                                                         |
| PYTHON_MODULE_MODE                         | Project       | No         | user                                  | Python module mode. Can take values of `user` and `global`. For CDSW, it should be always set to `user`.                                                                                                                                          |
| INSTALL_REQUIREMENTS                         | Project       | No         | True                                  | Whether to run the [install-requirements.sh](https://github.com/szilard-nemeth/yarn-dev-tools/blob/fb3473ba7d92c96baf8788ef850e4527c5a0cb3a/yarndevtools/cdsw/scripts/install-requirements.sh) script. Do not modify unless absolutely necessary! |
| RESTART_PROCESS_WHEN_REQUIREMENTS_INSTALLED                         | Project       | No         | False                                 | Only used for testing                                                                                                                                                                                                                             | 
                                                                                                                                           |

### Environment variables for job: Jira umbrella data fetcher

Corresponding class: [JiraUmbrellaFetcherEnvVar](https://github.com/szilard-nemeth/yarn-dev-tools/blob/b484daffde3c6f70dc3dab71f92150738855d668/yarndevtools/cdsw/constants.py#L41-L43)

| Name             | Level        | Mandatory? | Description                               |
|------------------|--------------|------------|-------------------------------------------|
| UMBRELLA_IDS     | Only for job | Yes        | Comma separated list of umbrella Jira IDs |



### Environment variables for job: Unit test result fetcher

Corresponding class: [UnitTestResultFetcherEnvVar](https://github.com/szilard-nemeth/yarn-dev-tools/blob/b484daffde3c6f70dc3dab71f92150738855d668/yarndevtools/cdsw/constants.py#L80-L83)
Some of these env vars are declared in a class called [CdswEnvVar](https://github.com/szilard-nemeth/yarn-dev-tools/blob/b484daffde3c6f70dc3dab71f92150738855d668/yarndevtools/cdsw/constants.py#L15-L32)

| Name             | Level        | Mandatory? | Description                                                                  |
|------------------|--------------|------------|------------------------------------------------------------------------------|
| JENKINS_USER     | Only for job | Yes        | User name for Cloudera Jenkins API access.  |
| JENKINS_PASSWORD | Only for job | Yes        | Password for Cloudera Jenkins API access.                                    |
| TODO             | Only for job | Yes        | Password for Cloudera Jenkins API access                                     |
| TODO | TODO |         |                                      |
| TODO | TODO |         |                                      |
| TODO | TODO |         |                                      |
| TODO | TODO |         |                                      |


### Environment variables for job: Branch comparator

Corresponding class: [BranchComparatorEnvVar](https://github.com/szilard-nemeth/yarn-dev-tools/blob/b484daffde3c6f70dc3dab71f92150738855d668/yarndevtools/cdsw/constants.py#L35-L38)

| Name                       | Level        | Mandatory? | Default value                      | Description                           |
|----------------------------|--------------|------------|:-----------------------------------|---------------------------------------|
| BRANCH_COMP_FEATURE_BRANCH | Only for job | No         | origin/CDH-7.1-maint               | Name of the feature branch            |
| BRANCH_COMP_MASTER_BRANCH  | Only for job | Yes        | origin/cdpd-master                 | Name of the master branch             |
| BRANCH_COMP_REPO_TYPE      | Only for job | Yes        | downstream (`RepoType.DOWNSTREAM`) | Repository type. Can take a value of `RepoType` |



# Use-cases


### Examples for YARN backporter
To backport YARN-6221 to 2 branches, run these commands:
```
yarn-backport YARN-6221 COMPX-6664 cdpd-master
yarn-backport YARN-6221 COMPX-6664 CDH-7.1-maint --no-fetch
```
The first argument is the upstream Jira ID<br>
The second argument is the downstream Jira ID.<br>
The third argument is the downstream branch.<br>
The `--no-fetch` option is a means to skip git fetch on both repos.

### How to backport to an already existing relation chain?
1. Go to Gerrit UI and download the patch.
For example: 
```
git fetch "https://gerrit.sjc.cloudera.com/cdh/hadoop" refs/changes/29/156429/5 && git checkout FETCH_HEAD
```
2. Checkout a new branch
```
git checkout -b my-relation-chain 
```

3. Run backporter with: 
```
yarn-backport YARN-10314 COMPX-7855 CDH-7.1.7.1000 --no-fetch --downstream_base_ref my-relation-chain
```
where:<br>
The first argument is the upstream Jira ID<br>
The second argument is the downstream Jira ID.<br>
The third argument is the downstream branch.<br>
The `--no-fetch` option is a means to skip git fetch on both repos.<br>
The `--downstream_base_ref <local-branch` is a way to use a local branch to base the backport on so the Git remote name won't be prepended.


Finally, I set up two aliases for pushing the changes to the downstream repo:
```
alias git-push-to-cdpdmaster="git push <REMOTE> HEAD:refs/for/cdpd-master%<REVIEWER_LIST>"
alias git-push-to-cdh71maint="git push <REMOTE> HEAD:refs/for/CDH-7.1-maint%<REVIEWER_LIST>"
```
where REVIEWER_LIST is in this format: "r=user1,r=user2,r=user3,..."


# Contributing

## Setup of pre-commit

Configure precommit as described in [this blogpost](https://ljvmiranda921.github.io/notebook/2018/06/21/precommits-using-black-and-flake8/).

Commands:
1. Install precommit: `pip install pre-commit`
2. Make sure to add pre-commit to your path. For example, on a Mac system, pre-commit is installed here: 
   `$HOME/Library/Python/3.8/bin/pre-commit`.
2. Execute `pre-commit install` to install git hooks in your `.git/` directory.

## Running the tests

TODO

## Troubleshooting

### Installation issues
In case you're facing a similar issue:
```
An error has occurred: InvalidManifestError: 
=====> /<userhome>/.cache/pre-commit/repoBP08UH/.pre-commit-hooks.yaml does not exist
Check the log at /<userhome>/.cache/pre-commit/pre-commit.log
```
, please run: `pre-commit autoupdate`

More info can be found [here](https://github.com/pre-commit/pre-commit/issues/577).