import re
from enum import Enum


class ExecutionMode(Enum):
    PRODUCTION = "prod"
    TEST = "test"


REPO_ROOT_DIRNAME = "yarn-dev-tools"
APACHE = "apache"
ORIGIN_TRUNK = "origin/trunk"
TRUNK = "trunk"
BRANCH_3_1 = "branch-3.1"
GERRIT_REVIEWER_LIST = "r=shuzirra,r=pbacsko,r=kmarton,r=gandras,r=bteke"
ENV_CLOUDERA_HADOOP_ROOT = "CLOUDERA_HADOOP_ROOT"
ENV_HADOOP_DEV_DIR = "HADOOP_DEV_DIR"
YARN_JIRA_ID_PATTERN = re.compile(r"(YARN-\d+)")
ANY_JIRA_ID_PATTERN = re.compile(r"([A-Z]+-\d+)")

# Symlink names
LATEST_LOG = "latest-log"
LATEST_SESSION = "latest-session"
LATEST_DATA_ZIP = "latest-command-data-zip"
DEFAULT_COMMAND_DATA_FILE_NAME = "command_data.zip"

# Do not leak bad ENV variable namings into the python code
LOADED_ENV_UPSTREAM_DIR = "upstream-hadoop-dir"
LOADED_ENV_DOWNSTREAM_DIR = "downstream-hadoop-dir"
PROJECT_NAME = "yarn_dev_tools"
DEST_DIR_PREFIX = "test"
HADOOP_REPO_TEMPLATE = "https://github.com/{user}/hadoop.git"
HADOOP_REPO_APACHE = HADOOP_REPO_TEMPLATE.format(user=APACHE)

# Patch constants
YARN_PATCH_FILENAME_REGEX = ".*(YARN-[0-9]+).*\\.patch"
PATCH_FILE_REGEX = "\\.\\d.*\\.patch$"
PATCH_EXTENSION = ".patch"
FIRST_PATCH_NUMBER = "001"

# TODO REMOVE THESE CONSTANTS LATER
YARN_TASKS = "yarn-tasks"
JIRA_UMBRELLA_DATA = "jira-umbrella-data"
JIRA_PATCH_DIFFER = "jira-patch-differ"
BRANCH_COMPARATOR = "branch-comparator"
