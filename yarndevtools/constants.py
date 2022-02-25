import re


REPO_ROOT_DIRNAME = "yarn-dev-tools"
YARNDEVTOOLS_MODULE_NAME = "yarndevtools"
APACHE = "apache"
HADOOP = "hadoop"
CLOUDERA = "cloudera"
ORIGIN_TRUNK = "origin/trunk"
ORIGIN_BRANCH_3_3 = "origin/branch-3.3"
ORIGIN_BRANCH_3_2 = "origin/branch-3.2"
TRUNK = "trunk"
BRANCH_3_1 = "branch-3.1"
BRANCH_3_3 = "branch-3.3"
# TODO Migrate these to a new enum
ENV_CLOUDERA_HADOOP_ROOT = "CLOUDERA_HADOOP_ROOT"
ENV_HADOOP_DEV_DIR = "HADOOP_DEV_DIR"
YARN_JIRA_ID_PATTERN = re.compile(r"(YARN-\d+)")
ANY_JIRA_ID_PATTERN = re.compile(r"([A-Z]+-\d+)")

# Symlink names
LATEST_DATA_ZIP_LINK_NAME = "latest-command-data-zip"


class SummaryFile:
    TXT = "summary.txt"
    HTML = "summary.html"


class ReportFile:
    SHORT_TXT = "report-short.txt"
    DETAILED_TXT = "report-detailed.txt"
    SHORT_HTML = "report-short.html"
    DETAILED_HTML = "report-detailed.html"


# Do not leak bad ENV variable namings into the python code
LOADED_ENV_UPSTREAM_DIR = "upstream-hadoop-dir"
LOADED_ENV_DOWNSTREAM_DIR = "downstream-hadoop-dir"
DEST_DIR_PREFIX = "test"
HADOOP_REPO_TEMPLATE = "https://github.com/{user}/hadoop.git"
HADOOP_REPO_APACHE = HADOOP_REPO_TEMPLATE.format(user=APACHE)

# Patch constants
YARN_PATCH_FILENAME_REGEX = ".*(YARN-[0-9]+).*\\.patch"
PATCH_FILE_REGEX = "\\.\\d.*\\.patch$"
PATCH_EXTENSION = ".patch"
FIRST_PATCH_NUMBER = "001"

# Other constants
CLOUDERA_CDH_HADOOP_COMMIT_LINK_PREFIX = "https://github.infra.cloudera.com/CDH/hadoop/commit/"
