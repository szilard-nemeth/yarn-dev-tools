from enum import Enum
from os.path import expanduser

from pythoncommons.file_utils import FileUtils

from yarndevtools.common.shared_command_utils import YarnDevToolsEnvVar

PROJECT_NAME = "cdsw"
INSTALL_REQUIREMENTS_SCRIPT = "install-requirements.sh"
SECRET_PROJECTS_DIR = FileUtils.join_path(expanduser("~"), ".secret", "projects", "cloudera")


# TODO Add default value of all env vars to enum
# TODO Move all EnvVar classes to commands?
class CdswEnvVar(Enum):
    MAIL_ACC_PASSWORD = "MAIL_ACC_PASSWORD"
    MAIL_ACC_USER = "MAIL_ACC_USER"
    MAIL_RECIPIENTS = "MAIL_RECIPIENTS"
    JENKINS_USER = "JENKINS_USER"
    JENKINS_PASSWORD = "JENKINS_PASSWORD"
    CLOUDERA_HADOOP_ROOT = YarnDevToolsEnvVar.ENV_CLOUDERA_HADOOP_ROOT.value
    HADOOP_DEV_DIR = YarnDevToolsEnvVar.ENV_HADOOP_DEV_DIR.value
    PYTHONPATH = "PYTHONPATH"
    TEST_EXECUTION_MODE = "TEST_EXEC_MODE"
    PYTHON_MODULE_MODE = "PYTHON_MODULE_MODE"
    ENABLE_GOOGLE_DRIVE_INTEGRATION = "ENABLE_GOOGLE_DRIVE_INTEGRATION"
    INSTALL_REQUIREMENTS = "INSTALL_REQUIREMENTS"
    RESTART_PROCESS_WHEN_REQUIREMENTS_INSTALLED = "RESTART_PROCESS_WHEN_REQUIREMENTS_INSTALLED"
    DEBUG_ENABLED = "DEBUG_ENABLED"
    OVERRIDE_SCRIPT_BASEDIR = "OVERRIDE_SCRIPT_BASEDIR"
    ENABLE_LOGGER_HANDLER_SANITY_CHECK = "ENABLE_LOGGER_HANDLER_SANITY_CHECK"
    REMOVE_COMMAND_DATA_FILES = "REMOVE_COMMAND_DATA_FILES"
    YARNDEVTOOLS_VERSION = "YARNDEVTOOLS_VERSION"


class BranchComparatorEnvVar(Enum):
    BRANCH_COMP_FEATURE_BRANCH = "BRANCH_COMP_FEATURE_BRANCH"
    BRANCH_COMP_MASTER_BRANCH = "BRANCH_COMP_MASTER_BRANCH"
    BRANCH_COMP_REPO_TYPE = "BRANCH_COMP_REPO_TYPE"


class JiraUmbrellaFetcherEnvVar(Enum):
    UMBRELLA_IDS = "UMBRELLA_IDS"


class ReviewSheetBackportUpdaterEnvVar(Enum):
    GSHEET_CLIENT_SECRET = "GSHEET_CLIENT_SECRET"
    GSHEET_SPREADSHEET = "GSHEET_SPREADSHEET"
    GSHEET_WORKSHEET = "GSHEET_WORKSHEET"
    GSHEET_JIRA_COLUMN = "GSHEET_JIRA_COLUMN"
    GSHEET_UPDATE_DATE_COLUMN = "GSHEET_UPDATE_DATE_COLUMN"
    GSHEET_STATUS_INFO_COLUMN = "GSHEET_STATUS_INFO_COLUMN"
    BRANCHES = "BRANCHES"


class ReviewSyncEnvVar(Enum):
    GSHEET_CLIENT_SECRET = "GSHEET_CLIENT_SECRET"
    GSHEET_SPREADSHEET = "GSHEET_SPREADSHEET"
    GSHEET_WORKSHEET = "GSHEET_WORKSHEET"
    GSHEET_JIRA_COLUMN = "GSHEET_JIRA_COLUMN"
    GSHEET_UPDATE_DATE_COLUMN = "GSHEET_UPDATE_DATE_COLUMN"
    GSHEET_STATUS_INFO_COLUMN = "GSHEET_STATUS_INFO_COLUMN"
    BRANCHES = "BRANCHES"


class UnitTestResultAggregatorEnvVar(Enum):
    GSHEET_CLIENT_SECRET = "GSHEET_CLIENT_SECRET"
    GSHEET_SPREADSHEET = "GSHEET_SPREADSHEET"
    GSHEET_WORKSHEET = "GSHEET_WORKSHEET"
    REQUEST_LIMIT = "REQUEST_LIMIT"
    MATCH_EXPRESSION = "MATCH_EXPRESSION"

    # OPTIONALS
    ABBREV_TC_PACKAGE = "ABBREV_TC_PACKAGE"
    AGGREGATE_FILTERS = "AGGREGATE_FILTERS"
    SKIP_AGGREGATION_RESOURCE_FILE = "SKIP_AGGREGATION_RESOURCE_FILE"
    SKIP_AGGREGATION_RESOURCE_FILE_AUTO_DISCOVERY = "SKIP_AGGREGATION_RESOURCE_FILE_AUTO_DISCOVERY"
    GSHEET_COMPARE_WITH_JIRA_TABLE = "GSHEET_COMPARE_WITH_JIRA_TABLE"


class UnitTestResultFetcherEnvVar(Enum):
    BUILD_PROCESSING_LIMIT = "BUILD_PROCESSING_LIMIT"
    FORCE_SENDING_MAIL = "FORCE_SENDING_MAIL"
    RESET_JOB_BUILD_DATA = "RESET_JOB_BUILD_DATA"
