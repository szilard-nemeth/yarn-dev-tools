from enum import Enum

UNIT_TEST_RESULT_AGGREGATOR_DIR_NAME = "unit-test-result-aggregator"
REVIEW_SHEET_BACKPORT_UPDATER_DIR_NAME = "review-sheet-backport-updater"
REVIEWSYNC_DIR_NAME = "reviewsync"
UNIT_TEST_RESULT_REPORTING_DIR_NAME = "unit-test-result-reporting"
PROJECT_NAME = "cdsw"
INSTALL_REQUIREMENTS_SCRIPT = "install-requirements.sh"
CDSW_RUNNER_PY = "cdsw_runner.py"


# TODO Add default value of all env vars to enum
# TODO Move all EnvVar classes to commands?
class CdswEnvVar(Enum):
    MAIL_ACC_PASSWORD = "MAIL_ACC_PASSWORD"
    MAIL_ACC_USER = "MAIL_ACC_USER"
    MAIL_RECIPIENTS = "MAIL_RECIPIENTS"
    CLOUDERA_HADOOP_ROOT = "CLOUDERA_HADOOP_ROOT"
    HADOOP_DEV_DIR = "HADOOP_DEV_DIR"
    PYTHONPATH = "PYTHONPATH"
    TEST_EXECUTION_MODE = "TEST_EXEC_MODE"
    PYTHON_MODULE_MODE = "PYTHON_MODULE_MODE"
    ENABLE_GOOGLE_DRIVE_INTEGRATION = "ENABLE_GOOGLE_DRIVE_INTEGRATION"
    INSTALL_REQUIREMENTS = "INSTALL_REQUIREMENTS"
    RESTART_PROCESS_WHEN_REQUIREMENTS_INSTALLED = "RESTART_PROCESS_WHEN_REQUIREMENTS_INSTALLED"


class BranchComparatorEnvVar(Enum):
    FEATURE_BRANCH = "BRANCH_COMP_FEATURE_BRANCH"
    MASTER_BRANCH = "BRANCH_COMP_MASTER_BRANCH"
    REPO_TYPE = "BRANCH_COMP_REPO_TYPE"


class JiraUmbrellaCheckerEnvVar(Enum):
    UMBRELLA_IDS = "UMBRELLA_IDS"  # TODO use this env var class


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


class UnitTestResultAggregatorOptionalEnvVar(Enum):
    ABBREV_TC_PACKAGE = "ABBREV_TC_PACKAGE"
    AGGREGATE_FILTERS = "AGGREGATE_FILTERS"
    SKIP_AGGREGATION_RESOURCE_FILE = "SKIP_AGGREGATION_RESOURCE_FILE"
    SKIP_AGGREGATION_RESOURCE_FILE_AUTO_DISCOVERY = "SKIP_AGGREGATION_RESOURCE_FILE_AUTO_DISCOVERY"
    GSHEET_COMPARE_WITH_JIRA_TABLE = "GSHEET_COMPARE_WITH_JIRA_TABLE"


class JenkinsTestReporterEnvVar(Enum):
    BUILD_PROCESSING_LIMIT = "BUILD_PROCESSING_LIMIT"
    FORCE_SENDING_MAIL = "FORCE_SENDING_MAIL"
    RESET_JOB_BUILD_DATA = "RESET_JOB_BUILD_DATA"
