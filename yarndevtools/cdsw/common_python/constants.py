from enum import Enum

BRANCH_DIFF_REPORTER_DIR_NAME = "daily-downstream-branchdiff-reporting"


class CdswEnvVar(Enum):
    MAIL_ACC_PASSWORD = "MAIL_ACC_PASSWORD"
    MAIL_ACC_USER = "MAIL_ACC_USER"
    CLOUDERA_HADOOP_ROOT = "CLOUDERA_HADOOP_ROOT"
    HADOOP_DEV_DIR = "HADOOP_DEV_DIR"
    PYTHONPATH = "PYTHONPATH"
    TEST_EXECUTION_MODE = "TEST_EXEC_MODE"


class BranchComparatorEnvVar(Enum):
    FEATURE_BRANCH = "BRANCH_COMP_FEATURE_BRANCH"
    MASTER_BRANCH = "BRANCH_COMP_MASTER_BRANCH"
    REPO_TYPE = "BRANCH_COMP_REPO_TYPE"


class JenkinsTestReporterEnvVar(Enum):
    BUILD_PROCESSING_LIMIT = "BUILD_PROCESSING_LIMIT"
