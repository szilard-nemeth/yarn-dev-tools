import dataclasses
import logging
import os
import site
import sys
from enum import Enum
from typing import Dict, List

# https://stackoverflow.com/a/50255019/1106893
from googleapiwrapper.common import ServiceType
from googleapiwrapper.google_auth import GoogleApiAuthorizer
from googleapiwrapper.google_drive import (
    DriveApiWrapper,
    DriveApiWrapperSessionSettings,
    FileFindMode,
    DuplicateFileWriteResolutionMode,
    DriveApiScope,
    DriveApiFile,
)
from pythoncommons.constants import ExecutionMode
from pythoncommons.file_utils import FileUtils
from pythoncommons.jira_utils import JiraUtils
from pythoncommons.logging_setup import SimpleLoggingSetup, SimpleLoggingSetupConfig
from pythoncommons.object_utils import ObjUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import (
    ProjectUtils,
    ProjectRootDeterminationStrategy,
    PROJECTS_BASEDIR,
    PROJECTS_BASEDIR_NAME,
)
from yarndevtools.cdsw.constants import (
    CdswEnvVar,
    PROJECT_NAME,
    UnitTestResultAggregatorEmailEnvVar,
    SECRET_PROJECTS_DIR,
)

from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME, UPSTREAM_JIRA_BASE_URL

# MAKE SURE THIS PRECEDES IMPORT TO pythoncommons

CDSW_PROJECT = "cdsw"


class TestExecMode(Enum):
    CLOUDERA = "cloudera"
    UPSTREAM = "upstream"


DEFAULT_TEST_EXECUTION_MODE = TestExecMode.CLOUDERA.value


SKIP_AGGREGATION_DEFAULTS_FILENAME = "skip_aggregation_defaults.txt"
LOG = logging.getLogger(__name__)
CMD_LOG = SimpleLoggingSetup.create_command_logger(__name__)
BASEDIR = None
PY3 = "python3"
BASH = "bash"
BASHX = "bash -x"
MAIL_ADDR_YARN_ENG_BP = "yarn_eng_bp@cloudera.com"
MAIL_ADDR_SNEMETH = "snemeth@cloudera.com"


class CommonDirs:
    CDSW_BASEDIR = FileUtils.join_path("home", "cdsw")
    YARN_DEV_TOOLS_SCRIPTS_BASEDIR = FileUtils.join_path(CDSW_BASEDIR, "scripts")
    YARN_DEV_TOOLS_JOBS_BASEDIR = FileUtils.join_path(CDSW_BASEDIR, "jobs")
    HADOOP_UPSTREAM_BASEDIR = FileUtils.join_path(CDSW_BASEDIR, "repos", "apache", "hadoop")
    HADOOP_CLOUDERA_BASEDIR = FileUtils.join_path(CDSW_BASEDIR, "repos", "cloudera", "hadoop")
    USER_DEV_ROOT = FileUtils.join_path("/", "Users", "snemeth", "development")
    YARN_DEV_TOOLS_MODULE_ROOT = None


class CommonFiles:
    YARN_DEV_TOOLS_SCRIPT = None


class PythonModuleMode(Enum):
    USER = "user"
    GLOBAL = "global"


@dataclasses.dataclass
class CdswSetupResult:
    basedir: str
    output_basedir: str
    env_vars: Dict[str, str]


class CdswSetup:
    @staticmethod
    def initial_setup(env_var_dict: Dict[str, str] = None):
        enable_handler_sanity_check = OsUtils.is_env_var_true(
            CdswEnvVar.ENABLE_LOGGER_HANDLER_SANITY_CHECK.value, default_val=True
        )

        ProjectUtils.set_root_determine_strategy(ProjectRootDeterminationStrategy.SYS_PATH, allow_overwrite=False)
        output_basedir = ProjectUtils.get_output_basedir(
            YARNDEVTOOLS_MODULE_NAME, basedir=PROJECTS_BASEDIR, project_name_hint=YARNDEVTOOLS_MODULE_NAME
        )
        logging_config: SimpleLoggingSetupConfig = SimpleLoggingSetup.init_logger(
            project_name=PROJECT_NAME,
            logger_name_prefix=YARNDEVTOOLS_MODULE_NAME,
            execution_mode=ExecutionMode.PRODUCTION,
            console_debug=True,
            sanity_check_number_of_handlers=enable_handler_sanity_check,
        )
        LOG.info("Logging to files: %s", logging_config.log_file_paths)
        LOG.info(f"Python version info: {sys.version}")
        env_var_dict = CdswSetup._prepare_env_vars(env_var_dict)
        basedir = CdswSetup._determine_basedir()

        # This must happen before other operations as it sets: CommonDirs.YARN_DEV_TOOLS_MODULE_ROOT
        CdswSetup._setup_python_module_root_and_yarndevtools_path()
        LOG.info("Using basedir for scripts: %s", basedir)
        LOG.debug("Common dirs after setup: %s", ObjUtils.get_class_members(CommonDirs))
        LOG.debug("Common files after setup: %s", ObjUtils.get_class_members(CommonFiles))
        return CdswSetupResult(basedir, output_basedir, env_var_dict)

    @staticmethod
    def _determine_basedir():
        if CdswEnvVar.OVERRIDE_SCRIPT_BASEDIR.value in os.environ:
            basedir = OsUtils.get_env_value(CdswEnvVar.OVERRIDE_SCRIPT_BASEDIR.value)
        else:
            basedir = CommonDirs.YARN_DEV_TOOLS_SCRIPTS_BASEDIR
        return basedir

    @staticmethod
    def _prepare_env_vars(env_var_dict):
        if not env_var_dict:
            env_var_dict = {}
        env_var_dict.update(
            {
                CdswEnvVar.CLOUDERA_HADOOP_ROOT.value: CommonDirs.HADOOP_CLOUDERA_BASEDIR,
                CdswEnvVar.HADOOP_DEV_DIR.value: CommonDirs.HADOOP_UPSTREAM_BASEDIR,
            }
        )
        for k, v in env_var_dict.items():
            OsUtils.set_env_value(k, v)
        return env_var_dict

    @staticmethod
    def _setup_python_module_root_and_yarndevtools_path():
        # For CDSW execution, user python module mode is preferred.
        # For test execution, it depends on how the initial-cdsw-setup.sh script was executed in the container.
        env_value = OsUtils.get_env_value(CdswEnvVar.PYTHON_MODULE_MODE.value, PythonModuleMode.USER.value)
        python_module_mode = PythonModuleMode[env_value.upper()]

        LOG.info("Using Python module mode: %s", python_module_mode.value)
        if python_module_mode == PythonModuleMode.GLOBAL:
            python_site = site.getsitepackages()[0]
            LOG.info("Using global python-site basedir: %s", python_site)
        elif python_module_mode == PythonModuleMode.USER:
            python_site = site.USER_SITE
            LOG.info("Using user python-site basedir: %s", python_site)
        else:
            raise ValueError("Invalid python module mode: {}".format(python_module_mode))
        CommonDirs.YARN_DEV_TOOLS_MODULE_ROOT = FileUtils.join_path(python_site, YARNDEVTOOLS_MODULE_NAME)
        CommonFiles.YARN_DEV_TOOLS_SCRIPT = os.path.join(CommonDirs.YARN_DEV_TOOLS_MODULE_ROOT, "yarn_dev_tools.py")


class CommonMailConfig:
    def __init__(self):
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 465
        self.account_user = OsUtils.get_env_value(CdswEnvVar.MAIL_ACC_USER.value)
        self.account_password = OsUtils.get_env_value(CdswEnvVar.MAIL_ACC_PASSWORD.value)

    def as_arguments(self):
        return (
            f'--smtp_server "{self.smtp_server}" '
            f"--smtp_port {self.smtp_port} "
            f'--account_user "{self.account_user}" '
            f'--account_password "{self.account_password}" '
        )


class GoogleDriveCdswHelper:
    def __init__(self):
        self.authorizer = self.create_authorizer()
        session_settings = DriveApiWrapperSessionSettings(
            FileFindMode.JUST_UNTRASHED, DuplicateFileWriteResolutionMode.FAIL_FAST, enable_path_cache=True
        )
        self.drive_wrapper = DriveApiWrapper(self.authorizer, session_settings=session_settings)
        self.drive_command_data_basedir = FileUtils.join_path(
            PROJECTS_BASEDIR_NAME, YARNDEVTOOLS_MODULE_NAME, CDSW_PROJECT, "command-data"
        )

    def upload(self, cmd_type: CommandType, local_file_path: str, drive_filename: str) -> DriveApiFile:
        drive_path = FileUtils.join_path(self.drive_command_data_basedir, cmd_type.real_name, drive_filename)
        drive_api_file: DriveApiFile = self.drive_wrapper.upload_file(local_file_path, drive_path)
        return drive_api_file

    def create_authorizer(self):
        return GoogleApiAuthorizer(
            ServiceType.DRIVE,
            project_name=CDSW_PROJECT,
            secret_basedir=SECRET_PROJECTS_DIR,
            account_email="snemeth@cloudera.com",
            scopes=[DriveApiScope.DRIVE_PER_FILE_ACCESS.value],
        )


class GenericCdswConfigUtils:
    @staticmethod
    def quote_list_items(lst):
        return " ".join(f'"{w}"' for w in lst)

    @staticmethod
    def quote(val):
        if '"' in val:
            return val
        return '"' + val + '"'

    @staticmethod
    def unquote(val):
        return val.strip('"')


class JiraUmbrellaDataFetcherCdswUtils:
    @staticmethod
    def fetch_umbrella_titles(jira_ids: List[str]) -> Dict[str, str]:
        return {j_id: JiraUmbrellaDataFetcherCdswUtils._fetch_umbrella_title(j_id) for j_id in jira_ids}

    @staticmethod
    def _fetch_umbrella_title(jira_id: str):
        jira_html_file = f"/tmp/jira_{jira_id}.html"
        LOG.info("Fetching HTML of jira: %s", jira_id)
        jira_html = JiraUtils.download_jira_html(UPSTREAM_JIRA_BASE_URL, jira_id, jira_html_file)
        return JiraUtils.parse_jira_title(jira_html)


class UnitTestResultAggregatorCdswUtils:
    DEFAULT_SKIP_LINES_STARTING_WITH = ["Failed testcases:", "Failed testcases (", "FILTER:", "Filter expression: "]

    @classmethod
    def determine_lines_to_skip(cls) -> List[str]:
        skip_lines_starting_with: List[str] = cls.DEFAULT_SKIP_LINES_STARTING_WITH
        # If env var "SKIP_AGGREGATION_RESOURCE_FILE" is specified, try to read file
        # The file takes precedence over the default list of DEFAULT_SKIP_LINES_STARTING_WITH
        skip_aggregation_res_file = OsUtils.get_env_value(
            UnitTestResultAggregatorEmailEnvVar.SKIP_AGGREGATION_RESOURCE_FILE.value
        )
        skip_aggregation_res_file_auto_discovery_str = OsUtils.get_env_value(
            UnitTestResultAggregatorEmailEnvVar.SKIP_AGGREGATION_RESOURCE_FILE_AUTO_DISCOVERY.value
        )
        LOG.info(
            "Value of env var '%s': %s",
            UnitTestResultAggregatorEmailEnvVar.SKIP_AGGREGATION_RESOURCE_FILE_AUTO_DISCOVERY.value,
            skip_aggregation_res_file_auto_discovery_str,
        )

        # TODO Bool parsing should be done in get_env_value
        if skip_aggregation_res_file_auto_discovery_str in ("True", "true", "1"):
            skip_aggregation_res_file_auto_discovery = True
        elif skip_aggregation_res_file_auto_discovery_str in ("False", "false", "0"):
            skip_aggregation_res_file_auto_discovery = False
        else:
            raise ValueError(
                "Invalid value for environment variable '{}': {}".format(
                    UnitTestResultAggregatorEmailEnvVar.SKIP_AGGREGATION_RESOURCE_FILE_AUTO_DISCOVERY.value,
                    skip_aggregation_res_file_auto_discovery_str,
                )
            )

        if skip_aggregation_res_file_auto_discovery:
            found_with_auto_discovery = cls._auto_discover_skip_aggregation_result_file()
            if found_with_auto_discovery:
                LOG.info("Found Skip aggregation resource file with auto-discovery: %s", found_with_auto_discovery)
                return FileUtils.read_file_to_list(found_with_auto_discovery)
        elif skip_aggregation_res_file:
            LOG.info("Trying to check specified skip aggregation resource file: %s", skip_aggregation_res_file)
            FileUtils.ensure_is_file(skip_aggregation_res_file)
            return FileUtils.read_file_to_list(skip_aggregation_res_file)
        return skip_lines_starting_with

    @classmethod
    def _auto_discover_skip_aggregation_result_file(cls):
        found_with_auto_discovery: str or None = None
        search_basedir = CommonDirs.YARN_DEV_TOOLS_MODULE_ROOT
        LOG.info("Looking for file '%s' in basedir: %s", SKIP_AGGREGATION_DEFAULTS_FILENAME, search_basedir)
        results = FileUtils.search_files(search_basedir, SKIP_AGGREGATION_DEFAULTS_FILENAME)
        if not results:
            LOG.warning(
                "Skip aggregation resource file auto-discovery is enabled, "
                "but failed to find file '%s' from base directory '%s'.",
                SKIP_AGGREGATION_DEFAULTS_FILENAME,
                search_basedir,
            )
        elif len(results) > 1:
            LOG.warning(
                "Skip aggregation resource file auto-discovery is enabled, "
                "but multiple files found from base directory '%s'. Found files: %s",
                SKIP_AGGREGATION_DEFAULTS_FILENAME,
                search_basedir,
                results,
            )
        else:
            found_with_auto_discovery = results[0]
        return found_with_auto_discovery
