import dataclasses
import inspect
import logging
import os
import site
import sys
from abc import ABC, abstractmethod
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
from pythoncommons.date_utils import DateUtils
from pythoncommons.file_utils import FileUtils
from pythoncommons.logging_setup import SimpleLoggingSetup, SimpleLoggingSetupConfig
from pythoncommons.os_utils import OsUtils
from pythoncommons.process import SubprocessCommandRunner
from pythoncommons.project_utils import (
    ProjectUtils,
    ProjectRootDeterminationStrategy,
    PROJECTS_BASEDIR,
    PROJECTS_BASEDIR_NAME,
)

from tests.cdsw.common.testutils.cdsw_testing_common import SECRET_PROJECTS_DIR
from yarndevtools.cdsw.common_python.constants import (
    CdswEnvVar,
    PROJECT_NAME,
)

# Constants
# TODO Move this to EnvVar enum
from yarndevtools.cdsw.common_python.restarter import Restarter
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME

# MAKE SURE THIS PRECEDES IMPORT TO pythoncommons

CDSW_PROJECT = "cdsw"


class TestExecMode(Enum):
    CLOUDERA = "cloudera"
    UPSTREAM = "upstream"


DEFAULT_TEST_EXECUTION_MODE = TestExecMode.CLOUDERA.value

ENV_OVERRIDE_SCRIPT_BASEDIR = "OVERRIDE_SCRIPT_BASEDIR"
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
    # TODO seems unused
    CDSW_SCRIPT_DIR_NAMES: List[str] = [
        CommandType.BRANCH_COMPARATOR.output_dir_name,
        CommandType.JIRA_UMBRELLA_DATA_FETCHER.output_dir_name,
        CommandType.UNIT_TEST_RESULT_AGGREGATOR.output_dir_name,
        CommandType.UNIT_TEST_RESULT_FETCHER.output_dir_name,
        CommandType.REVIEW_SHEET_BACKPORT_UPDATER.output_dir_name,
        CommandType.REVIEWSYNC.output_dir_name,
    ]


class CommonFiles:
    YARN_DEV_TOOLS_SCRIPT = None


class PythonModuleMode(Enum):
    USER = "user"
    GLOBAL = "global"


@dataclasses.dataclass
class CdswSetupResult:
    basedir: str
    env_vars: Dict[str, str]


class CdswSetup:
    @staticmethod
    def initial_setup(env_var_dict: Dict[str, str] = None, mandatory_env_vars: List[str] = None):
        ProjectUtils.set_root_determine_strategy(ProjectRootDeterminationStrategy.SYS_PATH)
        ProjectUtils.get_output_basedir(YARNDEVTOOLS_MODULE_NAME, basedir=PROJECTS_BASEDIR)
        # TODO sanity_check_number_of_handlers should be set to True
        logging_config: SimpleLoggingSetupConfig = SimpleLoggingSetup.init_logger(
            project_name=PROJECT_NAME,
            logger_name_prefix=YARNDEVTOOLS_MODULE_NAME,
            execution_mode=ExecutionMode.PRODUCTION,
            console_debug=True,
            sanity_check_number_of_handlers=False,
        )
        LOG.info("Logging to files: %s", logging_config.log_file_paths)
        LOG.info(f"Python version info: {sys.version}")
        if not env_var_dict:
            env_var_dict = {}
        if not mandatory_env_vars:
            mandatory_env_vars = []

        if mandatory_env_vars:
            LOG.info(f"Printing env vars: {os.environ}")

        env_var_dict.update(
            {
                CdswEnvVar.CLOUDERA_HADOOP_ROOT.value: CommonDirs.HADOOP_CLOUDERA_BASEDIR,
                CdswEnvVar.HADOOP_DEV_DIR.value: CommonDirs.HADOOP_UPSTREAM_BASEDIR,
            }
        )

        CdswSetup.prepare_env_vars(env_var_dict=env_var_dict, mandatory_env_vars=mandatory_env_vars)
        # TODO Migrate this to CdswEnvVar
        if ENV_OVERRIDE_SCRIPT_BASEDIR in os.environ:
            basedir = OsUtils.get_env_value(ENV_OVERRIDE_SCRIPT_BASEDIR)
        else:
            basedir = CommonDirs.YARN_DEV_TOOLS_SCRIPTS_BASEDIR

        # This must happen before other operations as it sets: CommonDirs.YARN_DEV_TOOLS_MODULE_ROOT
        CdswSetup._setup_python_module_root_and_yarndevtools_path()
        LOG.info("Using basedir for scripts: " + basedir)
        return CdswSetupResult(basedir, env_var_dict)

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

    @staticmethod
    def prepare_env_vars(env_var_dict: Dict[str, str] = None, mandatory_env_vars: List[str] = None):
        for k, v in env_var_dict.items():
            OsUtils.set_env_value(k, v)

        for env_var in mandatory_env_vars:
            if env_var not in os.environ:
                raise ValueError(f"{env_var} is not set. Please set it to a valid value!")


class CdswRunnerBase(ABC):
    def __init__(self, dry_run: bool = False):
        self.executed_commands = []
        self.cdsw_runner_script_path = None
        self.start_date_str = None
        self.common_mail_config = CommonMailConfig()
        self._setup_google_drive()

        # Dynamic
        self.dry_run = dry_run

    def _setup_google_drive(self):
        if OsUtils.is_env_var_true(CdswEnvVar.ENABLE_GOOGLE_DRIVE_INTEGRATION.value, default_val=True):
            self.drive_cdsw_helper = GoogleDriveCdswHelper()
        else:
            self.drive_cdsw_helper = None

    @property
    def is_drive_integration_enabled(self):
        return self.drive_cdsw_helper is not None

    def start_common(self, setup_result: CdswSetupResult, cdsw_runner_script_path: str):
        LOG.info("Starting CDSW runner...")
        LOG.info("Setup result: %s", setup_result)
        self.cdsw_runner_script_path = cdsw_runner_script_path
        self.start_date_str = self.current_date_formatted()
        if OsUtils.is_env_var_true(CdswEnvVar.RESTART_PROCESS_WHEN_REQUIREMENTS_INSTALLED.value, default_val=False):
            Restarter.restart_execution(self.cdsw_runner_script_path)

    @abstractmethod
    def start(self, basedir, cdsw_runner_script_path: str):
        pass

    def run_clone_downstream_repos_script(self, basedir):
        script = os.path.join(basedir, "clone_downstream_repos.sh")
        cmd = f"{BASHX} {script}"
        self._run_command(cmd)

    def run_clone_upstream_repos_script(self, basedir):
        script = os.path.join(basedir, "clone_upstream_repos.sh")
        cmd = f"{BASHX} {script}"
        self._run_command(cmd)

    def execute_yarndevtools_script(self, script_args):
        cmd = f"{PY3} {CommonFiles.YARN_DEV_TOOLS_SCRIPT} {script_args}"
        self._run_command(cmd)

    def _run_command(self, cmd):
        self.executed_commands.append(cmd)
        if self.dry_run:
            LOG.info("[DRY-RUN] Would run command: %s", cmd)
        else:
            SubprocessCommandRunner.run_and_follow_stdout_stderr(
                cmd, stdout_logger=CMD_LOG, exit_on_nonzero_exitcode=True
            )

    @staticmethod
    def current_date_formatted():
        return DateUtils.get_current_datetime()

    def run_zipper(self, command_type: CommandType, debug=False, ignore_filetypes: str = "java js"):
        debug_mode = "--debug" if debug else ""
        self.execute_yarndevtools_script(
            f"{debug_mode} "
            f"{CommandType.ZIP_LATEST_COMMAND_DATA.name} {command_type.name} "
            f"--dest_dir /tmp "
            f"--ignore-filetypes {ignore_filetypes}"
        )

    def upload_command_data_to_drive(self, cmd_type: CommandType, drive_filename: str) -> DriveApiFile:
        output_basedir = ProjectUtils.get_output_basedir(YARNDEVTOOLS_MODULE_NAME)
        full_file_path_of_cmd_data = FileUtils.join_path(output_basedir, cmd_type.command_data_zip_name)
        return self.drive_cdsw_helper.upload(cmd_type, full_file_path_of_cmd_data, drive_filename)

    def send_latest_command_data_in_email(
        self,
        sender,
        subject,
        recipients=None,
        attachment_filename=None,
        email_body_file: str = None,
        prepend_text_to_email_body: str = None,
        send_attachment: bool = True,
    ):
        if not recipients:
            recipients = self.determine_recipients()
        attachment_filename_val = f"{attachment_filename}" if attachment_filename else ""
        email_body_file_param = f"--file-as-email-body-from-zip {email_body_file}" if email_body_file else ""
        email_body_prepend_param = (
            f"--prepend_email_body_with_text '{prepend_text_to_email_body}'" if prepend_text_to_email_body else ""
        )
        send_attachment_param = "--send-attachment" if send_attachment else ""
        self.execute_yarndevtools_script(
            f"--debug {CommandType.SEND_LATEST_COMMAND_DATA.name} "
            f"{self.common_mail_config.as_arguments()}"
            f'--subject "{subject}" '
            f'--sender "{sender}" '
            f'--recipients "{recipients}" '
            f"--attachment-filename {attachment_filename_val} "
            f"{email_body_file_param} "
            f"{email_body_prepend_param} "
            f"{send_attachment_param}"
        )

    @staticmethod
    def determine_recipients(default_recipients=MAIL_ADDR_YARN_ENG_BP):
        recipients_env = OsUtils.get_env_value(CdswEnvVar.MAIL_RECIPIENTS.value)
        if recipients_env:
            return recipients_env
        return default_recipients

    @staticmethod
    def get_filename(dir_name: str):
        # Apparently, there is no chance to get the stackframe that called this method.
        # The 0th frame holds this method, though.
        # See file: cdsw_stacktrace_example.txt
        # Let's put the path together by hand
        stack = inspect.stack()
        LOG.debug("Discovered stack while getting filename: %s", stack)
        file_path = stack[0].filename
        rindex = file_path.rindex("cdsw" + os.sep)
        script_abs_path = file_path[:rindex] + f"cdsw{os.sep}{dir_name}{os.sep}cdsw_runner.py"
        if not os.path.exists(script_abs_path):
            raise ValueError(
                "Script should have existed under path: {}. "
                "Please double-check the code that assembles the path!".format(script_abs_path)
            )
        return script_abs_path


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
        self.authorizer = GoogleApiAuthorizer(
            ServiceType.DRIVE,
            project_name=CDSW_PROJECT,
            secret_basedir=SECRET_PROJECTS_DIR,
            account_email="snemeth@cloudera.com",
            scopes=[DriveApiScope.DRIVE_PER_FILE_ACCESS.value],
        )
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
