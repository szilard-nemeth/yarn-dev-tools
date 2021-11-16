import sys
from abc import ABC, abstractmethod
import logging
import os
import site
from enum import Enum
from typing import Dict, List

# MAKE SURE THIS PRECEDES IMPORT TO pythoncommons


# https://stackoverflow.com/a/50255019/1106893
from pythoncommons.constants import ExecutionMode
from pythoncommons.date_utils import DateUtils
from pythoncommons.file_utils import FileUtils, FindResultType
from pythoncommons.logging_setup import SimpleLoggingSetup, SimpleLoggingSetupConfig
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils, ProjectRootDeterminationStrategy, PROJECTS_BASEDIR

from yarndevtools.argparser import CommandType
from yarndevtools.cdsw.common_python.constants import (
    CdswEnvVar,
    PROJECT_NAME,
    INSTALL_REQUIREMENTS_SCRIPT,
    CDSW_RUNNER_PY,
)

from pythoncommons.process import SubprocessCommandRunner

# Constants
# Move this to EnvVar enum
from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME

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
    CDSW_SCRIPT_DIR_NAMES: List[str] = [
        "downstream-branchdiff-reporting",
        "jira-umbrella-checker",
        "unit-test-result-aggregator",
        "unit-test-result-reporting",
    ]


class CommonFiles:
    YARN_DEV_TOOLS_SCRIPT = None


class PythonModuleMode(Enum):
    USER = "user"
    GLOBAL = "global"


class CdswSetup:
    @staticmethod
    def fix_pythonpath(additional_dir):
        pypath = CdswEnvVar.PYTHONPATH.value
        if pypath in os.environ:
            LOG.debug(f"Old {pypath}: {CdswSetup._get_pythonpath()}")
            OsUtils.set_env_value(pypath, f"{CdswSetup._get_pythonpath()}:{additional_dir}")
            LOG.debug(f"New {pypath}: {CdswSetup._get_pythonpath()}")
        else:
            LOG.debug(f"Old {pypath}: not set")
            OsUtils.set_env_value(pypath, additional_dir)
            LOG.debug(f"New {pypath}: {CdswSetup._get_pythonpath}")

    @staticmethod
    def _get_pythonpath():
        return OsUtils.get_env_value(CdswEnvVar.PYTHONPATH.value)

    @staticmethod
    def initial_setup(env_var_dict: Dict[str, str] = None, mandatory_env_vars: List[str] = None):
        print("***TESTPRINT")
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
        if ENV_OVERRIDE_SCRIPT_BASEDIR in os.environ:
            basedir = OsUtils.get_env_value(ENV_OVERRIDE_SCRIPT_BASEDIR)
        else:
            basedir = CommonDirs.YARN_DEV_TOOLS_SCRIPTS_BASEDIR

        CdswSetup._setup_python_module_root_and_yarndevtools_path()
        CdswSetup._run_install_requirements_script()
        CdswSetup._relink_cdsw_jobs_to_yarndevtools_cdsw_runner_scripts()
        LOG.info("Using basedir for scripts: " + basedir)
        return basedir

    @staticmethod
    def _run_install_requirements_script(exit_on_nonzero_exitcode=False):
        """
        Do not exit on non-zero exit code as pip can fail to remove residual package files on NFS.
        See: https://github.com/pypa/pip/issues/6327
        :param exit_on_nonzero_exitcode:
        :return:
        """
        results = FileUtils.search_files(CommonDirs.YARN_DEV_TOOLS_MODULE_ROOT, INSTALL_REQUIREMENTS_SCRIPT)
        if not results:
            raise ValueError(
                "Expected to find file: {} from basedir: {}".format(
                    INSTALL_REQUIREMENTS_SCRIPT, CommonDirs.YARN_DEV_TOOLS_MODULE_ROOT
                )
            )
        script = results[0]
        exec_mode = OsUtils.get_env_value(CdswEnvVar.TEST_EXECUTION_MODE.value, "upstream")
        cmd = f"{BASHX} {script} {exec_mode}"
        SubprocessCommandRunner.run_and_follow_stdout_stderr(
            cmd, stdout_logger=CMD_LOG, exit_on_nonzero_exitcode=exit_on_nonzero_exitcode
        )

    @staticmethod
    def _setup_python_module_root_and_yarndevtools_path():
        # For CDSW, user python module mode is preferred.
        # For tests, it depends on how the initial-cdsw-setup.sh script was executed in the container.
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
            raise ValueError("Invalid python module mode: " + python_module_mode)
        CommonDirs.YARN_DEV_TOOLS_MODULE_ROOT = FileUtils.join_path(python_site, YARNDEVTOOLS_MODULE_NAME)
        CommonFiles.YARN_DEV_TOOLS_SCRIPT = os.path.join(CommonDirs.YARN_DEV_TOOLS_MODULE_ROOT, "yarn_dev_tools.py")

    @staticmethod
    def _relink_cdsw_jobs_to_yarndevtools_cdsw_runner_scripts():
        LOG.info("Linking jobs to place...")
        for cdsw_script_dirname in CommonDirs.CDSW_SCRIPT_DIR_NAMES:
            # It's safer to delete dirs one by one explictly, without specifying just the parent
            cdsw_job_dir = FileUtils.join_path(CommonDirs.YARN_DEV_TOOLS_JOBS_BASEDIR, cdsw_script_dirname)
            FileUtils.remove_dir(cdsw_job_dir, force=True)

            found_files = FileUtils.find_files(
                CommonDirs.YARN_DEV_TOOLS_MODULE_ROOT,
                find_type=FindResultType.FILES,
                regex=CDSW_RUNNER_PY,
                parent_dir=cdsw_script_dirname,
                single_level=False,
                full_path_result=True,
            )
            if len(found_files) != 1:
                raise ValueError(
                    f"Expected to find 1 file with name {CDSW_RUNNER_PY} "
                    f"and parent dir '{cdsw_script_dirname}'. "
                    f"Actual results: {found_files}"
                )
            cdsw_script_path = found_files[0]
            FileUtils.create_new_dir(cdsw_job_dir)
            new_link_path = FileUtils.join_path(cdsw_job_dir, CDSW_RUNNER_PY)
            FileUtils.create_symlink(cdsw_script_path, new_link_path)

    @staticmethod
    def prepare_env_vars(env_var_dict: Dict[str, str] = None, mandatory_env_vars: List[str] = None):
        for k, v in env_var_dict.items():
            OsUtils.set_env_value(k, v)

        for env_var in mandatory_env_vars:
            if env_var not in os.environ:
                raise ValueError(f"{env_var} is not set. Please set it to a valid value!")


class CdswRunnerBase(ABC):
    def __init__(self):
        self.common_mail_config = CommonMailConfig()

    def start_common(self, basedir):
        LOG.info("Starting CDSW runner...")

    @abstractmethod
    def start(self, basedir):
        pass

    @staticmethod
    def run_clone_downstream_repos_script(basedir):
        script = os.path.join(basedir, "clone_downstream_repos.sh")
        cmd = f"{BASHX} {script}"
        SubprocessCommandRunner.run_and_follow_stdout_stderr(cmd, stdout_logger=CMD_LOG, exit_on_nonzero_exitcode=True)

    @staticmethod
    def run_clone_upstream_repos_script(basedir):
        script = os.path.join(basedir, "clone_upstream_repos.sh")
        cmd = f"{BASHX} {script}"
        SubprocessCommandRunner.run_and_follow_stdout_stderr(cmd, stdout_logger=CMD_LOG, exit_on_nonzero_exitcode=True)

    @staticmethod
    def execute_yarndevtools_script(script_args):
        cmd = f"{PY3} {CommonFiles.YARN_DEV_TOOLS_SCRIPT} {script_args}"
        SubprocessCommandRunner.run_and_follow_stdout_stderr(cmd, stdout_logger=CMD_LOG, exit_on_nonzero_exitcode=True)

    @staticmethod
    def current_date_formatted():
        return DateUtils.get_current_datetime()

    def run_zipper(self, command_type: CommandType, debug=False, ignore_filetypes: str = "java js"):
        debug_mode = "--debug" if debug else ""
        self.execute_yarndevtools_script(
            f"{debug_mode} "
            f"{CommandType.ZIP_LATEST_COMMAND_DATA.name} {command_type.name} "
            f"--dest_dir /tmp "
            f"--ignore-filetypes {ignore_filetypes} "
        )

    def send_latest_command_data_in_email(
        self, sender, subject, recipients=None, attachment_filename=None, email_body_file: str = None
    ):
        if not recipients:
            recipients = self.determine_recipients()
        attachment_filename_val = f"{attachment_filename}" if attachment_filename else ""
        email_body_file_param = f"--file-as-email-body-from-zip {email_body_file}" if email_body_file else ""
        self.execute_yarndevtools_script(
            f"--debug {CommandType.SEND_LATEST_COMMAND_DATA.name} "
            f"{self.common_mail_config.as_arguments()}"
            f'--subject "{subject}" '
            f'--sender "{sender}" '
            f'--recipients "{recipients}" '
            f"--attachment-filename {attachment_filename_val} "
            f"{email_body_file_param}"
        )

    def determine_recipients(self, default_recipients=MAIL_ADDR_YARN_ENG_BP):
        recipients_env = OsUtils.get_env_value(CdswEnvVar.MAIL_RECIPIENTS.value)
        if recipients_env:
            return recipients_env
        return default_recipients


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
