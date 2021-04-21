import sys
from abc import ABC, abstractmethod
import logging
import os
from typing import Dict, List

# MAKE SURE THIS PRECEDES IMPORT TO pythoncommons
import subprocess

# https://stackoverflow.com/a/50255019/1106893
from pythoncommons.date_utils import DateUtils
from pythoncommons.file_utils import FileUtils

from yarndevtools.cdsw.common_python.constants import EnvVar

from pythoncommons.process import SubprocessCommandRunner

# Constants
# Move this to EnvVar enum
ENV_OVERRIDE_SCRIPT_BASEDIR = "OVERRIDE_SCRIPT_BASEDIR"
LOG = logging.getLogger(__name__)
CMD_LOG = logging.getLogger(__name__)
BASEDIR = None
PYPATH = "PYTHONPATH"
PY3 = "python3"
BASH = "bash"
BASHX = "bash -x"
MAIL_ADDR_YARN_ENG_BP = "yarn_eng_bp@cloudera.com"

CDSW_BASEDIR = FileUtils.join_path("home", "cdsw")
HADOOP_CLOUDERA_BASEDIR = FileUtils.join_path(CDSW_BASEDIR, "repos", "cloudera", "hadoop")
YARN_DEV_TOOLS_ROOT_DIR = FileUtils.join_path(CDSW_BASEDIR, "repos", "snemeth", "yarn-dev-tools")
YARN_DEV_TOOLS_CDSW_ROOT_DIR = FileUtils.join_path(
    CDSW_BASEDIR, "repos", "snemeth", "yarn-dev-tools", "yarndevtools", "cdsw"
)


class CdswSetup:
    @staticmethod
    def setup_logging():
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
        cmd_log_handler = logging.StreamHandler(stream=sys.stdout)
        CMD_LOG.propagate = False
        CMD_LOG.addHandler(cmd_log_handler)
        cmd_log_handler.setFormatter(logging.Formatter("%(message)s"))

    @staticmethod
    def fix_pythonpath(additional_dir):
        if PYPATH in os.environ:
            LOG.debug(f"Old {PYPATH}: {CdswSetup._get_pythonpath()}")
            os.environ[PYPATH] = f"{CdswSetup._get_pythonpath()}:{additional_dir}"
            LOG.debug(f"New {PYPATH}: {CdswSetup._get_pythonpath()}")
        else:
            LOG.debug(f"Old {PYPATH}: not set")
            os.environ[PYPATH] = additional_dir
            LOG.debug(f"New {PYPATH}: {CdswSetup._get_pythonpath}")

    @staticmethod
    def _get_pythonpath():
        return os.environ[PYPATH]

    @staticmethod
    def initial_setup(env_var_dict: Dict[str, str] = None, mandatory_env_vars: List[EnvVar] = None):
        LOG.info(f"Python version info: {sys.version}")
        CdswSetup.setup_logging()
        if not env_var_dict:
            env_var_dict = {}
        if not mandatory_env_vars:
            mandatory_env_vars = []

        if mandatory_env_vars:
            LOG.info(f"Printing env vars: {os.environ}")

        # Could have set it to dummy dir but dir must be an existing dir and a valid git repo as well,
        # so it's more simple to reuse CLOUDERA_HADOOP_ROOT.
        env_var_dict.update(
            {
                EnvVar.CLOUDERA_HADOOP_ROOT.value: HADOOP_CLOUDERA_BASEDIR,
                EnvVar.HADOOP_DEV_DIR.value: HADOOP_CLOUDERA_BASEDIR,
            }
        )

        CdswSetup.prepare_env_vars(env_var_dict=env_var_dict, mandatory_env_vars=mandatory_env_vars)
        if ENV_OVERRIDE_SCRIPT_BASEDIR in os.environ:
            basedir = os.environ[ENV_OVERRIDE_SCRIPT_BASEDIR]
        else:
            basedir = YARN_DEV_TOOLS_CDSW_ROOT_DIR
        LOG.info("Using basedir for scripts: " + basedir)
        return basedir

    @staticmethod
    def prepare_env_vars(env_var_dict: Dict[str, str] = None, mandatory_env_vars: List[EnvVar] = None):
        for k, v in env_var_dict.items():
            LOG.info(f"Setting env var. {k}={v}")
            os.environ[k] = v

        for env_var in mandatory_env_vars:
            if env_var.value not in os.environ:
                raise ValueError(f"{env_var} is not set. Please set it to a valid value!")


class CdswRunnerBase(ABC):
    def __init__(self):
        self.yarn_dev_tools_script = os.path.join(YARN_DEV_TOOLS_ROOT_DIR, "yarndevtools", "yarn_dev_tools.py")
        self.common_mail_config = CommonMailConfig()

    @abstractmethod
    def start(self, basedir):
        pass

    def run_clone_repos_script(self, basedir):
        clone_repos_script = os.path.join(basedir, "scripts", "clone_repos.sh")
        cmd = f"{BASHX} {clone_repos_script}"
        SubprocessCommandRunner.run_and_follow_stdout_stderr(cmd, stdout_logger=CMD_LOG, exit_on_nonzero_exitcode=True)

    def execute_yarndevtools_script(self, script_args):
        cmd = f"{PY3} {self.yarn_dev_tools_script} {script_args}"
        SubprocessCommandRunner.run_and_follow_stdout_stderr(cmd, stdout_logger=CMD_LOG, exit_on_nonzero_exitcode=True)

    @staticmethod
    def current_date_formatted():
        return DateUtils.get_current_datetime()


class CommonMailConfig:
    def __init__(self):
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 465
        self.account_user = os.environ[EnvVar.MAIL_ACC_USER.value]
        self.account_password = os.environ[EnvVar.MAIL_ACC_PASSWORD.value]

    def as_arguments(self):
        return (
            f'--smtp_server "{self.smtp_server}" '
            f"--smtp_port {self.smtp_port} "
            f'--account_user "{self.account_user}" '
            f'--account_password "{self.account_password}" '
        )