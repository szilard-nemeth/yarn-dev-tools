import logging
import os
import sys
import unittest
from typing import Dict
from pythoncommons.docker_wrapper import DockerTestSetup
from pythoncommons.file_utils import FileUtils
from pythoncommons.process import SubprocessCommandRunner
from pythoncommons.project_utils import PROJECTS_BASEDIR_NAME

from yarndevtools.cdsw.common_python.cdsw_common import (
    HADOOP_CLOUDERA_BASEDIR,
    CDSW_BASEDIR,
    YARN_DEV_TOOLS_ROOT_DIR,
    YARN_DEV_TOOLS_CDSW_ROOT_DIR,
)
from yarndevtools.cdsw.common_python.constants import EnvVar, BRANCH_DIFF_REPORTER_DIR_NAME

CREATE_IMAGE = True

YARN_DEV_TOOLS_OUTPUT_CONTAINER_DIR = FileUtils.join_path("root", PROJECTS_BASEDIR_NAME, "yarn_dev_tools")
PROJECT_NAME = "yarn-cdsw-branchdiff-reporting"
PROJECT_VERSION = "1.0"
MOUNT_MODE_RW = "rw"
PYTHON3 = "python3"
BASH = "bash"
CDSW_DIRNAME = "cdsw"
REPO_ROOT_DIRNAME = "yarn-dev-tools"
CDSW_RUNNER_PY = "cdsw_runner.py"
BRANCH_DIFF_SCRIPT_CONTAINER = FileUtils.join_path(
    YARN_DEV_TOOLS_CDSW_ROOT_DIR, BRANCH_DIFF_REPORTER_DIR_NAME, "cdsw_runner.py"
)
DOCKER_IMAGE = f"szyszy/{PROJECT_NAME}:{PROJECT_VERSION}"
LOG = logging.getLogger(__name__)
CMD_LOG = logging.getLogger(__name__)

CONTAINER_SLEEP = 300


class YarnCdswBranchDiffTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Test expects that MAIL_ACC_PASSWORD is set with env var
        if EnvVar.MAIL_ACC_PASSWORD.value not in os.environ:
            raise ValueError(f"Please set '{EnvVar.MAIL_ACC_PASSWORD.value}' env var and re-run the test!")
        cls._setup_logging()
        cls.repo_cdsw_root_dir = FileUtils.find_repo_root_dir(__file__, CDSW_DIRNAME)
        cls.repo_root_dir = FileUtils.find_repo_root_dir(__file__, REPO_ROOT_DIRNAME)
        cls.yarn_dev_tools_results_dir = FileUtils.join_path(cls.repo_cdsw_root_dir, "yarndevtools-results")
        cls.branchdiff_cdsw_runner_script = YarnCdswBranchDiffTests.find_cdsw_runner_script(
            os.path.join(cls.repo_cdsw_root_dir, BRANCH_DIFF_REPORTER_DIR_NAME)
        )
        cls.docker_test_setup = DockerTestSetup(
            DOCKER_IMAGE, create_image=CREATE_IMAGE, dockerfile_location=cls.repo_cdsw_root_dir, logger=CMD_LOG
        )

        # !! WARNING: User-specific setting !!
        os.environ[EnvVar.CLOUDERA_HADOOP_ROOT.value] = "/Users/snemeth/development/cloudera/hadoop/"

    @classmethod
    def _setup_logging(cls):
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
        handler = logging.StreamHandler(stream=sys.stdout)
        CMD_LOG.propagate = False
        CMD_LOG.addHandler(handler)
        handler.setFormatter(logging.Formatter("%(message)s"))

    @staticmethod
    def find_cdsw_runner_script(parent_dir):
        results = FileUtils.search_files(parent_dir, CDSW_RUNNER_PY)
        if not results:
            raise ValueError(f"Expected to find file: {CDSW_RUNNER_PY}")
        return results[0]

    def setUp(self):
        self.docker_test_setup.test_instance = self

    def _setup_default_docker_mounts(self):
        # Mount dev dir so source code changes are visible in container immediately
        self.docker_test_setup.mount_dir(self.repo_root_dir, YARN_DEV_TOOLS_ROOT_DIR, mode=MOUNT_MODE_RW)

        # Mount local Cloudera Hadoop dir so that container won't clone it again and again
        self.docker_test_setup.mount_dir(
            os.environ[EnvVar.CLOUDERA_HADOOP_ROOT.value], HADOOP_CLOUDERA_BASEDIR, mode=MOUNT_MODE_RW
        )
        # Mount results dir so all output files will be available on the host
        self.docker_test_setup.mount_dir(
            self.yarn_dev_tools_results_dir, YARN_DEV_TOOLS_OUTPUT_CONTAINER_DIR, mode=MOUNT_MODE_RW
        )

    def tearDown(self) -> None:
        self.docker_test_setup.cleanup()

    def save_latest_zip_from_container(self):
        zip_link = FileUtils.join_path(self.yarn_dev_tools_results_dir, "latest-command-data-zip")
        cont_target_path = os.readlink(zip_link)
        local_target_path = FileUtils.join_path(self.yarn_dev_tools_results_dir, "latest-command-data-real.zip")
        command = f"docker cp {self.docker_test_setup.container.id}:{cont_target_path} {local_target_path}"
        SubprocessCommandRunner.run_and_follow_stdout_stderr(command)

    @classmethod
    def tearDownClass(cls) -> None:
        pass

    def exec_branch_diff_script(self, args="", env: Dict[str, str] = None):
        return self.docker_test_setup.exec_cmd_in_container(
            f"{PYTHON3} {BRANCH_DIFF_SCRIPT_CONTAINER} {args}", stdin=False, tty=False, env=env
        )

    def test_basic_cdsw_runner(self):
        self._setup_default_docker_mounts()
        self.docker_test_setup.run_container(sleep=CONTAINER_SLEEP)
        # self.docker_test_setup.inspect_container(self.docker_test_setup.container.id)
        exit_code = self.exec_branch_diff_script(env=self.cdsw_runner_env_dict())
        self.assertEqual(exit_code, 0)
        self.save_latest_zip_from_container()
        # TODO check if zip exists and size is bigger than 0 and extractable
        # TODO verify files are placed to correct dir in zip
        # TODO verify if all files are present and they are non-zero sized
        # TODO verify if HTML output is contained in email's body

    @classmethod
    def cdsw_runner_env_dict(cls):
        env_dict = {
            EnvVar.MAIL_ACC_USER.value: os.environ[EnvVar.MAIL_ACC_USER.value],
            EnvVar.MAIL_ACC_PASSWORD.value: os.environ[EnvVar.MAIL_ACC_PASSWORD.value],
        }
        # Manually fix PYTHONPATH like CDSW init script does
        env_dict.update([cls.create_python_path_env_var(YARN_DEV_TOOLS_ROOT_DIR)])
        return env_dict

    @staticmethod
    def create_python_path_env_var(new_dir, fresh=True):
        if not fresh:
            curr_pythonpath = os.environ[EnvVar.PYTHONPATH.value]
            new_pythonpath = f"{curr_pythonpath}:{new_dir}"
        else:
            new_pythonpath = new_dir
        return EnvVar.PYTHONPATH.value, new_pythonpath

    def test_streaming_cmd_output(self):
        captured_output = []

        def _kill_after_5_lines(cmd, out, docker_setup):
            captured_output.append(out)
            if len(captured_output) >= 3:
                captured_output.clear()
                pid = docker_setup.exec_cmd_in_container(f"pgrep -f {os.path.basename(cmd)}", stream=False)
                docker_setup.exec_cmd_in_container(f"kill {pid}", stream=False)

        self._setup_default_docker_mounts()
        self.docker_test_setup.run_container()
        self.docker_test_setup.exec_cmd_in_container(
            f"{CDSW_BASEDIR}/common/test.sh", callback=_kill_after_5_lines, fail_on_error=False
        )
        self.docker_test_setup.exec_cmd_in_container(
            f"{PYTHON3} {CDSW_BASEDIR}/common/test.py", callback=_kill_after_5_lines, fail_on_error=False
        )

    # TODO write testcase to test ut-results-reporting with fake jenkins: It can return a valid & invalid UT result JSON response
