import logging
import os
import sys
import unittest
from enum import Enum
from typing import Dict
from pythoncommons.docker_wrapper import DockerTestSetup
from pythoncommons.file_utils import FileUtils, FindResultType
from pythoncommons.object_utils import ObjUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.process import SubprocessCommandRunner
from pythoncommons.project_utils import PROJECTS_BASEDIR_NAME, SimpleProjectUtils

from yarndevtools.cdsw.common_python.cdsw_common import CommonDirs, PythonModuleMode
from yarndevtools.cdsw.common_python.constants import CdswEnvVar, BRANCH_DIFF_REPORTER_DIR_NAME, BranchComparatorEnvVar
from yarndevtools.common.shared_command_utils import RepoType, EnvVar
from yarndevtools.constants import ORIGIN_BRANCH_3_3, ORIGIN_TRUNK


CREATE_IMAGE = True
PROJECT_NAME = "yarn-cdsw-branchdiff-reporting"
PROJECT_VERSION = "1.0"
DOCKER_IMAGE = f"szyszy/{PROJECT_NAME}:{PROJECT_VERSION}"

MOUNT_MODE_RW = "rw"
PYTHON3 = "python3"
BASH = "bash"
CDSW_DIRNAME = "cdsw"
REPO_ROOT_DIRNAME = "yarn-dev-tools"
CDSW_RUNNER_PY = "cdsw_runner.py"
LOG = logging.getLogger(__name__)
CMD_LOG = logging.getLogger(__name__)
CONTAINER_SLEEP = 300
INITIAL_CDSW_SETUP_SCRIPT = "initial-cdsw-setup.sh"


class ContainerFiles:
    BRANCH_DIFF_SCRIPT = FileUtils.join_path(
        CommonDirs.YARN_DEV_TOOLS_JOBS_BASEDIR, BRANCH_DIFF_REPORTER_DIR_NAME, CDSW_RUNNER_PY
    )
    INITIAL_CDSW_SETUP_SCRIPT = FileUtils.join_path(
        CommonDirs.YARN_DEV_TOOLS_SCRIPTS_BASEDIR, INITIAL_CDSW_SETUP_SCRIPT
    )


class ContainerDirs:
    YARN_DEV_TOOLS_OUTPUT_DIR = FileUtils.join_path("root", PROJECTS_BASEDIR_NAME, "yarn_dev_tools")
    CDSW_BASEDIR = CommonDirs.CDSW_BASEDIR
    YARN_DEV_TOOLS_SCRIPTS_BASEDIR = CommonDirs.YARN_DEV_TOOLS_SCRIPTS_BASEDIR
    HADOOP_CLOUDERA_BASEDIR = CommonDirs.HADOOP_CLOUDERA_BASEDIR
    HADOOP_UPSTREAM_BASEDIR = CommonDirs.HADOOP_UPSTREAM_BASEDIR


class LocalDirs:
    REPO_ROOT_DIR = FileUtils.find_repo_root_dir(__file__, REPO_ROOT_DIRNAME)
    CDSW_ROOT_DIR = None
    SCRIPTS_DIR = None
    YARNDEVTOOLS_RESULT_DIR = None


class DockerMounts:
    def __init__(self, docker_test_setup, exec_mode, python_module_mode):
        self.docker_test_setup = docker_test_setup
        self.exec_mode = exec_mode
        self.python_module_mode = python_module_mode

    def setup_env_vars(self):
        # !! WARNING: User-specific settings below !!
        if self.exec_mode == TestExecMode.CLOUDERA:
            # We need both upstream / downstream repos for Cloudera-mode
            os.environ[CdswEnvVar.CLOUDERA_HADOOP_ROOT.value] = FileUtils.join_path(
                CommonDirs.USER_DEV_ROOT, "cloudera", "hadoop"
            )
            os.environ[CdswEnvVar.HADOOP_DEV_DIR.value] = FileUtils.join_path(
                CommonDirs.USER_DEV_ROOT, "apache", "hadoop"
            )
        elif self.exec_mode == TestExecMode.UPSTREAM:
            os.environ[CdswEnvVar.HADOOP_DEV_DIR.value] = FileUtils.join_path(
                CommonDirs.USER_DEV_ROOT, "apache", "hadoop"
            )
            os.environ[BranchComparatorEnvVar.REPO_TYPE.value] = RepoType.UPSTREAM.value
            os.environ[BranchComparatorEnvVar.FEATURE_BRANCH.value] = ORIGIN_BRANCH_3_3
            os.environ[BranchComparatorEnvVar.MASTER_BRANCH.value] = ORIGIN_TRUNK

        if self.python_module_mode == PythonModuleMode.GLOBAL:
            os.environ[CdswEnvVar.PYTHON_MODULE_MODE.value] = PythonModuleMode.GLOBAL.value
        elif self.python_module_mode == PythonModuleMode.USER:
            os.environ[CdswEnvVar.PYTHON_MODULE_MODE.value] = PythonModuleMode.USER.value

    def setup_default_docker_mounts(self):
        self.setup_env_vars()
        # Mount scripts dir, initial-cdsw-setup.sh will be executed from there
        self.docker_test_setup.mount_dir(
            LocalDirs.SCRIPTS_DIR, ContainerDirs.YARN_DEV_TOOLS_SCRIPTS_BASEDIR, mode=MOUNT_MODE_RW
        )
        # Mount results dir so all output files will be available on the host
        self.docker_test_setup.mount_dir(
            LocalDirs.YARNDEVTOOLS_RESULT_DIR, ContainerDirs.YARN_DEV_TOOLS_OUTPUT_DIR, mode=MOUNT_MODE_RW
        )
        if self.exec_mode == TestExecMode.CLOUDERA:
            self._mount_downstream_hadoop_repo()
            self._mount_upstream_hadoop_repo()
        elif self.exec_mode == TestExecMode.UPSTREAM:
            self._mount_upstream_hadoop_repo()

    def _mount_downstream_hadoop_repo(self):
        # Mount local Cloudera Hadoop dir so that container won't clone the repo again and again
        self.docker_test_setup.mount_dir(
            os.environ[CdswEnvVar.CLOUDERA_HADOOP_ROOT.value], ContainerDirs.HADOOP_CLOUDERA_BASEDIR, mode=MOUNT_MODE_RW
        )

    def _mount_upstream_hadoop_repo(self):
        # Mount local upstream Hadoop dir so that container won't clone the repo again and again
        self.docker_test_setup.mount_dir(
            os.environ[CdswEnvVar.HADOOP_DEV_DIR.value], ContainerDirs.HADOOP_UPSTREAM_BASEDIR, mode=MOUNT_MODE_RW
        )


class TestExecMode(Enum):
    CLOUDERA = "cloudera"
    UPSTREAM = "upstream"


class YarnCdswBranchDiffTests(unittest.TestCase):
    python_module_mode = None
    exec_mode = None
    docker_test_setup = None
    docker_mounts = None

    @classmethod
    def setUpClass(cls):
        if CdswEnvVar.MAIL_ACC_PASSWORD.value not in os.environ:
            raise ValueError(f"Please set '{CdswEnvVar.MAIL_ACC_PASSWORD.value}' env var and re-run the test!")
        cls._setup_logging()
        cls.setup_local_dirs()
        cls.exec_mode: TestExecMode = cls.determine_execution_mode()
        # Only user-site mode can work in Docker containers
        # With global mode, the following error is coming up:
        # cp /root/.local/lib/python3.8/site-packages/yarndevtools/cdsw/downstream-branchdiff-reporting/cdsw_runner.py /home/cdsw/jobs//downstream-branchdiff-reporting/cdsw_runner.py
        # cp: cannot stat '/root/.local/lib/python3.8/site-packages/yarndevtools/cdsw/downstream-branchdiff-reporting/cdsw_runner.py'
        # No such file or directory
        cls.python_module_mode = PythonModuleMode.USER
        cls.docker_test_setup = DockerTestSetup(
            DOCKER_IMAGE, create_image=CREATE_IMAGE, dockerfile_location=LocalDirs.CDSW_ROOT_DIR, logger=CMD_LOG
        )
        cls.docker_mounts = DockerMounts(cls.docker_test_setup, cls.exec_mode, cls.python_module_mode)
        cls.docker_mounts.setup_default_docker_mounts()

    @classmethod
    def tearDownClass(cls) -> None:
        pass

    @classmethod
    def setup_local_dirs(cls):
        LocalDirs.CDSW_ROOT_DIR = cls.get_cdsw_root_dir()
        LocalDirs.SCRIPTS_DIR = FileUtils.join_path(LocalDirs.CDSW_ROOT_DIR, "scripts")
        LocalDirs.YARNDEVTOOLS_RESULT_DIR = FileUtils.join_path(LocalDirs.CDSW_ROOT_DIR, "yarndevtools-results")
        # TODO
        cls.branchdiff_cdsw_runner_script = YarnCdswBranchDiffTests.find_cdsw_runner_script(
            os.path.join(LocalDirs.CDSW_ROOT_DIR, BRANCH_DIFF_REPORTER_DIR_NAME)
        )
        # LOG.info("Local files: %s", ObjUtils.get_static_fields_with_values(LocalFiles))
        LOG.info("Local dirs: %s", ObjUtils.get_static_fields_with_values(LocalDirs))
        LOG.info("Container files: %s", ObjUtils.get_static_fields_with_values(ContainerFiles))
        LOG.info("Container dirs: %s", ObjUtils.get_static_fields_with_values(ContainerDirs))

    @classmethod
    def determine_execution_mode(cls):
        exec_mode_env: str = OsUtils.get_env_value(
            CdswEnvVar.TEST_EXECUTION_MODE.value, default_value=TestExecMode.CLOUDERA.value
        )
        return TestExecMode[exec_mode_env.upper()]

    @classmethod
    def get_cdsw_root_dir(cls):
        return SimpleProjectUtils.get_project_dir(
            basedir=LocalDirs.REPO_ROOT_DIR,
            parent_dir="yarndevtools",
            dir_to_find=CDSW_DIRNAME,
            find_result_type=FindResultType.DIRS,
        )

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

    @classmethod
    def exec_branch_diff_script(cls, args="", env: Dict[str, str] = None):
        return cls.docker_test_setup.exec_cmd_in_container(
            f"{PYTHON3} {ContainerFiles.BRANCH_DIFF_SCRIPT} {args}", stdin=False, tty=False, env=env
        )

    @classmethod
    def exec_initial_cdsw_setup_script(cls, args="", env: Dict[str, str] = None):
        if cls.python_module_mode == PythonModuleMode.GLOBAL:
            args = PythonModuleMode.GLOBAL.value
        return cls.docker_test_setup.exec_cmd_in_container(
            f"{BASH} {ContainerFiles.INITIAL_CDSW_SETUP_SCRIPT} {args}", stdin=False, tty=False, env=env
        )

    def setUp(self):
        self.docker_test_setup.test_instance = self

    def tearDown(self) -> None:
        self.docker_test_setup.cleanup()

    def save_latest_zip_from_container(self):
        zip_link = FileUtils.join_path(LocalDirs.YARNDEVTOOLS_RESULT_DIR, "latest-command-data-zip")
        cont_target_path = os.readlink(zip_link)
        local_target_path = FileUtils.join_path(LocalDirs.YARNDEVTOOLS_RESULT_DIR, "latest-command-data-real.zip")
        command = f"docker cp {self.docker_test_setup.container.id}:{cont_target_path} {local_target_path}"
        SubprocessCommandRunner.run_and_follow_stdout_stderr(command)

    @classmethod
    def cdsw_runner_env_dict(cls):
        env_dict = {
            e.value: OsUtils.get_env_value(e.value, None)
            for e in [
                CdswEnvVar.MAIL_ACC_USER,
                CdswEnvVar.MAIL_ACC_PASSWORD,
                BranchComparatorEnvVar.REPO_TYPE,
                BranchComparatorEnvVar.MASTER_BRANCH,
                BranchComparatorEnvVar.FEATURE_BRANCH,
                EnvVar.IGNORE_SMTP_AUTH_ERROR,
            ]
        }
        # TODO
        # Manually fix PYTHONPATH like CDSW init script does
        # env_dict.update([cls.create_python_path_env_var(CommonDirs.YARN_DEV_TOOLS_MODULE_ROOT)])
        return env_dict

    @staticmethod
    def create_python_path_env_var(new_dir, fresh=True):
        if not fresh:
            curr_pythonpath = os.environ[CdswEnvVar.PYTHONPATH.value]
            new_pythonpath = f"{curr_pythonpath}:{new_dir}"
        else:
            new_pythonpath = new_dir
        return CdswEnvVar.PYTHONPATH.value, new_pythonpath

    def test_basic_cdsw_runner(self):
        self.docker_mounts.setup_default_docker_mounts()
        self.docker_test_setup.run_container(sleep=CONTAINER_SLEEP)
        # TODO Run this only at Docker image creation
        self.exec_initial_cdsw_setup_script()
        # self.docker_test_setup.inspect_container(self.docker_test_setup.container.id)
        exit_code = self.exec_branch_diff_script(env=self.cdsw_runner_env_dict())
        self.assertEqual(exit_code, 0)
        self.save_latest_zip_from_container()
        # TODO check if zip exists and size is bigger than 0 and extractable
        # TODO verify files are placed to correct dir in zip
        # TODO verify if all files are present and they are non-zero sized
        # TODO verify if HTML output is contained in email's body

    def test_streaming_cmd_output(self):
        captured_output = []

        def _kill_after_5_lines(cmd, out, docker_setup):
            captured_output.append(out)
            if len(captured_output) >= 3:
                captured_output.clear()
                pid = docker_setup.exec_cmd_in_container(f"pgrep -f {os.path.basename(cmd)}", stream=False)
                docker_setup.exec_cmd_in_container(f"kill {pid}", stream=False)

        self.docker_mounts.setup_default_docker_mounts()
        self.docker_test_setup.run_container()
        self.docker_test_setup.exec_cmd_in_container(
            f"{ContainerDirs.CDSW_BASEDIR}/common/test.sh", callback=_kill_after_5_lines, fail_on_error=False
        )
        self.docker_test_setup.exec_cmd_in_container(
            f"{PYTHON3} {ContainerDirs.CDSW_BASEDIR}/common/test.py", callback=_kill_after_5_lines, fail_on_error=False
        )

    # TODO write testcase to test ut-results-reporting with fake jenkins: It can return a valid & invalid UT result JSON response
