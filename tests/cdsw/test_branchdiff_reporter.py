import logging
import os
import unittest
from enum import Enum
from typing import Dict, List

from pythoncommons.constants import ExecutionMode
from pythoncommons.docker_wrapper import DockerTestSetup, CreatePathMode
from pythoncommons.file_utils import FileUtils, FindResultType
from pythoncommons.github_utils import GitHubUtils
from pythoncommons.logging_setup import SimpleLoggingSetupConfig, SimpleLoggingSetup
from pythoncommons.object_utils import ObjUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import (
    PROJECTS_BASEDIR_NAME,
    SimpleProjectUtils,
    ProjectRootDeterminationStrategy,
    ProjectUtils,
    ProjectUtilsEnvVar,
)

from yarndevtools.argparser import CommandType
from yarndevtools.cdsw.common_python.cdsw_common import (
    CommonDirs,
    PythonModuleMode,
    TestExecMode,
    DEFAULT_TEST_EXECUTION_MODE,
)
from yarndevtools.cdsw.common_python.constants import (
    CdswEnvVar,
    BRANCH_DIFF_REPORTER_DIR_NAME,
    BranchComparatorEnvVar,
    CDSW_RUNNER_PY,
)
from yarndevtools.common.shared_command_utils import RepoType, EnvVar, SECRET_PROJECTS_DIR
from yarndevtools.constants import ORIGIN_BRANCH_3_3, ORIGIN_TRUNK, YARNDEVTOOLS_MODULE_NAME, APACHE, HADOOP, CLOUDERA

PYTHON3 = "python3"
PROJECT_NAME = "yarn-cdsw-branchdiff-reporting"
PROJECT_VERSION = "1.0"
DOCKER_IMAGE = f"szyszy/{PROJECT_NAME}:{PROJECT_VERSION}"

# TODO Consolidate mount modes to enum, also MOUNT_MODE_RW is present in docker_wrapper.py
MOUNT_MODE_RW = "rw"
MOUNT_MODE_READ_ONLY = "ro"
BASH = "bash"
CDSW_DIRNAME = "cdsw"
REPO_ROOT_DIRNAME = "yarn-dev-tools"
LOG = logging.getLogger(__name__)
CMD_LOG = logging.getLogger(__name__)
INITIAL_CDSW_SETUP_SCRIPT = "initial-cdsw-setup.sh"


class ContainerFiles:
    BRANCH_DIFF_SCRIPT = FileUtils.join_path(
        CommonDirs.YARN_DEV_TOOLS_JOBS_BASEDIR, BRANCH_DIFF_REPORTER_DIR_NAME, CDSW_RUNNER_PY
    )
    INITIAL_CDSW_SETUP_SCRIPT = FileUtils.join_path(
        CommonDirs.YARN_DEV_TOOLS_SCRIPTS_BASEDIR, INITIAL_CDSW_SETUP_SCRIPT
    )


class ContainerDirs:
    CDSW_BASEDIR = CommonDirs.CDSW_BASEDIR
    YARN_DEV_TOOLS_OUTPUT_DIR = FileUtils.join_path(CDSW_BASEDIR, PROJECTS_BASEDIR_NAME, YARNDEVTOOLS_MODULE_NAME)
    YARN_DEV_TOOLS_SCRIPTS_BASEDIR = CommonDirs.YARN_DEV_TOOLS_SCRIPTS_BASEDIR
    HADOOP_CLOUDERA_BASEDIR = CommonDirs.HADOOP_CLOUDERA_BASEDIR
    HADOOP_UPSTREAM_BASEDIR = CommonDirs.HADOOP_UPSTREAM_BASEDIR
    CDSW_SECRET_DIR = FileUtils.join_path("/root", ".secret", "projects", "cloudera", CDSW_DIRNAME)


class LocalDirs:
    REPO_ROOT_DIR = FileUtils.find_repo_root_dir(__file__, REPO_ROOT_DIRNAME)
    CDSW_ROOT_DIR = None
    SCRIPTS_DIR = None
    YARNDEVTOOLS_RESULT_DIR = None
    CDSW_SECRET_DIR = FileUtils.join_path(SECRET_PROJECTS_DIR, CDSW_DIRNAME)


class DockerMounts:
    def __init__(self, class_of_test, docker_test_setup, exec_mode: TestExecMode, python_module_mode):
        self.class_of_test = class_of_test
        self.docker_test_setup = docker_test_setup
        self.exec_mode: TestExecMode = exec_mode
        self.python_module_mode = python_module_mode

    def setup_default_docker_mounts(self):
        # TODO Perhaps, mount logic can be changed to simple docker copy but keep the condition
        if self.class_of_test.config.mount_cdsw_dirs_from_local:
            # Mounting ContainerDirs.CDSW_BASEDIR is not a good idea in read-write mode as
            # files are being created to /home/cdsw inside the container.
            # Mounting it with readonly mode also does not make sense as writing files would be prevented.
            # So, the only option left is to mount dirs one by one.
            dirs_to_mount = FileUtils.find_files(
                LocalDirs.CDSW_ROOT_DIR,
                find_type=FindResultType.DIRS,
                single_level=True,
                full_path_result=True,
                exclude_dirs=["yarndevtools-results"],
            )
            for dir in dirs_to_mount:
                self.docker_test_setup.mount_dir(
                    dir,
                    FileUtils.join_path(ContainerDirs.CDSW_BASEDIR, FileUtils.basename(dir)),
                    mode=MOUNT_MODE_READ_ONLY,
                )
        else:
            # Mount scripts dir, initial-cdsw-setup.sh will be executed from there
            self.docker_test_setup.mount_dir(
                LocalDirs.SCRIPTS_DIR, ContainerDirs.YARN_DEV_TOOLS_SCRIPTS_BASEDIR, mode=MOUNT_MODE_RW
            )
        # Mount results dir so all output files will be available on the host
        self.docker_test_setup.mount_dir(
            LocalDirs.YARNDEVTOOLS_RESULT_DIR, ContainerDirs.YARN_DEV_TOOLS_OUTPUT_DIR, mode=MOUNT_MODE_RW
        )
        # TODO Remove code that sends mail attachment
        if self.exec_mode == TestExecMode.CLOUDERA:
            self._mount_downstream_hadoop_repo()
            self._mount_upstream_hadoop_repo()
        elif self.exec_mode == TestExecMode.UPSTREAM:
            self._mount_upstream_hadoop_repo()

        # Only print mounts in the end
        self.docker_test_setup.print_mounts()

    def _mount_downstream_hadoop_repo(self):
        # Mount local Cloudera Hadoop dir so that container won't clone the repo again and again
        self.docker_test_setup.mount_dir(
            OsUtils.get_env_value(CdswEnvVar.CLOUDERA_HADOOP_ROOT.value),
            ContainerDirs.HADOOP_CLOUDERA_BASEDIR,
            mode=MOUNT_MODE_RW,
        )

    def _mount_upstream_hadoop_repo(self):
        # Mount local upstream Hadoop dir so that container won't clone the repo again and again
        self.docker_test_setup.mount_dir(
            OsUtils.get_env_value(CdswEnvVar.HADOOP_DEV_DIR.value),
            ContainerDirs.HADOOP_UPSTREAM_BASEDIR,
            mode=MOUNT_MODE_RW,
        )


class DockerBasedTestConfig:
    GLOBAL_SITE_COMMAND = f"{PYTHON3} -c 'import site; print(site.getsitepackages()[0])'"
    USER_SITE_COMMAND = f"{PYTHON3} -m site --user-site"
    # TODO Add flag to control if running initial-cdsw-setup.sh is required or not

    def __init__(
        self,
        create_image: bool,
        mount_cdsw_dirs_from_local: bool,
        run_cdsw_initial_setup_script: bool,
        container_sleep_seconds: int,
    ):
        self.create_image = create_image
        self.mount_cdsw_dirs_from_local = mount_cdsw_dirs_from_local
        self.run_cdsw_initial_setup_scr = run_cdsw_initial_setup_script
        self.container_sleep_seconds = container_sleep_seconds

        # Only global-site mode can work in Docker containers
        # With user mode, the following error is coming up:
        # cp /root/.local/lib/python3.8/site-packages/yarndevtools/cdsw/downstream-branchdiff-reporting/cdsw_runner.py /home/cdsw/jobs//downstream-branchdiff-reporting/cdsw_runner.py
        # cp: cannot stat '/root/.local/lib/python3.8/site-packages/yarndevtools/cdsw/downstream-branchdiff-reporting/cdsw_runner.py'
        # No such file or directory
        self.python_module_mode = PythonModuleMode.GLOBAL

        # Dynamic properties
        self.python_module_root = None
        self.exec_mode: TestExecMode = self.determine_execution_mode()
        self.python_module_mode_query_cmd = self.determine_python_module_mode_query_command()
        self.github_ci_execution: bool = GitHubUtils.is_github_ci_execution()
        self.cdsw_root_dir: str = self.determine_cdsw_root_dir()
        if self.github_ci_execution:
            self.mount_cdsw_dirs_from_local = False
        self.env_dict = self.setup_env_vars()
        self.setup_local_dirs()

    @classmethod
    def determine_execution_mode(cls):
        exec_mode_env: str = OsUtils.get_env_value(
            CdswEnvVar.TEST_EXECUTION_MODE.value, default_value=DEFAULT_TEST_EXECUTION_MODE
        )
        return TestExecMode[exec_mode_env.upper()]

    def determine_python_module_mode_query_command(self) -> str:
        if self.python_module_mode == PythonModuleMode.GLOBAL:
            return DockerBasedTestConfig.GLOBAL_SITE_COMMAND
        elif self.python_module_mode == PythonModuleMode.USER:
            return DockerBasedTestConfig.USER_SITE_COMMAND
        else:
            raise ValueError("Unknown Python module mode: {}".format(self.python_module_mode))

    def determine_cdsw_root_dir(self):
        if self.github_ci_execution:
            # When GitHub Actions CI runs the tests, it returns two or more paths,
            # so it's better to define the path by hand.
            # Example of paths: [
            # '/home/runner/work/yarn-dev-tools/yarn-dev-tools/yarndevtools/cdsw',
            # '/home/runner/work/yarn-dev-tools/yarn-dev-tools/build/lib/yarndevtools/cdsw'
            # ]
            LOG.debug("Github Actions CI execution, crafting CDSW root dir path manually..")
            github_actions_workspace: str = GitHubUtils.get_workspace_path()
            return FileUtils.join_path(github_actions_workspace, YARNDEVTOOLS_MODULE_NAME, CDSW_DIRNAME)

        LOG.debug("Normal test execution, finding project dir..")
        return SimpleProjectUtils.get_project_dir(
            basedir=LocalDirs.REPO_ROOT_DIR,
            parent_dir="yarndevtools",
            dir_to_find=CDSW_DIRNAME,
            find_result_type=FindResultType.DIRS,
        )

    def setup_env_vars(self) -> Dict[str, str]:
        def get_str(key):
            if isinstance(key, str):
                return key
            elif isinstance(key, Enum):
                return key.value
            else:
                raise ValueError("Unknown key type. Should be str or Enum. Type: {}".format(type(key)))

        def make_key(prefix, conf_value):
            return f"{prefix}_{conf_value}"

        p_common = "common"
        p_exec_mode = "exec_mode"
        p_module_mode = "module_mode"
        p_github_ci_execution = "github_ci_execution"
        env_vars = {
            p_common: {
                get_str(ProjectUtilsEnvVar.OVERRIDE_USER_HOME_DIR): FileUtils.join_path("home", CDSW_DIRNAME),
                get_str(CdswEnvVar.MAIL_RECIPIENTS): "nsziszy@gmail.com",
                get_str(CdswEnvVar.TEST_EXECUTION_MODE): self.exec_mode.value,
            },
            # !! WARNING: User-specific settings below !!
            make_key(p_exec_mode, get_str(TestExecMode.CLOUDERA)): {
                # We need both upstream / downstream repos for Cloudera-mode
                get_str(CdswEnvVar.CLOUDERA_HADOOP_ROOT): FileUtils.join_path(
                    CommonDirs.USER_DEV_ROOT, CLOUDERA, HADOOP
                ),
                get_str(CdswEnvVar.HADOOP_DEV_DIR): FileUtils.join_path(CommonDirs.USER_DEV_ROOT, APACHE, HADOOP),
            },
            make_key(p_exec_mode, get_str(TestExecMode.UPSTREAM)): {
                get_str(CdswEnvVar.HADOOP_DEV_DIR): FileUtils.join_path(CommonDirs.USER_DEV_ROOT, APACHE, HADOOP),
                get_str(BranchComparatorEnvVar.REPO_TYPE): RepoType.UPSTREAM.value,
                get_str(BranchComparatorEnvVar.FEATURE_BRANCH): ORIGIN_BRANCH_3_3,
                get_str(BranchComparatorEnvVar.MASTER_BRANCH): ORIGIN_TRUNK,
            },
            make_key(p_module_mode, get_str(PythonModuleMode.GLOBAL)): {
                get_str(CdswEnvVar.PYTHON_MODULE_MODE): PythonModuleMode.GLOBAL.value
            },
            make_key(p_module_mode, get_str(PythonModuleMode.USER)): {
                get_str(CdswEnvVar.PYTHON_MODULE_MODE): PythonModuleMode.USER.value
            },
            make_key(p_github_ci_execution, str(True)): {
                get_str(CdswEnvVar.ENABLE_GOOGLE_DRIVE_INTEGRATION.value): str(False)
            },
            make_key(p_github_ci_execution, str(False)): {
                get_str(CdswEnvVar.ENABLE_GOOGLE_DRIVE_INTEGRATION.value): str(True)
            },
        }

        OsUtils.track_env_updates()
        for k, v in env_vars[p_common].items():
            LOG.debug("Adding common env var. %s=%s", k, v)
            OsUtils.set_env_value(k, v)

        prefix_and_value_tuples = [
            (p_exec_mode, self.exec_mode.value),
            (p_module_mode, self.python_module_mode.value),
            (p_github_ci_execution, str(self.github_ci_execution)),
        ]
        for prefix, conf_value in prefix_and_value_tuples:
            dict_key: str = make_key(prefix, conf_value)
            for k, v in env_vars[dict_key].items():
                LOG.debug("Adding %s=%s-based env var. %s=%s", prefix, self.exec_mode.value, k, v)
                OsUtils.set_env_value(k, v)

        tracked_env_updates: Dict[str, str] = OsUtils.get_tracked_updates()
        OsUtils.stop_tracking_updates(clear_updates_dict=True)
        env_keys = set(tracked_env_updates.keys())
        env_keys.update(
            {
                get_str(CdswEnvVar.MAIL_ACC_USER),
                get_str(CdswEnvVar.MAIL_ACC_PASSWORD),
                get_str(EnvVar.IGNORE_SMTP_AUTH_ERROR),
            }
        )

        env_dict = {env_name: OsUtils.get_env_value(env_name) for env_name in env_keys}
        return env_dict

    def setup_local_dirs(self):
        LocalDirs.CDSW_ROOT_DIR = self.cdsw_root_dir
        LocalDirs.SCRIPTS_DIR = FileUtils.join_path(LocalDirs.CDSW_ROOT_DIR, "scripts")
        LocalDirs.YARNDEVTOOLS_RESULT_DIR = FileUtils.join_path(LocalDirs.CDSW_ROOT_DIR, "yarndevtools-results")
        LOG.info("Local dirs: %s", ObjUtils.get_static_fields_with_values(LocalDirs))
        LOG.info("Container files: %s", ObjUtils.get_static_fields_with_values(ContainerFiles))
        LOG.info("Container dirs: %s", ObjUtils.get_static_fields_with_values(ContainerDirs))


PROD_CONFIG = DockerBasedTestConfig(
    create_image=True, mount_cdsw_dirs_from_local=False, run_cdsw_initial_setup_script=True, container_sleep_seconds=200
)
DEV_CONFIG = DockerBasedTestConfig(
    create_image=False,
    mount_cdsw_dirs_from_local=True,
    run_cdsw_initial_setup_script=False,
    container_sleep_seconds=500,
)
ACTIVE_CONFIG = DEV_CONFIG  # <-- !!! CHANGE THE ACTIVE CONFIG HERE !!!


class YarnCdswBranchDiffTests(unittest.TestCase):
    docker_test_setup = None
    docker_mounts = None
    config: DockerBasedTestConfig = ACTIVE_CONFIG

    @classmethod
    def setUpClass(cls):
        ProjectUtils.set_root_determine_strategy(ProjectRootDeterminationStrategy.COMMON_FILE)
        ProjectUtils.get_test_output_basedir(PROJECT_NAME)
        if CdswEnvVar.MAIL_ACC_PASSWORD.value not in os.environ:
            raise ValueError(f"Please set '{CdswEnvVar.MAIL_ACC_PASSWORD.value}' env var and re-run the test!")
        cls._setup_logging()
        if cls.config.github_ci_execution:
            dockerfile = FileUtils.join_path(LocalDirs.CDSW_ROOT_DIR, "Dockerfile-github")
        else:
            dockerfile = FileUtils.join_path(LocalDirs.CDSW_ROOT_DIR, "Dockerfile")
        cls.docker_test_setup = DockerTestSetup(
            DOCKER_IMAGE, create_image=cls.config.create_image, dockerfile=dockerfile, logger=CMD_LOG
        )
        cls.docker_mounts = DockerMounts(
            cls, cls.docker_test_setup, cls.config.exec_mode, cls.config.python_module_mode
        )
        cls.docker_mounts.setup_default_docker_mounts()

    @classmethod
    def _setup_logging(cls):
        loggging_setup: SimpleLoggingSetupConfig = SimpleLoggingSetup.init_logger(
            project_name=CommandType.BRANCH_COMPARATOR.real_name,
            logger_name_prefix=YARNDEVTOOLS_MODULE_NAME,
            execution_mode=ExecutionMode.TEST,
            console_debug=True,
            format_str="%(message)s",
        )
        CMD_LOG.propagate = False
        CMD_LOG.addHandler(loggging_setup.console_handler)

    @classmethod
    def exec_branch_diff_script(cls, args="", env: Dict[str, str] = None):
        return cls.docker_test_setup.exec_cmd_in_container(
            f"{PYTHON3} {ContainerFiles.BRANCH_DIFF_SCRIPT} {args}", stdin=False, tty=False, env=env
        )

    @classmethod
    def exec_initial_cdsw_setup_script(cls, args: List[str] = None, env: Dict[str, str] = None):
        if not args:
            args = []
        args.append(cls.config.python_module_mode.value)
        args.append(cls.config.exec_mode.value)
        args_str = " ".join(args)
        return cls.docker_test_setup.exec_cmd_in_container(
            f"{BASH} {ContainerFiles.INITIAL_CDSW_SETUP_SCRIPT} {args_str}", stdin=False, tty=False, env=env
        )

    @classmethod
    def exec_get_python_module_root(cls, env: Dict[str, str] = None, callback=None):
        return cls.docker_test_setup.exec_cmd_in_container(
            cls.config.python_module_mode_query_cmd, stdin=False, tty=False, env=env, callback=callback
        )

    def setUp(self):
        self.docker_test_setup.test_instance = self

    def tearDown(self) -> None:
        self.docker_test_setup.cleanup()

    def save_latest_zip_from_container(self):
        zip_link = FileUtils.join_path(LocalDirs.YARNDEVTOOLS_RESULT_DIR, "latest-command-data-zip")
        cont_src_path = os.readlink(zip_link)
        local_target_path = FileUtils.join_path(LocalDirs.YARNDEVTOOLS_RESULT_DIR, "latest-command-data-real.zip")
        self.docker_test_setup.docker_cp_from_container(cont_src_path, local_target_path)

    def copy_yarndevtools_cdsw_recursively(self):
        local_dir = LocalDirs.CDSW_ROOT_DIR
        container_target_path = FileUtils.join_path(
            self.config.python_module_root, YARNDEVTOOLS_MODULE_NAME, CDSW_DIRNAME
        )
        local_dir_docker_cp_arg = self._convert_to_docker_cp_dir_contents_copy_path(local_dir)
        self.docker_test_setup.docker_cp_to_container(container_target_path, local_dir_docker_cp_arg)

    @staticmethod
    def _convert_to_docker_cp_dir_contents_copy_path(path):
        # As per the user guide of docker cp: https://docs.docker.com/engine/reference/commandline/cp/#extended-description
        # 1. SRC_PATH specifies a directory
        # 2. DEST_PATH exists and is a directory
        # 3. SRC_PATH does end with /. (that is: slash followed by dot)
        # 4. OUTCOME: the content of the source directory is copied into this directory
        return path + os.sep + "."

    def test_basic_cdsw_runner(self):
        def _callback(cmd, cmd_output, docker_setup):
            self.config.python_module_root = cmd_output

        self.docker_mounts.setup_default_docker_mounts()
        self.docker_test_setup.run_container(sleep=self.config.container_sleep_seconds)
        self.exec_get_python_module_root(callback=_callback)
        self.exec_initial_cdsw_setup_script()
        if self.config.mount_cdsw_dirs_from_local:
            # TODO Copy pythoncommons, googleapiwrapper as well, control this with an enum
            self.copy_yarndevtools_cdsw_recursively()

        # Instead of mounting, copy the file as google-api-wrapper would write token pickle
        # so it basically requires this to be mounted with 'RW' which we don't want to do to pollute the local FS
        local_dir_docker_cp_arg = self._convert_to_docker_cp_dir_contents_copy_path(LocalDirs.CDSW_SECRET_DIR)
        self.docker_test_setup.docker_cp_to_container(
            ContainerDirs.CDSW_SECRET_DIR,
            local_dir_docker_cp_arg,
            create_container_path_mode=CreatePathMode.FULL_PATH,
            double_check_with_ls=True,
        )

        exit_code = self.exec_branch_diff_script(env=self.config.env_dict)
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
                # TODO IS this really the exit code or the stdout of pgrep returned?
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
