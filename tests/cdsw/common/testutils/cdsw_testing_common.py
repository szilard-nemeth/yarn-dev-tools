from os.path import expanduser

from pythoncommons.file_utils import FileUtils, FindResultType
from pythoncommons.github_utils import GitHubUtils
import logging

from pythoncommons.object_utils import ObjUtils
from pythoncommons.project_utils import SimpleProjectUtils

from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME

TESTS_DIR_NAME = "tests"

CDSW_DIRNAME = "cdsw"
REPO_ROOT_DIRNAME = "yarn-dev-tools"
LOG = logging.getLogger(__name__)

SECRET_PROJECTS_DIR = FileUtils.join_path(expanduser("~"), ".secret", "projects", "cloudera")


class LocalDirs:
    REPO_ROOT_DIR = FileUtils.find_repo_root_dir(__file__, REPO_ROOT_DIRNAME)
    CDSW_ROOT_DIR = None
    SCRIPTS_DIR = None
    YARNDEVTOOLS_RESULT_DIR = None
    CDSW_SECRET_DIR = FileUtils.join_path(SECRET_PROJECTS_DIR, CDSW_DIRNAME)


class CdswTestingCommons:
    def __init__(self):
        self.github_ci_execution: bool = GitHubUtils.is_github_ci_execution()
        self.cdsw_root_dir: str = self.determine_cdsw_root_dir()
        self.setup_local_dirs()
        self.cdsw_tests_root_dir: str = self.determine_cdsw_tests_root_dir()

    def setup_local_dirs(self):
        LocalDirs.CDSW_ROOT_DIR = self.cdsw_root_dir
        LocalDirs.SCRIPTS_DIR = FileUtils.join_path(LocalDirs.CDSW_ROOT_DIR, "scripts")
        LocalDirs.YARNDEVTOOLS_RESULT_DIR = FileUtils.join_path(LocalDirs.CDSW_ROOT_DIR, "yarndevtools-results")
        LOG.info("Local dirs: %s", ObjUtils.get_static_fields_with_values(LocalDirs))

    def get_path_from_test_basedir(self, *path_components):
        return FileUtils.join_path(self.cdsw_tests_root_dir, *path_components)

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
            parent_dir=YARNDEVTOOLS_MODULE_NAME,
            dir_to_find=CDSW_DIRNAME,
            find_result_type=FindResultType.DIRS,
            exclude_dirs=["venv", "build"],
        )

    def determine_cdsw_tests_root_dir(self):
        if self.github_ci_execution:
            LOG.debug("Github Actions CI execution, crafting CDSW testing root dir path manually..")
            github_actions_workspace: str = GitHubUtils.get_workspace_path()
            return FileUtils.join_path(github_actions_workspace, TESTS_DIR_NAME, CDSW_DIRNAME)

        LOG.debug("Normal test execution, finding project dir..")
        return SimpleProjectUtils.get_project_dir(
            basedir=LocalDirs.REPO_ROOT_DIR,
            parent_dir=TESTS_DIR_NAME,
            dir_to_find=CDSW_DIRNAME,
            find_result_type=FindResultType.DIRS,
            exclude_dirs=["venv", "build"],
        )
