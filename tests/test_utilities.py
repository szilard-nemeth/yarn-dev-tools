import logging
import os
import unittest

from git import InvalidGitRepositoryError, Repo, GitCommandError, Actor
from pythoncommons.constants import ExecutionMode
from pythoncommons.file_utils import FileUtils
from pythoncommons.github_utils import GitHubUtils
from pythoncommons.logging_setup import SimpleLoggingSetup
from pythoncommons.os_utils import OsUtils
from pythoncommons.patch_utils import PatchUtils
from pythoncommons.project_utils import ProjectUtils, ProjectRootDeterminationStrategy, PROJECTS_BASEDIR
from pythoncommons.zip_utils import ZipFileUtils

from yarndevtools.common.shared_command_utils import YarnDevToolsTestEnvVar, CommandType
from yarndevtools.constants import (
    HADOOP_REPO_APACHE,
    TRUNK,
    ORIGIN_TRUNK,
    YARNDEVTOOLS_MODULE_NAME,
    ENV_HADOOP_DEV_DIR,
    ENV_CLOUDERA_HADOOP_ROOT,
)
from pythoncommons.git_constants import HEAD, ORIGIN
from pythoncommons.git_wrapper import GitWrapper, ProgressPrinter

SANDBOX_REPO = "sandbox_repo"
SANDBOX_REPO_DOWNSTREAM_HOTFIX = "_downstream"
DUMMY_PATCHES = "dummy-patches"
SAVED_PATCHES = "saved-patches"

DUMMYFILE_1 = "dummyfile1"
DUMMYFILE_2 = "dummyfile2"

LOG = logging.getLogger(__name__)
YARNCONFIGURATION_PATH = (
    "hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java"
)

TESTCASE = unittest.TestCase("__init__")


class Object(object):
    def __contains__(self, key):
        return key in self.__dict__


class TestUtilities:
    repo = None
    base_branch = TRUNK

    def __init__(self, test_instance, test_branch):
        self.test_instance = test_instance
        self.test_branch = test_branch
        self.repo_postfix = ""

    @property
    def sandbox_repo_path(self):
        return ProjectUtils.get_test_output_child_dir(SANDBOX_REPO + self.repo_postfix)

    @property
    def saved_patches_dir(self):
        return ProjectUtils.get_test_output_child_dir(SAVED_PATCHES)

    @property
    def dummy_patches_dir(self):
        return ProjectUtils.get_test_output_child_dir(DUMMY_PATCHES)

    @property
    def jira_umbrella_data_dir(self):
        return ProjectUtils.get_test_output_child_dir(CommandType.JIRA_UMBRELLA_DATA_FETCHER.output_dir_name)

    def set_env_vars(self, upstream_repo, downstream_repo):
        OsUtils.set_env_value(ENV_HADOOP_DEV_DIR, upstream_repo)
        OsUtils.set_env_value(ENV_CLOUDERA_HADOOP_ROOT, downstream_repo)

    def setUpClass(self, command_type: CommandType, repo_postfix=None, init_logging=True, console_debug=False):
        if repo_postfix:
            self.repo_postfix = repo_postfix
        ProjectUtils.set_root_determine_strategy(ProjectRootDeterminationStrategy.COMMON_FILE)
        ProjectUtils.get_test_output_basedir(YARNDEVTOOLS_MODULE_NAME)
        try:
            self.setup_repo()
            if init_logging:
                SimpleLoggingSetup.init_logger(
                    project_name=command_type.real_name,
                    logger_name_prefix=YARNDEVTOOLS_MODULE_NAME,
                    execution_mode=ExecutionMode.TEST,
                    console_debug=console_debug,
                    repos=[self.repo],
                )
            self.repo_wrapper.setup_pull_mode_ff_only(global_mode=True)
            LOG.info("Git config: %s", self.repo_wrapper.read_config(global_mode=True))
            self.reset_and_checkout_trunk()
        except InvalidGitRepositoryError:
            LOG.info(f"Cloning repo '{HADOOP_REPO_APACHE}' for the first time...")
            Repo.clone_from(HADOOP_REPO_APACHE, self.sandbox_repo_path, progress=ProgressPrinter("clone"))
            self.setup_repo(log=False)
            self.reset_and_checkout_trunk()

    @staticmethod
    def tearDownClass(test_name, command_type):
        TestUtilities.collect_and_zip_test_artifacts(test_name, command_type)

    @staticmethod
    def collect_and_zip_test_artifacts(test_name, command_type):
        github_ci_exec: bool = GitHubUtils.is_github_ci_execution()
        github_workspace_path = None
        if github_ci_exec:
            github_workspace_path = GitHubUtils.get_workspace_path()
        if OsUtils.get_env_value(YarnDevToolsTestEnvVar.FORCE_COLLECTING_ARTIFACTS.value) or github_ci_exec:
            output_export_basedir = (
                github_workspace_path
                if github_ci_exec
                else ProjectUtils.get_output_basedir(YARNDEVTOOLS_MODULE_NAME, basedir=PROJECTS_BASEDIR)
            )
            output_export_basedir = FileUtils.ensure_dir_created(
                FileUtils.join_path(output_export_basedir, YARNDEVTOOLS_MODULE_NAME + "_export")
            )
            LOG.info("Export artifacts output basedir is: %s", output_export_basedir)

            # Keep track of zip files
            all_zip_files = []
            # Export logs to a zip per testcase
            logs_zipfile_path = FileUtils.join_path(output_export_basedir, f"test_logs_{test_name}.zip")
            all_zip_files.append(logs_zipfile_path)
            ZipFileUtils.create_zip_file(
                src_files=SimpleLoggingSetup.get_all_log_files(),
                filename=logs_zipfile_path,
                compress=True,
            )

            for project_name, project_basedir in ProjectUtils.get_project_basedirs_dict().items():
                # Export project basedirs individual zip files per testcase
                project_basedir_zipfile_path = FileUtils.join_path(
                    output_export_basedir, f"test_project_basedir_{project_name}_{test_name}.zip"
                )
                all_zip_files.append(project_basedir_zipfile_path)
                zip_basedir = FileUtils.join_path(project_basedir, command_type.output_dir_name)
                ZipFileUtils.create_zip_file(
                    src_files=[zip_basedir],
                    filename=project_basedir_zipfile_path,
                    ignore_files=[SANDBOX_REPO, SANDBOX_REPO + SANDBOX_REPO_DOWNSTREAM_HOTFIX, "yarndevtools_export"],
                    compress=True,
                )

            # Finally, zip all created zip files into a single zip
            project_basedir_zipfile_path = FileUtils.join_path(output_export_basedir, f"test_artifacts_{test_name}.zip")
            ZipFileUtils.create_zip_file(
                src_files=all_zip_files,
                filename=project_basedir_zipfile_path,
                compress=True,
            )

    def setup_repo(self, log=True):
        # This call will raise InvalidGitRepositoryError in case git repo is not cloned yet to this path
        self.repo_wrapper = GitWrapper(self.sandbox_repo_path)
        self.repo = self.repo_wrapper.repo
        if log:
            LOG.info(f"Repo '{self.repo}' is already cloned to path '{self.sandbox_repo_path}'")

    def reset_and_checkout_trunk(self):
        self.reset_changes()
        self.checkout_trunk()

    def checkout_trunk(self):
        LOG.info(f"Checking out branch: {TRUNK}")
        self.repo_wrapper.checkout_branch(TRUNK)

    def cleanup_and_checkout_test_branch(self, branch=None, remove=True, pull=True, checkout_from=None):
        if not branch:
            if not self.test_branch:
                raise ValueError("Test branch must be set!")
            branch = self.test_branch
        self.reset_changes()
        if pull:
            self.pull_to_trunk()
        try:
            if branch in self.repo_wrapper.get_all_branch_names():
                LOG.info(f"Resetting changes on branch (hard reset): {branch}")
                self.repo_wrapper.checkout_branch(branch)
                self.repo_wrapper.reset(hard=True)

                if branch != self.base_branch:
                    if remove:
                        self.repo_wrapper.remove_branch(branch, checkout_before_remove=TRUNK)
        except GitCommandError:
            # Do nothing if branch does not exist
            LOG.exception("Failed to remove branch.", exc_info=True)
            pass

        if branch != self.base_branch:
            base_ref = checkout_from if checkout_from else self.base_branch
            self.repo_wrapper.checkout_new_branch(branch, base_ref)
        else:
            LOG.info(f"Checking out branch: {branch}")
            self.checkout_trunk()

    def pull_to_trunk(self, no_ff=False, ff_only=False):
        self.repo_wrapper.checkout_and_pull(TRUNK, remote_to_pull=ORIGIN, no_ff=no_ff, ff_only=ff_only)

    def reset_and_checkout_existing_branch(self, branch, pull=True):
        self.reset_changes()
        if pull:
            self.pull_to_trunk()
        self.repo_wrapper.checkout_branch(branch)

    def reset_changes(self):
        self.repo_wrapper.reset_changes(reset_to=ORIGIN_TRUNK, reset_index=True, reset_working_tree=True, clean=True)

    @staticmethod
    def assert_file_contains(file, string):
        if not FileUtils.does_file_contain_str(file, string):
            TESTCASE.fail(f"File '{file}' does not contain expected string: '{string}'")

    def add_some_file_changes(self, commit=False, commit_message_prefix=None):
        FileUtils.save_to_file(FileUtils.join_path(self.sandbox_repo_path, DUMMYFILE_1), DUMMYFILE_1)
        FileUtils.save_to_file(FileUtils.join_path(self.sandbox_repo_path, DUMMYFILE_2), DUMMYFILE_2)
        yarn_config_java = FileUtils.join_path(self.sandbox_repo_path, YARNCONFIGURATION_PATH)
        FileUtils.append_to_file(yarn_config_java, "dummy_changes_to_conf_1\n")
        FileUtils.append_to_file(yarn_config_java, "dummy_changes_to_conf_2\n")

        if commit:
            commit_msg = "test_commit"
            if commit_message_prefix:
                commit_msg = commit_message_prefix + commit_msg
            self.repo_wrapper.commit(
                commit_msg,
                author=Actor("A test author", "unittest@example.com"),
                committer=Actor("A test committer", "unittest@example.com"),
                add_files_to_index=[DUMMYFILE_1, DUMMYFILE_2, yarn_config_java],
            )

    def add_file_changes_and_save_to_patch(self, patch_file):
        self.add_some_file_changes()
        yarn_config_java = FileUtils.join_path(self.sandbox_repo_path, YARNCONFIGURATION_PATH)
        self.repo_wrapper.add_to_index([DUMMYFILE_1, DUMMYFILE_2, yarn_config_java])

        diff = self.repo_wrapper.diff(HEAD, cached=True)
        PatchUtils.save_diff_to_patch_file(diff, patch_file)
        self.reset_changes()

        # Verify file
        self.assert_file_contains(patch_file, "+dummyfile1")
        self.assert_file_contains(patch_file, "+dummyfile2")
        self.assert_file_contains(patch_file, "+dummy_changes_to_conf_1")
        self.assert_file_contains(patch_file, "+dummy_changes_to_conf_2")

    def verify_commit_message_of_branch(self, branch, expected_commit_message, verify_cherry_picked_from=False):
        commit_msg = self.repo_wrapper.get_commit_message_of_branch(branch)
        # Example commit message: 'XXX-1234: YARN-123456: test_commit
        # (cherry picked from commit 51583ec3dbc715f9ff0c5a9b52f1cc7b607b6b26)'
        TESTCASE.assertIn(expected_commit_message, commit_msg)
        if verify_cherry_picked_from:
            TESTCASE.assertIn("cherry picked from commit ", commit_msg)

    def verify_if_branch_is_moved_to_latest_commit(self, branch):
        branch_hash = self.repo_wrapper.get_hash_of_commit(branch)
        head_hash = self.repo_wrapper.repo.head.commit.hexsha
        TESTCASE.assertEqual(branch_hash, head_hash)

    def assert_files_not_empty(self, basedir, expected_files=None):
        found_files = FileUtils.find_files(basedir, regex=".*", single_level=True, full_path_result=True)
        for f in found_files:
            self.assert_file_not_empty(f)
        if expected_files:
            TESTCASE.assertEqual(expected_files, len(found_files))

    @staticmethod
    def assert_file_not_empty(f):
        TESTCASE.assertTrue(os.path.getsize(f) > 0)
