import logging
import os
import unittest
from git import InvalidGitRepositoryError, Repo, GitCommandError, Actor
from pythoncommons.file_utils import FileUtils
from pythoncommons.patch_utils import PatchUtils
from pythoncommons.project_utils import ProjectUtils

from yarndevtools.constants import (
    HADOOP_REPO_APACHE,
    TRUNK,
    PROJECT_NAME,
    JIRA_UMBRELLA_DATA,
    ExecutionMode,
    ORIGIN_TRUNK,
)
from pythoncommons.git_constants import HEAD, ORIGIN
from pythoncommons.git_wrapper import GitWrapper, ProgressPrinter
from yarndevtools.yarn_dev_tools import Setup

SANDBOX_REPO = "sandbox_repo"
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
    sandbox_repo_path = None
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
        return ProjectUtils.get_test_output_child_dir(JIRA_UMBRELLA_DATA)

    def set_env_vars(self, upstream_repo, downstream_repo):
        os.environ["HADOOP_DEV_DIR"] = upstream_repo
        os.environ["CLOUDERA_HADOOP_ROOT"] = downstream_repo

    def setUpClass(self, repo_postfix=None, init_logging=True):
        if repo_postfix:
            self.repo_postfix = repo_postfix
        ProjectUtils.get_test_output_basedir(PROJECT_NAME)
        try:
            self.setup_repo()
            self.repo_wrapper.setup_pull_mode_no_ff()
            if init_logging:
                Setup.init_logger(execution_mode=ExecutionMode.TEST, console_debug=False, repos=[self.repo])
            self.reset_and_checkout_trunk()
        except InvalidGitRepositoryError:
            LOG.info(f"Cloning repo '{HADOOP_REPO_APACHE}' for the first time...")
            Repo.clone_from(HADOOP_REPO_APACHE, self.sandbox_repo_path, progress=ProgressPrinter("clone"))
            self.setup_repo(log=False)
            self.reset_and_checkout_trunk()

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

    def pull_to_trunk(self):
        self.repo_wrapper.checkout_and_pull(TRUNK, remote_to_pull=ORIGIN)

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

    def assert_files_not_empty(self, basedir, expected_files=None):
        found_files = FileUtils.find_files(basedir, regex=".*", single_level=True, full_path_result=True)
        for f in found_files:
            self.assert_file_not_empty(f)
        if expected_files:
            TESTCASE.assertEqual(expected_files, len(found_files))

    @staticmethod
    def assert_file_not_empty(f):
        TESTCASE.assertTrue(os.path.getsize(f) > 0)
