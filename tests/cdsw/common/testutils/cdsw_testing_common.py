import unittest
from dataclasses import dataclass, field
from typing import List, Set
from unittest.mock import _CallList, patch, Mock

from googleapiwrapper.google_auth import GoogleApiAuthorizer
from googleapiwrapper.google_drive import (
    DuplicateFileWriteResolutionMode,
    DriveApiWrapperSessionSettings,
    FileFindMode,
    DriveApiWrapper,
)
from pythoncommons.file_utils import FileUtils, FindResultType
from pythoncommons.github_utils import GitHubUtils
import logging

from pythoncommons.object_utils import ObjUtils
from pythoncommons.project_utils import SimpleProjectUtils

from tests.test_utilities import Object
from yarndevtools.cdsw.cdsw_common import GoogleDriveCdswHelper, CDSW_PROJECT
from yarndevtools.cdsw.constants import SECRET_PROJECTS_DIR
from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME

DRIVE_API_WRAPPER_PATH = "googleapiwrapper.google_drive.DriveApiWrapper"

ESCAPED_ARGS = {"--aggregate-filters"}
ESCAPED_ARGS_TUPLE = tuple(ESCAPED_ARGS)
DO_NOT_SPLIT_ARG_PARAMS = {
    "--prepend_email_body_with_text",
    "--subject",
    "--sender",
    "--aggregate-filters",
    "--gsheet-compare-with-jira-table",
}
DO_NOT_SPLIT_ARG_PARAMS_TUPLE = tuple(DO_NOT_SPLIT_ARG_PARAMS)

TESTS_DIR_NAME = "tests"

CDSW_DIRNAME = "cdsw"
REPO_ROOT_DIRNAME = "yarn-dev-tools"
LOG = logging.getLogger(__name__)


class FakeGoogleDriveCdswHelper(GoogleDriveCdswHelper):
    def __init__(self):
        with patch("googleapiwrapper.google_drive.DriveApiWrapper._build_service") as mock_build_service:
            mock_service = Mock()
            mock_service.files.return_value = ["file1", "file2"]
            mock_build_service.return_value = mock_service
            self.authorizer = self.create_authorizer()
            session_settings = DriveApiWrapperSessionSettings(
                FileFindMode.JUST_UNTRASHED, DuplicateFileWriteResolutionMode.FAIL_FAST, enable_path_cache=True
            )
            self.drive_wrapper = DriveApiWrapper(self.authorizer, session_settings=session_settings)
            self.drive_command_data_basedir = FileUtils.join_path(
                "/tmp", YARNDEVTOOLS_MODULE_NAME, CDSW_PROJECT, "command-data"
            )

    def create_authorizer(self):
        mock_auth = Mock(spec=GoogleApiAuthorizer)
        authed_session = Object()
        authed_session.authed_creds = "creds"
        mock_auth.authorize.return_value = authed_session
        service_type = Object()
        service_type.default_api_version = "1.0"
        service_type.service_name = "fakeService"
        mock_auth.service_type = service_type
        return mock_auth


class LocalDirs:
    REPO_ROOT_DIR = FileUtils.find_repo_root_dir(__file__, REPO_ROOT_DIRNAME)
    CDSW_ROOT_DIR = None
    CDSW_TESTS_DIR = None
    SCRIPTS_DIR = None
    YARNDEVTOOLS_RESULT_DIR = None
    CDSW_SECRET_DIR = FileUtils.join_path(SECRET_PROJECTS_DIR, CDSW_DIRNAME)


@dataclass
class CommandExpectations:
    testcase: unittest.TestCase
    arguments_with_any_order: List[str] = field(default_factory=list)
    arguments_in_order: List[str] = field(default_factory=list)

    def add_expected_arg(self, argument, param: str = None):
        s = argument
        if param:
            s = f"{s} {param}"
        self.arguments_with_any_order.append(s)
        return self

    def add_expected_arg_at_position(self, argument, pos: int):
        self.arguments_in_order.insert(pos, argument)
        return self

    def add_expected_ordered_arg(self, argument):
        self.arguments_in_order.append(argument)
        return self

    def verify_command(self, command):
        LOG.info("Verifying command: %s", command)
        if not self.arguments_in_order and not self.arguments_with_any_order:
            raise ValueError("Expectation argument lists are both empty!")

        expected_args_set: Set[str] = self._get_expected_arguments_as_set()
        actual_args_set: Set[str] = self.extract_args_from_command(command)

        # Check set of args first
        self.testcase.assertEqual(expected_args_set, actual_args_set)

        # Check ordering as well
        indices = []
        for idx, arg in enumerate(self.arguments_in_order):
            indices.append(command.index(arg))
            if idx > 1 and indices[idx] < indices[idx - 1]:
                prev = self.arguments_in_order[indices[idx - 1]]
                self.testcase.fail(
                    "Detected wrong order of arguments. {} should be after {}. "
                    "All expected arguments (In this particular order): {}, "
                    "Command: {}".format(arg, prev, self.arguments_in_order, command)
                )
        arguments_not_found = []
        for arg in self.arguments_with_any_order:
            if arg not in command:
                arguments_not_found.append(arg)

        self.testcase.assertTrue(
            len(arguments_not_found) == 0,
            msg="The following arguments are not found: {}, " "command: {}".format(arguments_not_found, command),
        )

    def _get_expected_arguments_as_set(self):
        set_of_args = {*self._split_by(self.arguments_with_any_order), *self._split_by(self.arguments_in_order)}
        return set_of_args

    @staticmethod
    def _split_by(lst: List[str]):
        lists: List[List[str]] = []
        for arg in lst:
            if arg.startswith(DO_NOT_SPLIT_ARG_PARAMS_TUPLE):
                split = arg.split(" ")
                joined_args = " ".join(split[1:])
                new_list = [split[0], joined_args]
                lists.append(new_list)
            else:
                lists.append(arg.split(" "))
        return [item for sublist in lists for item in sublist]

    @staticmethod
    def extract_args_from_command(command):
        command_parts = command.split(" ")

        args_set = set()
        inside_special_arg = False
        special_arg = ""
        # 22 = {str} '--prepend_email_body_with_text'
        # 23 = {str} '\'<a'
        # 24 = {str} 'href="dummy_link">Command'
        # 25 = {str} 'data'
        # 26 = {str} 'file:'
        # 27 = {str} 'testGoogleDriveApiFilename</a>\''
        for arg in command_parts:
            if inside_special_arg and arg.startswith("--"):
                # New argument starts, close special_arg and add it to set
                inside_special_arg = False
                args_set.add(special_arg)
                args_set.add(arg)
                special_arg = ""
            if arg in DO_NOT_SPLIT_ARG_PARAMS:
                # Found argument that is special
                inside_special_arg = True
                args_set.add(arg)
            elif inside_special_arg:
                if len(special_arg) > 0:
                    special_arg += " "
                special_arg += arg
            else:
                inside_special_arg = False
                args_set.add(arg)
        if inside_special_arg and special_arg != "":
            args_set.add(special_arg)
        return args_set


class CdswTestingCommons:
    def __init__(self):
        self.github_ci_execution: bool = GitHubUtils.is_github_ci_execution()
        self.cdsw_root_dir: str = self.determine_cdsw_root_dir()
        self.setup_local_dirs()
        self.cdsw_tests_root_dir: str = self.determine_cdsw_tests_root_dir()

    def setup_local_dirs(self):
        LocalDirs.CDSW_ROOT_DIR = self.cdsw_root_dir
        LocalDirs.CDSW_TESTS_DIR = SimpleProjectUtils.get_project_dir(
            basedir=LocalDirs.REPO_ROOT_DIR,
            parent_dir="tests",
            dir_to_find=CDSW_DIRNAME,
            find_result_type=FindResultType.DIRS,
            exclude_dirs=["venv", "build"],
        )
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

    @staticmethod
    def verify_commands(tc, expectations: List[CommandExpectations], actual_commands: List[str]):
        tc.assertEqual(
            len(actual_commands),
            len(expectations),
            msg="Not all commands are having expectations set. Commands: {}, Expectations: {}".format(
                actual_commands, expectations
            ),
        )
        for actual_command, expectation in zip(actual_commands, expectations):
            expectation.verify_command(actual_command)

    @staticmethod
    def assert_no_calls_with_arg(tc, call_list: _CallList, arg: str):
        for call in call_list:
            actual_args = list(call.args)
            if arg in actual_args:
                tc.fail("Unexpected call with argument that is forbidden in call: {}".format(arg))

    @staticmethod
    def mock_google_drive():
        with patch(DRIVE_API_WRAPPER_PATH) as MockDriveWrapper:
            instance = MockDriveWrapper.return_value
        instance.upload_file.return_value = "mockedUpload"
        assert MockDriveWrapper() is instance
        assert MockDriveWrapper().upload_file() == "mockedUpload"
