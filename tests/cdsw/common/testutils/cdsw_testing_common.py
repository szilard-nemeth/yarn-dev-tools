import logging
import os
import unittest
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Set, Dict
from unittest.mock import _CallList, patch, Mock

from dotenv import dotenv_values
from googleapiwrapper.google_auth import GoogleApiAuthorizer
from googleapiwrapper.google_drive import (
    DuplicateFileWriteResolutionMode,
    DriveApiWrapperSessionSettings,
    FileFindMode,
    DriveApiWrapper,
)
from pythoncommons.file_utils import FileUtils, FindResultType
from pythoncommons.github_utils import GitHubUtils
from pythoncommons.object_utils import ObjUtils
from pythoncommons.project_utils import SimpleProjectUtils

from tests.test_utilities import Object
from yarndevtools.cdsw.cdsw_common import GoogleDriveCdswHelper, CDSW_PROJECT
from yarndevtools.cdsw.constants import SECRET_PROJECTS_DIR
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME, PYTHON3

MANY_PARAMS = 9999

DRIVE_API_WRAPPER_PATH = "googleapiwrapper.google_drive.DriveApiWrapper"

ESCAPED_ARGS = {"--aggregate-filters"}
ESCAPED_ARGS_TUPLE = tuple(ESCAPED_ARGS)


class ArgumentType(Enum):
    NORMAL = "normal"
    QUOTE_BASED = "quote_based"


NO_ARG = (0, ArgumentType.NORMAL)

MANY_NORMAL_ARGS = ("many", ArgumentType.NORMAL)
MANY_QUOTE_BASED_ARGS = ("many", ArgumentType.QUOTE_BASED)
SINGLE_NORMAL_ARG = (1, ArgumentType.NORMAL)
SINGLE_QUOTE_BASED_ARG = (1, ArgumentType.QUOTE_BASED)

GSHEET_ARGUMENTS = {
    "--gsheet-client-secret": SINGLE_NORMAL_ARG,
    "--gsheet-worksheet": SINGLE_NORMAL_ARG,
    "--gsheet-spreadsheet": SINGLE_NORMAL_ARG,
}
GSHEET_ARGUMENTS_ADDITIONAL = {
    "--gsheet-jira-column": SINGLE_NORMAL_ARG,
    "--gsheet-spreadsheet": SINGLE_NORMAL_ARG,
    "--gsheet-worksheet": SINGLE_NORMAL_ARG,
    "--gsheet-update-date-column": SINGLE_NORMAL_ARG,
    "--gsheet-status-info-column": SINGLE_NORMAL_ARG,
}

COMMAND_ARGUMENTS_COMMON = {
    PYTHON3: 0,
    "--gsheet": NO_ARG,
    "--debug": NO_ARG,
    "--prepend_email_body_with_text": SINGLE_QUOTE_BASED_ARG,
}
COMMAND_ARGUMENTS_EMAIL = {
    "--smtp_server": SINGLE_NORMAL_ARG,
    "--smtp_port": SINGLE_NORMAL_ARG,
    "--account_user": SINGLE_NORMAL_ARG,
    "--account_password": SINGLE_NORMAL_ARG,
    "--subject": SINGLE_QUOTE_BASED_ARG,
    "--sender": SINGLE_QUOTE_BASED_ARG,
    "--recipients": MANY_NORMAL_ARGS,
    "--attachment-filename": SINGLE_NORMAL_ARG,
}
COMMAND_ARGUMENTS_COMMON.update({ct.name: 0 for ct in CommandType})

COMMAND_ARGUMENTS = {
    CommandType.ZIP_LATEST_COMMAND_DATA: {"--dest_dir": SINGLE_NORMAL_ARG, "--ignore-filetypes": MANY_NORMAL_ARGS},
    CommandType.SEND_LATEST_COMMAND_DATA: {
        **COMMAND_ARGUMENTS_EMAIL,
        "--file-as-email-body-from-zip": SINGLE_NORMAL_ARG,
        "--prepend_email_body_with_text": SINGLE_NORMAL_ARG,
        "--send-attachment": NO_ARG,
    },
    CommandType.UNIT_TEST_RESULT_AGGREGATOR: {
        **GSHEET_ARGUMENTS,
        "--account-email": SINGLE_NORMAL_ARG,
        "--request-limit": SINGLE_NORMAL_ARG,
        "--match-expression": MANY_NORMAL_ARGS,
        "--gmail-query": SINGLE_QUOTE_BASED_ARG,
        "--summary-mode": SINGLE_NORMAL_ARG,
        "--skip-lines-starting-with": MANY_QUOTE_BASED_ARGS,
        "--smart-subject-query": NO_ARG,
        "--abbreviate-testcase-package": SINGLE_NORMAL_ARG,
        "--aggregate-filters": MANY_NORMAL_ARGS,
        "--gsheet-compare-with-jira-table": SINGLE_QUOTE_BASED_ARG,
    },
    CommandType.UNIT_TEST_RESULT_FETCHER: {
        **COMMAND_ARGUMENTS_EMAIL,
        "--mode": SINGLE_NORMAL_ARG,
        "--jenkins-url": SINGLE_NORMAL_ARG,
        "--job-names": MANY_NORMAL_ARGS,
        "--testcase-filter": MANY_NORMAL_ARGS,
        "--num-builds": SINGLE_NORMAL_ARG,
        "--omit-job-summary": NO_ARG,
        "--download-uncached-job-data": NO_ARG,
        "--request-limit": SINGLE_NORMAL_ARG,
        "--cache-type": SINGLE_NORMAL_ARG,
        "--jenkins-user": SINGLE_NORMAL_ARG,
        "--jenkins-password": SINGLE_NORMAL_ARG,
    },
    CommandType.BRANCH_COMPARATOR: {
        "--commit_author_exceptions": MANY_NORMAL_ARGS,
        "--console-mode": NO_ARG,
        "--run-legacy-script": NO_ARG,
        "--repo-type": SINGLE_NORMAL_ARG,
    },
    CommandType.JIRA_UMBRELLA_DATA_FETCHER: {
        "--ignore-changes": NO_ARG,
        "--add-common-upstream-branches": NO_ARG,
        "--branches": MANY_NORMAL_ARGS,
        "--force-mode": NO_ARG,
    },
    CommandType.REVIEW_SHEET_BACKPORT_UPDATER: {
        **GSHEET_ARGUMENTS,
        **GSHEET_ARGUMENTS_ADDITIONAL,
        "--verbose": NO_ARG,
        "--branches": MANY_NORMAL_ARGS,
    },
    CommandType.REVIEWSYNC: {
        **GSHEET_ARGUMENTS,
        **GSHEET_ARGUMENTS_ADDITIONAL,
        "--branches": MANY_NORMAL_ARGS,
        "--verbose": NO_ARG,
        "--issues": MANY_NORMAL_ARGS,
    },
}


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
    command_type = None
    exact_command_expectation = None
    fake_command = None

    @staticmethod
    def _extract_param_count_for_arg_from_dict(d, arg):
        value = d[arg][0]
        if isinstance(value, str):
            if value == "many":
                return MANY_PARAMS
        elif isinstance(value, int):
            if value > 1:
                return MANY_PARAMS
            return value

    def _extract_param_count_for_arg(self, command_type: CommandType, arg: str):
        if arg in COMMAND_ARGUMENTS_COMMON:
            return CommandExpectations._extract_param_count_for_arg_from_dict(COMMAND_ARGUMENTS_COMMON, arg)
        elif command_type in COMMAND_ARGUMENTS and arg in COMMAND_ARGUMENTS[command_type]:
            return CommandExpectations._extract_param_count_for_arg_from_dict(COMMAND_ARGUMENTS[command_type], arg)
        elif self.fake_command and command_type is None:
            # Assuming one paramter per arg
            return 1
        else:
            raise ValueError("Unknown argument '{}' of command type {}".format(arg, command_type))

    def does_arg_has_one_param(self, command_type: CommandType, arg: str):
        count = self._extract_param_count_for_arg(command_type, arg)
        if count == 1:
            return True
        return False

    def does_arg_has_many_params(self, command_type: CommandType, arg: str):
        count = self._extract_param_count_for_arg(command_type, arg)
        if count == MANY_PARAMS:
            return True
        return False

    def is_arg_quote_based(self, command_type: CommandType, arg: str):
        if arg in COMMAND_ARGUMENTS_COMMON:
            return COMMAND_ARGUMENTS_COMMON[arg][1] == ArgumentType.QUOTE_BASED
        elif command_type in COMMAND_ARGUMENTS and arg in COMMAND_ARGUMENTS[command_type]:
            return COMMAND_ARGUMENTS[command_type][arg][1] == ArgumentType.QUOTE_BASED
        elif self.fake_command:
            return False
        else:
            raise ValueError("Unknown argument '{}' of command type {}".format(arg, command_type))

    def add_expected_arg(self, argument, param: str = None):
        s = argument
        if param:
            s = f"{s} {param}"
        self.arguments_with_any_order.append(s)
        return self

    def add_expected_args(self, argument, *params: List[str]):
        s = argument
        if params:
            parameters = " ".join(params)
            s = f"{s} {parameters}"
        self.arguments_with_any_order.append(s)
        return self

    def add_expected_arg_at_position(self, argument, pos: int):
        self.arguments_in_order.insert(pos, argument)
        return self

    def add_expected_ordered_arg(self, argument):
        self.arguments_in_order.append(argument)
        return self

    def with_exact_command_expectation(self, exp_cmd):
        self.exact_command_expectation = exp_cmd
        return self

    def with_command_type(self, cmd_type: CommandType):
        self.command_type = cmd_type
        return self

    def with_fake_command(self):
        self.fake_command = True
        return self

    def verify_command(self, command):
        LOG.info("Verifying command: %s", command)

        if self.exact_command_expectation:
            if self.arguments_with_any_order or self.arguments_in_order:
                raise ValueError(
                    "Invalid expectation! Exact command expectation is set to True, but found argument expectations as well. "
                    "Current expectation object: {}".format(self)
                )
            self.testcase.assertEqual(self.exact_command_expectation, command)
        else:
            if not self.arguments_in_order and not self.arguments_with_any_order:
                raise ValueError("Expectation argument lists are both empty!")
            expected_args: Dict[str, Set[str]] = {
                **self._split_by(self.arguments_with_any_order),
                **self._split_by(self.arguments_in_order),
            }
            actual_args: Dict[str, Set[str]] = self.extract_args_from_command(command, self.command_type)

            # Check set of args first
            self.testcase.assertEqual(expected_args, actual_args)

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

    def _split_by(self, lst: List[str]) -> Dict[str, Set[str]]:
        LOG.debug("Splitting arguments: %s", lst)
        result: Dict[str, Set[str]] = {}
        for arg in lst:
            if arg.startswith("--"):
                spaces = arg.count(" ")
                if spaces == 0:
                    # Format: --arg
                    result[arg] = set()
                elif spaces == 1:
                    # Format: --arg <arguments>
                    split = arg.split(" ")
                    arg_name = split[0]
                    if not self.does_arg_has_one_param(self.command_type, arg_name) and len(split[1:]) > 1:
                        raise ValueError(
                            "Argument '{}' for commandType '{}' was expected to have 1 argument. Found: {}".format(
                                arg,
                                self.command_type,
                                CommandExpectations._extract_param_count_for_arg(self.command_type, arg),
                            )
                        )
                    result[arg_name] = set(split[1:])
                else:
                    # # Format: --arg <argument1> <argument2> ... <argumentN>
                    split = arg.split(" ")
                    arg_name = split[0]
                    joined_args = " ".join(split[1:])
                    if self.does_arg_has_many_params(self.command_type, arg_name):
                        wrap_to_quotes = True if '"' in joined_args else False
                        split_by_quotes = joined_args.split('"')
                        # Drop empty lines
                        split_by_quotes = CommandExpectations._drop_empty_lines(
                            split_by_quotes, wrap_to_quotes=wrap_to_quotes
                        )

                        if len(split_by_quotes) == 1:
                            result[arg_name] = set(split_by_quotes[0].split(" "))
                        else:
                            result[arg_name] = set(split_by_quotes)
                    else:
                        result[arg_name] = {joined_args}
            else:
                result[arg] = set()
        if not result:
            raise ValueError("Empty results!")
        return result

    # TODO this should be a new class with stored state
    def extract_args_from_command(self, command, command_type: CommandType):
        result = {}
        arg_with_param = None
        params_for_arg = set()
        one_param, many_params = False, False

        command_parts = command.split(" ")
        quoted_param_handler = QuotedParamHandler()

        for arg in command_parts:
            if arg.startswith("--"):
                one_param = self.does_arg_has_one_param(command_type, arg)
                many_params = self.does_arg_has_many_params(command_type, arg)
                quoted_param_handler.set_inside_param(self.is_arg_quote_based(command_type, arg))

                if arg_with_param:
                    # New argument starts, close multi_param_arg
                    if len(params_for_arg) == 1:
                        val = params_for_arg.pop().rstrip()
                        params_for_arg.add(val)
                    result[arg_with_param] = params_for_arg
                    arg_with_param = None
                    params_for_arg = set()

                    # Add new arg to dict
                    result[arg] = set()
                if one_param or many_params:
                    arg_with_param = arg
                else:
                    # Normal argument
                    result[arg] = set()
                    params_for_arg = set()
            elif one_param or many_params:
                # Save current arg as multi parameter argument if it has 1 or more params
                quote_based = self.is_arg_quote_based(command_type, arg_with_param)
                if not quoted_param_handler.is_inside_param and quote_based:
                    quoted_param_handler.set_inside_param(True)
                if quoted_param_handler.is_inside_param:
                    qparam = quoted_param_handler.handle(arg)
                    if qparam:
                        params_for_arg.add(qparam)
                elif many_params:
                    # Parameter for multi parameter argument
                    params_for_arg.add(arg)
                else:
                    # One param
                    val = params_for_arg.pop() if len(params_for_arg) == 1 else ""
                    if val:
                        val += f"{arg} "
                    else:
                        val = arg
                    params_for_arg.add(val)
                    one_param = False
                    result[arg_with_param] = params_for_arg
                    arg_with_param = None
                    params_for_arg = set()
            else:
                # Normal argument, without parameter
                result[arg] = set()
                params_for_arg = set()
        if len(params_for_arg) > 0:
            if len(params_for_arg) == 1:
                val = params_for_arg.pop().rstrip()
                params_for_arg.add(val)
            result[arg_with_param] = params_for_arg
        return result

    @staticmethod
    def _drop_empty_lines(lines, wrap_to_quotes=False):
        def process_line(s, wrap=False):
            if wrap:
                return '"' + s + '"'
            return s

        return [process_line(line, wrap=wrap_to_quotes) for line in [line.strip() for line in lines] if line]


class QuotedParamHandler:
    def __init__(self):
        self.param_string = ""
        self.param_quote_type = None
        self.inside_param = False

    @property
    def is_inside_param(self):
        return self.inside_param

    def set_inside_param(self, inside: bool):
        if self.inside_param and inside:
            raise ValueError(
                "Invalid state of {}. inside_param was already True "
                "and inside is set to True again!".format(self.__class__.__name__)
            )
        self.inside_param = inside

    def handle(self, arg) -> str or None:
        if self._is_param_closed(arg):
            # Param closed, --> "
            self.param_string += f" {arg}"
            return self._finish_param()
        elif self._is_complete_param(arg):
            # Complete param, e.g. "param" or 'param'
            self.param_string = arg
            return self._finish_param()
        if arg.startswith('"') or arg.startswith("'"):
            # Param starts
            self.param_string = arg
            self.param_quote_type = arg[0]
        else:
            if self._is_param_with_quote_inside(arg):
                # param with quote inside, e.g. subject:"YARN
                self.param_quote_type = "'" if "'" in arg else '"'
                self.param_string = arg
            else:
                # Continuation of quoted param
                return self._handle_continuation_of_param(arg)

    def _handle_continuation_of_param(self, arg):
        self.param_string += f" {arg}"
        if arg.endswith(self.param_quote_type):
            # Param closed, e.g. param"
            return self._finish_param()
        return None

    def _is_param_with_quote_inside(self, arg):
        return not self.param_string and ("'" in arg or '"' in arg)

    def _is_param_closed(self, arg):
        return self.param_string and (arg == self.param_quote_type or arg.endswith(self.param_quote_type))

    @staticmethod
    def _is_complete_param(arg):
        return (arg.startswith('"') and arg.endswith('"')) or arg.startswith("'") and arg.endswith("'")

    def _finish_param(self):
        tmp = self.param_string
        self._reset_state()
        return tmp

    def _reset_state(self):
        self.param_string = ""
        self.param_quote_type = None
        self.inside_param = False


class SecretsResolver:
    def __init__(self, github_ci_execution):
        self.github_ci_execution = github_ci_execution
        self.secrets = self._load_secrets()

    def _load_secrets(cls):
        return dotenv_values(verbose=True)

    def get(self, name: str):
        if self.github_ci_execution:
            if name not in os.environ:
                raise ValueError("Failed to resolve secret, undefined env var: {}".format(name))
            return os.environ[name]
        else:
            return self.secrets[name]


class CdswTestingCommons:
    def __init__(self):
        self.github_ci_execution: bool = GitHubUtils.is_github_ci_execution()
        self.secrets_resolver = SecretsResolver(self.github_ci_execution)
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
        for i in range(len(actual_commands)):
            actual_command = actual_commands[i]
            expectation = expectations[i]
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
