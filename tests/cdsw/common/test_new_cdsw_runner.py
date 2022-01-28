import unittest
import logging
from typing import List, Set
from unittest.mock import patch, Mock

from pythoncommons.os_utils import OsUtils
from pythoncommons.string_utils import StringUtils

from tests.test_utilities import Object
from yarndevtools.cdsw.common_python.cdsw_common import CommonFiles
from yarndevtools.cdsw.common_python.cdsw_config import (
    CdswRun,
    CdswJobConfig,
    EmailSettings,
    DriveApiUploadSettings,
    CdswJobConfigReader,
)
from yarndevtools.cdsw.common_python.cdsw_runner import (
    NewCdswRunnerConfig,
    NewCdswRunner,
    ExecutionMode,
    NewCdswConfigReaderAdapter,
)
from yarndevtools.cdsw.common_python.constants import CdswEnvVar
from yarndevtools.common.shared_command_utils import CommandType


DEFAULT_COMMAND_TYPE = CommandType.REVIEWSYNC
# CDSW_RUNNER_CLASSNAME = NewCdswRunner.__name__
CDSW_JOB_CONFIG_READER_CLASS_NAME = CdswJobConfigReader.__name__
# CDSW_RUNNER_BEGIN_PATH = "yarndevtools.cdsw.common_python.cdsw_runner.{}.begin".format(
#     CDSW_RUNNER_CLASSNAME)
CDSW_CONFIG_READER_READ_METHOD_PATH = "yarndevtools.cdsw.common_python.cdsw_config.{}".format(
    CDSW_JOB_CONFIG_READER_CLASS_NAME
)
PARSER = None
SETUP_RESULT = None
CDSW_RUNNER_SCRIPT_PATH = None
LOG = logging.getLogger(__name__)


class CommandExpectations:
    def __init__(self, testcase):
        self.testcase: unittest.TestCase = testcase
        self.arguments_with_any_order = []
        self.arguments_in_order = []

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

        set_of_args = self._get_expected_arguments_as_set()
        actual_args: Set[str] = self.x(command)
        self.testcase.assertEqual(set_of_args, actual_args)

    def _get_expected_arguments_as_set(self):
        set_of_args = {*self._split_by(self.arguments_with_any_order), *self._split_by(self.arguments_in_order)}
        return set_of_args

    @staticmethod
    def _split_by(lst: List[str]):
        lists: List[List[str]] = []
        for arg in lst:
            if arg.startswith("--prepend_email_body_with_text"):
                split = arg.split(" ")
                joined_args = " ".join(split[1:])
                new_list = [split[0], joined_args]
                lists.append(new_list)
            else:
                lists.append(arg.split(" "))
        return [item for sublist in lists for item in sublist]

    def x(self, command):
        command_parts = command.split(" ")

        args_set = set()
        inside_email_body_arg = False
        email_body_arg = ""
        # 22 = {str} '--prepend_email_body_with_text'
        # 23 = {str} '\'<a'
        # 24 = {str} 'href="dummy_link">Command'
        # 25 = {str} 'data'
        # 26 = {str} 'file:'
        # 27 = {str} 'testGoogleDriveApiFilename</a>\''
        for part in command_parts:
            if part == "--prepend_email_body_with_text":
                inside_email_body_arg = True
                args_set.add("--prepend_email_body_with_text")
            elif inside_email_body_arg and part.startswith("--"):
                inside_email_body_arg = False
                # Remove first extra space
                email_body_arg = email_body_arg[1:]
                args_set.add(email_body_arg)
                args_set.add(part)
            elif inside_email_body_arg:
                email_body_arg += " " + part
            else:
                args_set.add(part)
        return args_set


class TestNewCdswRunner(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        CommonFiles.YARN_DEV_TOOLS_SCRIPT = "yarndevtools.py"
        OsUtils.set_env_value(CdswEnvVar.MAIL_ACC_USER.value, "mailUser")
        OsUtils.set_env_value(CdswEnvVar.MAIL_ACC_PASSWORD.value, "mailPassword")

    @staticmethod
    def _create_args_for_auto_discovery(dry_run: bool):
        args = Object()
        args.debug = True
        args.verbose = True
        args.cmd_type = DEFAULT_COMMAND_TYPE.name
        args.dry_run = dry_run
        return args

    @staticmethod
    def _create_args_for_specified_file(config_file: str, dry_run: bool, override_cmd_type: str = None):
        args = Object()
        args.config_file = config_file
        args.debug = True
        args.verbose = True
        if override_cmd_type:
            args.cmd_type = override_cmd_type
        else:
            args.cmd_type = DEFAULT_COMMAND_TYPE.name
        args.dry_run = dry_run
        return args

    def test_argument_parsing_into_config_auto_discovery(self):
        args = self._create_args_for_auto_discovery(dry_run=True)
        config = NewCdswRunnerConfig(None, args)

        self.assertEqual(DEFAULT_COMMAND_TYPE, config.command_type)
        self.assertTrue(config.dry_run)
        self.assertEqual(ExecutionMode.AUTO_DISCOVERY, config.execution_mode)

    def test_argument_parsing_into_config(self):
        args = self._create_args_for_specified_file("fake-config-file.json", dry_run=True)
        config = NewCdswRunnerConfig(PARSER, args)

        self.assertEqual(DEFAULT_COMMAND_TYPE, config.command_type)
        self.assertTrue(config.dry_run)
        self.assertEqual(ExecutionMode.SPECIFIED_CONFIG_FILE, config.execution_mode)
        self.assertEqual("fake-config-file.json", config.job_config_file)

    def test_argument_parsing_into_config_invalid_command_type(self):
        args = self._create_args_for_specified_file(
            "fake-config-file.json", dry_run=True, override_cmd_type="WRONGCOMMAND"
        )
        with self.assertRaises(ValueError) as ve:
            NewCdswRunnerConfig(None, args)
        exc_msg = ve.exception.args[0]
        self.assertIn("Invalid command type specified! Possible values are:", exc_msg)

    def test_execute_runs_single_run(self):
        mock_job_config: CdswJobConfig = Mock(spec=CdswJobConfig)
        mock_job_config.command_type = DEFAULT_COMMAND_TYPE
        mock_run1: CdswRun = Mock(spec=CdswRun)
        mock_run1.yarn_dev_tools_arguments = ["--arg1", "--arg2 bla", "--arg3 bla3"]
        mock_run1.email_settings = EmailSettings(
            enabled=True,
            send_attachment=True,
            attachment_file_name="test_attachment_filename.zip",
            email_body_file_from_command_data="test",
            subject="testSubject",
            sender="testSender",
        )
        mock_run1.drive_api_upload_settings = DriveApiUploadSettings(
            enabled=True, file_name="testGoogleDriveApiFilename"
        )

        mock_job_config.runs = [mock_run1]

        args = self._create_args_for_specified_file("fake-config-file.json", dry_run=True)
        mock_job_config_reader: NewCdswConfigReaderAdapter = Mock(spec=NewCdswConfigReaderAdapter)
        mock_job_config_reader.read_from_file.return_value = mock_job_config
        cdsw_runner_config = NewCdswRunnerConfig(PARSER, args, config_reader=mock_job_config_reader)

        cdsw_runner = NewCdswRunner(cdsw_runner_config)
        cdsw_runner.start(SETUP_RESULT, CDSW_RUNNER_SCRIPT_PATH)

        exp_command_1 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg("yarndevtools.py")
            .add_expected_arg("--arg1")
            .add_expected_arg("--arg2", param="bla")
            .add_expected_arg("--arg3", param="bla3")
        )

        exp_command_2 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg("yarndevtools.py")
            .add_expected_ordered_arg("ZIP_LATEST_COMMAND_DATA")
            .add_expected_ordered_arg("REVIEWSYNC")
            .add_expected_arg("--debug")
            .add_expected_arg("--dest_dir", "/tmp")
            .add_expected_arg("--ignore-filetypes", "java js")
        )
        wrap_d = StringUtils.wrap_to_quotes
        wrap_s = StringUtils.wrap_to_single_quotes
        expected_html_link = wrap_s('<a href="dummy_link">Command data file: testGoogleDriveApiFilename</a>')
        exp_command_3 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg("yarndevtools.py")
            .add_expected_ordered_arg("SEND_LATEST_COMMAND_DATA")
            .add_expected_arg("--debug")
            .add_expected_arg("--smtp_server", wrap_d("smtp.gmail.com"))
            .add_expected_arg("--smtp_port", "465")
            .add_expected_arg("--account_user", wrap_d("mailUser"))
            .add_expected_arg("--account_password", wrap_d("mailPassword"))
            .add_expected_arg("--subject", wrap_d("testSubject"))
            .add_expected_arg("--sender", wrap_d("testSender"))
            .add_expected_arg("--recipients", wrap_d("yarn_eng_bp@cloudera.com"))
            .add_expected_arg("--attachment-filename", "test_attachment_filename.zip")
            .add_expected_arg("--file-as-email-body-from-zip", "test")
            .add_expected_arg("--prepend_email_body_with_text", expected_html_link)
            .add_expected_arg("--send-attachment")
        )

        expectations = [exp_command_1, exp_command_2, exp_command_3]
        self._assert_commands(expectations, cdsw_runner.executed_commands)

    def _assert_commands(self, expectations: List[CommandExpectations], actual_commands: List[str]):
        self.assertEqual(
            len(actual_commands),
            len(expectations),
            msg="Not all commands are having expectations set. Commands: {}, Expectations: {}".format(
                actual_commands, expectations
            ),
        )
        for actual_command, expectation in zip(actual_commands, expectations):
            expectation.verify_command(actual_command)
