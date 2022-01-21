import os
import unittest
import logging
from typing import List
from unittest.mock import patch, Mock, call as mock_call, _CallList

from googleapiwrapper.google_drive import DriveApiFile
from pythoncommons.file_utils import FileUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils
from pythoncommons.string_utils import StringUtils

from tests.cdsw.common.testutils.cdsw_testing_common import CommandExpectations, CdswTestingCommons
from tests.test_utilities import Object
from yarndevtools.cdsw.common.cdsw_common import CommonFiles
from yarndevtools.cdsw.common.cdsw_config import (
    CdswRun,
    CdswJobConfig,
    EmailSettings,
    DriveApiUploadSettings,
    CdswJobConfigReader,
)
from yarndevtools.cdsw.common.cdsw_runner import (
    NewCdswRunnerConfig,
    NewCdswRunner,
    ExecutionMode,
    NewCdswConfigReaderAdapter,
)
from yarndevtools.cdsw.common.constants import CdswEnvVar
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME

FAKE_CONFIG_FILE = "fake-config-file.py"

DEFAULT_COMMAND_TYPE = CommandType.REVIEWSYNC
# CDSW_RUNNER_CLASSNAME = NewCdswRunner.__name__
CDSW_JOB_CONFIG_READER_CLASS_NAME = CdswJobConfigReader.__name__
# CDSW_RUNNER_BEGIN_PATH = "yarndevtools.cdsw.common.cdsw_runner.{}.begin".format(
#     CDSW_RUNNER_CLASSNAME)
CDSW_CONFIG_READER_READ_METHOD_PATH = "yarndevtools.cdsw.common.cdsw_config.{}".format(
    CDSW_JOB_CONFIG_READER_CLASS_NAME
)
SUBPROCESSRUNNER_RUN_METHOD_PATH = "pythoncommons.process.SubprocessCommandRunner.run_and_follow_stdout_stderr"
CDSW_RUNNER_DRIVE_CDSW_HELPER_UPLOAD_PATH = "yarndevtools.cdsw.common.cdsw_common.GoogleDriveCdswHelper.upload"
DRIVE_API_WRAPPER_UPLOAD_PATH = "googleapiwrapper.google_drive.DriveApiWrapper.upload_file"
PARSER = None
SETUP_RESULT = None
CDSW_RUNNER_SCRIPT_PATH = None
LOG = logging.getLogger(__name__)


class TestNewCdswRunner(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        OsUtils.clear_env_vars([CdswEnvVar.MAIL_RECIPIENTS.name])
        CommonFiles.YARN_DEV_TOOLS_SCRIPT = "yarndevtools.py"
        OsUtils.set_env_value(CdswEnvVar.MAIL_ACC_USER.value, "mailUser")
        OsUtils.set_env_value(CdswEnvVar.MAIL_ACC_PASSWORD.value, "mailPassword")

    def setUp(self) -> None:
        if CdswEnvVar.ENABLE_GOOGLE_DRIVE_INTEGRATION.value in os.environ:
            del os.environ[CdswEnvVar.ENABLE_GOOGLE_DRIVE_INTEGRATION.value]

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

    @staticmethod
    def _create_cdsw_runner_with_mock_config(args, mock_job_config):
        mock_job_config_reader: NewCdswConfigReaderAdapter = Mock(spec=NewCdswConfigReaderAdapter)
        mock_job_config_reader.read_from_file.return_value = mock_job_config
        cdsw_runner_config = NewCdswRunnerConfig(PARSER, args, config_reader=mock_job_config_reader)
        cdsw_runner = NewCdswRunner(cdsw_runner_config)
        return cdsw_runner

    @staticmethod
    def _create_mock_job_config(runs: List[CdswRun]):
        mock_job_config: CdswJobConfig = Mock(spec=CdswJobConfig)
        mock_job_config.command_type = DEFAULT_COMMAND_TYPE
        mock_job_config.runs = runs
        return mock_job_config

    @staticmethod
    def _create_mock_cdsw_run(
        name: str,
        email_enabled=False,
        google_drive_upload_enabled=False,
        add_email_settings: bool = True,
        add_google_drive_settings: bool = True,
    ):
        mock_run1: CdswRun = Mock(spec=CdswRun)
        mock_run1.name = name
        mock_run1.yarn_dev_tools_arguments = ["--arg1", "--arg2 bla", "--arg3 bla3"]

        mock_run1.email_settings = None
        mock_run1.drive_api_upload_settings = None
        if add_email_settings:
            mock_run1.email_settings = EmailSettings(
                enabled=email_enabled,
                send_attachment=True,
                attachment_file_name="test_attachment_filename.zip",
                email_body_file_from_command_data="test",
                subject="testSubject",
                sender="testSender",
            )
        if add_google_drive_settings:
            mock_run1.drive_api_upload_settings = DriveApiUploadSettings(
                enabled=google_drive_upload_enabled, file_name="testGoogleDriveApiFilename"
            )
        return mock_run1

    @staticmethod
    def create_mock_drive_api_file(file_link: str):
        mock_drive_file = Mock(spec=DriveApiFile)
        mock_drive_file.link = file_link
        return mock_drive_file

    def test_argument_parsing_into_config_auto_discovery(self):
        args = self._create_args_for_auto_discovery(dry_run=True)
        config = NewCdswRunnerConfig(None, args)

        self.assertEqual(DEFAULT_COMMAND_TYPE, config.command_type)
        self.assertTrue(config.dry_run)
        self.assertEqual(ExecutionMode.AUTO_DISCOVERY, config.execution_mode)

    def test_argument_parsing_into_config(self):
        args = self._create_args_for_specified_file(FAKE_CONFIG_FILE, dry_run=True)
        config = NewCdswRunnerConfig(PARSER, args)

        self.assertEqual(DEFAULT_COMMAND_TYPE, config.command_type)
        self.assertTrue(config.dry_run)
        self.assertEqual(ExecutionMode.SPECIFIED_CONFIG_FILE, config.execution_mode)
        self.assertEqual(FAKE_CONFIG_FILE, config.job_config_file)

    def test_argument_parsing_into_config_invalid_command_type(self):
        args = self._create_args_for_specified_file(FAKE_CONFIG_FILE, dry_run=True, override_cmd_type="WRONGCOMMAND")
        with self.assertRaises(ValueError) as ve:
            NewCdswRunnerConfig(None, args)
        exc_msg = ve.exception.args[0]
        self.assertIn("Invalid command type specified! Possible values are:", exc_msg)

    def test_execute_runs_single_run_with_fake_args(self):
        mock_run1 = self._create_mock_cdsw_run("run1", email_enabled=True, google_drive_upload_enabled=True)
        mock_job_config = self._create_mock_job_config([mock_run1])

        args = self._create_args_for_specified_file(FAKE_CONFIG_FILE, dry_run=True)
        cdsw_runner = self._create_cdsw_runner_with_mock_config(args, mock_job_config)
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
        CdswTestingCommons.verify_commands(self, expectations, cdsw_runner.executed_commands)

    @patch(SUBPROCESSRUNNER_RUN_METHOD_PATH)
    @patch(CDSW_RUNNER_DRIVE_CDSW_HELPER_UPLOAD_PATH)
    def test_execute_two_runs_with_fake_args(self, mock_google_drive_cdsw_helper_upload, mock_subprocess_runner):
        mock_google_drive_cdsw_helper_upload.return_value = self.create_mock_drive_api_file(
            "http://googledrive/link-of-file-in-google-drive"
        )

        mock_run1 = self._create_mock_cdsw_run("run1", email_enabled=True, google_drive_upload_enabled=True)
        mock_run2 = self._create_mock_cdsw_run("run2", email_enabled=False, google_drive_upload_enabled=False)
        mock_job_config = self._create_mock_job_config([mock_run1, mock_run2])

        args = self._create_args_for_specified_file(FAKE_CONFIG_FILE, dry_run=False)
        cdsw_runner = self._create_cdsw_runner_with_mock_config(args, mock_job_config)
        cdsw_runner.start(SETUP_RESULT, CDSW_RUNNER_SCRIPT_PATH)

        calls_of_yarndevtools = mock_subprocess_runner.call_args_list
        calls_of_google_drive_uploader = mock_google_drive_cdsw_helper_upload.call_args_list
        self.assertIn(
            "python3 yarndevtools.py --arg1 --arg2 bla --arg3 bla3",
            self._get_call_arguments_as_str(calls_of_yarndevtools, 0),
        )
        self.assertIn(
            "python3 yarndevtools.py --debug ZIP_LATEST_COMMAND_DATA REVIEWSYNC",
            self._get_call_arguments_as_str(calls_of_yarndevtools, 1),
        )
        self.assertIn(
            "python3 yarndevtools.py --debug SEND_LATEST_COMMAND_DATA",
            self._get_call_arguments_as_str(calls_of_yarndevtools, 2),
        )
        self.assertEqual(
            calls_of_google_drive_uploader,
            [
                mock_call(
                    CommandType.REVIEWSYNC,
                    "/Users/snemeth/snemeth-dev-projects/yarndevtools/latest-command-data-zip-reviewsync",
                    "testGoogleDriveApiFilename",
                )
            ],
        )

        self.assertIn(
            "python3 yarndevtools.py --arg1 --arg2 bla --arg3 bla3",
            self._get_call_arguments_as_str(calls_of_yarndevtools, 3),
        )
        self.assertIn(
            "python3 yarndevtools.py --debug ZIP_LATEST_COMMAND_DATA REVIEWSYNC",
            self._get_call_arguments_as_str(calls_of_yarndevtools, 4),
        )

        # Assert there are no more calls
        self.assertTrue(
            len(calls_of_yarndevtools) == 5,
            msg="Unexpected calls of yarndevtools: {}. First 5 calls are okay.".format(calls_of_yarndevtools),
        )
        self.assertTrue(
            len(calls_of_google_drive_uploader) == 1,
            msg="Unexpected calls of Google Drive uploader: {}. First call is okay.".format(
                calls_of_google_drive_uploader
            ),
        )

    @staticmethod
    def _get_call_arguments_as_str(mock, index):
        return " ".join(list(mock[index][0]))

    @staticmethod
    def _get_call_arguments_as_list(mock, index):
        return list(mock[index][0])

    @patch(SUBPROCESSRUNNER_RUN_METHOD_PATH)
    @patch(CDSW_RUNNER_DRIVE_CDSW_HELPER_UPLOAD_PATH)
    def test_google_drive_settings_are_not_defined(self, mock_google_drive_cdsw_helper_upload, mock_subprocess_runner):
        mock_run1 = self._create_mock_cdsw_run(
            "run1",
            email_enabled=True,
            google_drive_upload_enabled=True,
            add_email_settings=False,
            add_google_drive_settings=False,
        )
        mock_run2 = self._create_mock_cdsw_run(
            "run2",
            email_enabled=True,
            google_drive_upload_enabled=True,
            add_email_settings=False,
            add_google_drive_settings=False,
        )
        mock_job_config = self._create_mock_job_config([mock_run1, mock_run2])

        args = self._create_args_for_specified_file(FAKE_CONFIG_FILE, dry_run=False)
        cdsw_runner = self._create_cdsw_runner_with_mock_config(args, mock_job_config)
        cdsw_runner.start(SETUP_RESULT, CDSW_RUNNER_SCRIPT_PATH)

        calls_of_yarndevtools = mock_subprocess_runner.call_args_list
        calls_of_google_drive_uploader = mock_google_drive_cdsw_helper_upload.call_args_list

        self.assertTrue(
            len(calls_of_google_drive_uploader) == 0,
            msg="Unexpected calls to Google Drive uploader: {}".format(calls_of_google_drive_uploader),
        )
        CdswTestingCommons.assert_no_calls_with_arg(self, calls_of_yarndevtools, "SEND_LATEST_COMMAND_DATA")

    @patch(SUBPROCESSRUNNER_RUN_METHOD_PATH)
    @patch(CDSW_RUNNER_DRIVE_CDSW_HELPER_UPLOAD_PATH)
    def test_google_drive_settings_and_email_settings_are_defined_but_disabled(
        self, mock_google_drive_cdsw_helper_upload, mock_subprocess_runner
    ):
        mock_google_drive_cdsw_helper_upload.return_value = self.create_mock_drive_api_file(
            "http://googledrive/link-of-file-in-google-drive"
        )

        mock_run1 = self._create_mock_cdsw_run(
            "run1",
            email_enabled=False,
            google_drive_upload_enabled=False,
            add_email_settings=True,
            add_google_drive_settings=True,
        )
        mock_run2 = self._create_mock_cdsw_run(
            "run2",
            email_enabled=False,
            google_drive_upload_enabled=False,
            add_email_settings=True,
            add_google_drive_settings=True,
        )
        mock_job_config = self._create_mock_job_config([mock_run1, mock_run2])

        args = self._create_args_for_specified_file(FAKE_CONFIG_FILE, dry_run=False)
        cdsw_runner = self._create_cdsw_runner_with_mock_config(args, mock_job_config)
        cdsw_runner.start(SETUP_RESULT, CDSW_RUNNER_SCRIPT_PATH)

        calls_of_yarndevtools = mock_subprocess_runner.call_args_list
        calls_of_google_drive_uploader = mock_google_drive_cdsw_helper_upload.call_args_list

        self.assertTrue(
            len(calls_of_google_drive_uploader) == 0,
            msg="Unexpected calls to Google Drive uploader: {}".format(calls_of_google_drive_uploader),
        )
        CdswTestingCommons.assert_no_calls_with_arg(self, calls_of_yarndevtools, "SEND_LATEST_COMMAND_DATA")

    @patch(SUBPROCESSRUNNER_RUN_METHOD_PATH)
    @patch(CDSW_RUNNER_DRIVE_CDSW_HELPER_UPLOAD_PATH)
    def test_dry_run_does_not_invoke_anything(self, mock_google_drive_cdsw_helper_upload, mock_subprocess_runner):
        mock_run1 = self._create_mock_cdsw_run(
            "run1",
            email_enabled=True,
            google_drive_upload_enabled=True,
            add_email_settings=False,
            add_google_drive_settings=False,
        )
        mock_run2 = self._create_mock_cdsw_run(
            "run2",
            email_enabled=True,
            google_drive_upload_enabled=True,
            add_email_settings=False,
            add_google_drive_settings=False,
        )
        mock_job_config = self._create_mock_job_config([mock_run1, mock_run2])

        args = self._create_args_for_specified_file(FAKE_CONFIG_FILE, dry_run=True)
        cdsw_runner = self._create_cdsw_runner_with_mock_config(args, mock_job_config)
        cdsw_runner.start(SETUP_RESULT, CDSW_RUNNER_SCRIPT_PATH)

        calls_of_yarndevtools = mock_subprocess_runner.call_args_list
        calls_of_google_drive_uploader = mock_google_drive_cdsw_helper_upload.call_args_list

        self.assertTrue(
            len(calls_of_google_drive_uploader) == 0,
            msg="Unexpected calls to Google Drive uploader: {}".format(calls_of_google_drive_uploader),
        )
        self.assertTrue(
            len(calls_of_yarndevtools) == 0,
            msg="Unexpected calls to yarndevtools.py: {}".format(calls_of_yarndevtools),
        )

    @patch(CDSW_RUNNER_DRIVE_CDSW_HELPER_UPLOAD_PATH)
    def test_execute_google_drive_is_disabled_by_env_var(self, mock_google_drive_cdsw_helper_upload):
        mock_google_drive_cdsw_helper_upload.return_value = self.create_mock_drive_api_file(
            "http://googledrive/link-of-file-in-google-drive"
        )

        OsUtils.set_env_value(CdswEnvVar.ENABLE_GOOGLE_DRIVE_INTEGRATION.value, False)
        mock_run1 = self._create_mock_cdsw_run(
            "run1", email_enabled=True, google_drive_upload_enabled=True, add_google_drive_settings=True
        )
        mock_job_config = self._create_mock_job_config([mock_run1])

        # Need to enable dry-run to not fail the whole script
        # But it's hard to differentiate if dry-run or the ENABLE_GOOGLE_DRIVE_INTEGRATION env var disabled the file upload to Google Drive
        # So an additional check is added for the google_drive_uploads
        args = self._create_args_for_specified_file(FAKE_CONFIG_FILE, dry_run=True)
        cdsw_runner = self._create_cdsw_runner_with_mock_config(args, mock_job_config)
        cdsw_runner.start(SETUP_RESULT, CDSW_RUNNER_SCRIPT_PATH)

        calls_of_google_drive_uploader = mock_google_drive_cdsw_helper_upload.call_args_list
        self.assertTrue(
            len(calls_of_google_drive_uploader) == 0,
            msg="Unexpected calls to Google Drive uploader: {}".format(calls_of_google_drive_uploader),
        )
        self.assertEqual([], cdsw_runner.google_drive_uploads)

    @patch(SUBPROCESSRUNNER_RUN_METHOD_PATH)
    @patch(DRIVE_API_WRAPPER_UPLOAD_PATH)
    def test_upload_command_data_to_drive(self, mock_drive_api_wrapper_upload, mock_subprocess_runner):
        mock_drive_api_wrapper_upload.return_value = self.create_mock_drive_api_file("testLink")
        mock_run1 = self._create_mock_cdsw_run(
            "run1", email_enabled=True, google_drive_upload_enabled=True, add_google_drive_settings=True
        )
        mock_job_config = self._create_mock_job_config([mock_run1])

        args = self._create_args_for_specified_file(FAKE_CONFIG_FILE, dry_run=False)
        cdsw_runner = self._create_cdsw_runner_with_mock_config(args, mock_job_config)
        cdsw_runner.start(SETUP_RESULT, CDSW_RUNNER_SCRIPT_PATH)

        calls_of_google_drive_uploader = mock_drive_api_wrapper_upload.call_args_list
        self.assertTrue(
            len(calls_of_google_drive_uploader) == 1,
            msg="Unexpected calls to Google Drive uploader: {}".format(calls_of_google_drive_uploader),
        )
        expected_local_file_name = FileUtils.join_path(
            ProjectUtils.get_output_basedir(YARNDEVTOOLS_MODULE_NAME), "latest-command-data-zip-reviewsync"
        )
        expected_google_drive_file_name = FileUtils.join_path(
            cdsw_runner.drive_cdsw_helper.drive_command_data_basedir, "reviewsync", "testGoogleDriveApiFilename"
        )

        call = self._get_call_arguments_as_list(calls_of_google_drive_uploader, 0)
        self.assertEqual(expected_local_file_name, call[0])
        self.assertEqual(expected_google_drive_file_name, call[1])

    # TODO Add TC: send_latest_command_data_in_email, various testcases
    # TODO Add TC: unknown command type
