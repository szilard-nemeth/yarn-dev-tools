import os
import unittest
import logging

from pythoncommons.constants import ExecutionMode
from pythoncommons.file_utils import FileUtils
from pythoncommons.logging_setup import SimpleLoggingSetup
from pythoncommons.project_utils import ProjectUtils, ProjectRootDeterminationStrategy

from yarndevtools.cdsw.common_python.cdsw_config import CdswJobConfigReader
from yarndevtools.common.shared_command_utils import CommandType

VALID_CONFIG = "cdsw_job_config.json"

PROJECT_NAME = "cdsw-config-reader"

LOG = logging.getLogger(__name__)


class CdswConfigReaderTest(unittest.TestCase):
    MANDATORY_VARS = {"GSHEET_CLIENT_SECRET", "GSHEET_SPREADSHEET", "GSHEET_JIRA_COLUMN"}

    @classmethod
    def setUpClass(cls):
        ProjectUtils.set_root_determine_strategy(ProjectRootDeterminationStrategy.COMMON_FILE)
        ProjectUtils.get_test_output_basedir(PROJECT_NAME)
        cls._setup_logging()

    def setUp(self):
        pass

    def tearDown(self) -> None:
        for var in self.MANDATORY_VARS:
            if var in os.environ:
                del os.environ[var]

    @staticmethod
    def _set_mandatory_env_vars():
        os.environ["GSHEET_CLIENT_SECRET"] = "ghseet client secret"
        os.environ["GSHEET_SPREADSHEET"] = "gsheet spreadsheet"
        os.environ["GSHEET_JIRA_COLUMN"] = "jira column"
        os.environ["MAIL_ACC_USER"] = "mail account user"

    @classmethod
    def _setup_logging(cls):
        SimpleLoggingSetup.init_logger(
            project_name="cdsw_config_reader",
            logger_name_prefix="cdswconfigreader",
            execution_mode=ExecutionMode.TEST,
            console_debug=True,
            format_str="%(message)s",
        )

    def test_config_reader_job_name(self):
        file = self._get_config_file(VALID_CONFIG)
        self._set_mandatory_env_vars()
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config)
        self.assertEqual("Reviewsync", config_reader.config.job_name)

    def test_config_reader_valid_command_type(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file(VALID_CONFIG)
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config)
        self.assertEqual(CommandType.REVIEWSYNC, config_reader.config.command_type)

    def test_config_reader_invalid_command_type(self):
        file = self._get_config_file("cdsw_job_config_bad_command_type.json")
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)

    def test_config_reader_valid_mandatory_env_vars(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file(VALID_CONFIG)
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config)
        self.assertEqual(
            ["GSHEET_CLIENT_SECRET", "GSHEET_SPREADSHEET", "MAIL_ACC_USER"], config_reader.config.mandatory_env_vars
        )

    def test_config_reader_invalid_mandatory_env_var(self):
        file = self._get_config_file("cdsw_job_config_invalid_mandatory_env_var.json")
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)

    def test_config_reader_check_if_mandatory_env_vars_are_provided_at_runtime_positive_case(self):
        file = self._get_config_file(VALID_CONFIG)
        self._set_mandatory_env_vars()
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config)
        self.assertEqual(
            ["GSHEET_CLIENT_SECRET", "GSHEET_SPREADSHEET", "MAIL_ACC_USER"], config_reader.config.mandatory_env_vars
        )

    def test_config_reader_check_if_mandatory_env_vars_are_provided_at_runtime_negative_case(self):
        file = self._get_config_file(VALID_CONFIG)
        os.environ["GSHEET_SPREADSHEET"] = "test_sheet"
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)
        self.assertIn("'GSHEET_CLIENT_SECRET'", exc_msg)
        self.assertNotIn("GSHEET_SPREADSHEET", exc_msg)

    def test_config_reader_mandatory_env_vars_are_of_correct_command_type(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file(VALID_CONFIG)
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config)
        self.assertEqual(CommandType.REVIEWSYNC, config_reader.config.command_type)

    def test_config_reader_valid_optional_env_vars(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file(VALID_CONFIG)
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config)
        self.assertEqual(["BRANCHES", "GSHEET_JIRA_COLUMN"], config_reader.config.optional_env_vars)

    def test_config_reader_valid_optional_env_vars_should_be_also_part_of_env_var_class(self):
        file = self._get_config_file("cdsw_job_config_invalid_optional_env_var.json")
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)

    def test_config_reader_env_vars_mapped_to_yarndevtools_args(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file(VALID_CONFIG)
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config)
        self.assertEqual(
            {
                "--gsheet-client-secret": "GSHEET_CLIENT_SECRET",
                "--gsheet-spreadsheet": "GSHEET_SPREADSHEET",
                "--gseet-jira-column": "GSHEET_JIRA_COLUMN",
            },
            config_reader.config.map_env_vars_to_yarn_dev_tools_argument,
        )

    def test_config_reader_if_optional_arg_is_mapped_to_yarndevtools_args_it_becomes_mandatory(self):
        os.environ["GSHEET_CLIENT_SECRET"] = "sshhhh_secret"
        os.environ["GSHEET_SPREADSHEET"] = "test_sheet"
        # "GSHEET_JIRA_COLUMN" is intentionally missing!
        file = self._get_config_file(VALID_CONFIG)

        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)
        self.assertIn("'GSHEET_JIRA_COLUMN'", exc_msg)

    @staticmethod
    def _get_config_file(file_name):
        file = FileUtils.join_path(os.getcwd(), "configfiles", file_name)
        return file
