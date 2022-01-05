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
    @classmethod
    def setUpClass(cls):
        ProjectUtils.set_root_determine_strategy(ProjectRootDeterminationStrategy.COMMON_FILE)
        ProjectUtils.get_test_output_basedir(PROJECT_NAME)
        cls._setup_logging()

    def setUp(self):
        pass

    def tearDown(self) -> None:
        pass

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
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config)
        self.assertEqual("Reviewsync", config_reader.config.job_name)

    def test_config_reader_valid_command_type(self):
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
        file = self._get_config_file(VALID_CONFIG)
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config)
        self.assertEqual(["GSHEET_CLIENT_SECRET", "GSHEET_SPREADSHEET"], config_reader.config.mandatory_env_vars)

    def test_config_reader_invalid_mandatory_env_var(self):
        file = self._get_config_file("cdsw_job_config_invalid_mandatory_env_var.json")
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)

    def test_config_reader_check_if_mandatory_env_vars_are_provided_at_runtime_positive_case(self):
        file = self._get_config_file(VALID_CONFIG)
        os.environ["GSHEET_CLIENT_SECRET"] = "sshhhh_secret"
        os.environ["GSHEET_SPREADSHEET"] = "test_sheet"
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config)
        self.assertEqual(["GSHEET_CLIENT_SECRET", "GSHEET_SPREADSHEET"], config_reader.config.mandatory_env_vars)

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
        file = self._get_config_file(VALID_CONFIG)
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config)
        self.assertEqual(CommandType.REVIEWSYNC, config_reader.config.command_type)

    @staticmethod
    def _get_config_file(file_name):
        file = FileUtils.join_path(os.getcwd(), "configfiles", file_name)
        return file
