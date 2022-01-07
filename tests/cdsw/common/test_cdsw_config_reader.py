import datetime
import logging
import os
import re
import unittest

from pythoncommons.constants import ExecutionMode
from pythoncommons.file_utils import FileUtils
from pythoncommons.logging_setup import SimpleLoggingSetup
from pythoncommons.project_utils import ProjectUtils, ProjectRootDeterminationStrategy

from tests.cdsw.common.testutils.cdsw_testing_common import CdswTestingCommons
from yarndevtools.cdsw.common_python.cdsw_config import CdswJobConfigReader
from yarndevtools.common.shared_command_utils import CommandType

VALID_CONFIG_FILE = "cdsw_job_config.json"

PROJECT_NAME = "cdsw-config-reader"

LOG = logging.getLogger(__name__)


class CdswConfigReaderTest(unittest.TestCase):
    configfiles_base_dir = None
    MANDATORY_VARS = {"GSHEET_CLIENT_SECRET", "GSHEET_SPREADSHEET", "GSHEET_JIRA_COLUMN"}
    cdsw_testing_commons = None

    @classmethod
    def setUpClass(cls):
        ProjectUtils.set_root_determine_strategy(ProjectRootDeterminationStrategy.COMMON_FILE)
        ProjectUtils.get_test_output_basedir(PROJECT_NAME)
        cls._setup_logging()
        cls.cdsw_testing_commons = CdswTestingCommons()
        cls.configfiles_base_dir = cls.cdsw_testing_commons.get_path_from_test_basedir("common", "configfiles")

    def setUp(self):
        pass

    def tearDown(self) -> None:
        for var in self.MANDATORY_VARS:
            if var in os.environ:
                del os.environ[var]

    @staticmethod
    def _set_mandatory_env_vars():
        os.environ["GSHEET_CLIENT_SECRET"] = "gsheet client secret"
        os.environ["GSHEET_SPREADSHEET"] = "gsheet spreadsheet"
        os.environ["GSHEET_JIRA_COLUMN"] = "gsheet jira column"
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
        file = self._get_config_file(VALID_CONFIG_FILE)
        self._set_mandatory_env_vars()
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config)
        self.assertEqual("Reviewsync", config_reader.config.job_name)

    def test_config_reader_valid_command_type(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file(VALID_CONFIG_FILE)
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
        file = self._get_config_file(VALID_CONFIG_FILE)
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
        file = self._get_config_file(VALID_CONFIG_FILE)
        self._set_mandatory_env_vars()
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config)
        self.assertEqual(
            ["GSHEET_CLIENT_SECRET", "GSHEET_SPREADSHEET", "MAIL_ACC_USER"], config_reader.config.mandatory_env_vars
        )

    def test_config_reader_check_if_mandatory_env_vars_are_provided_at_runtime_negative_case(self):
        file = self._get_config_file(VALID_CONFIG_FILE)
        os.environ["GSHEET_SPREADSHEET"] = "test_sheet"
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)
        self.assertIn("'GSHEET_CLIENT_SECRET'", exc_msg)
        self.assertNotIn("GSHEET_SPREADSHEET", exc_msg)

    def test_config_reader_mandatory_env_vars_are_of_correct_command_type(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file(VALID_CONFIG_FILE)
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config)
        self.assertEqual(CommandType.REVIEWSYNC, config_reader.config.command_type)

    def test_config_reader_valid_optional_env_vars(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file(VALID_CONFIG_FILE)
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
        file = self._get_config_file(VALID_CONFIG_FILE)
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config)
        self.assertEqual(
            {
                "--gsheet-client-secret": "GSHEET_CLIENT_SECRET",
                "--gsheet-spreadsheet": "GSHEET_SPREADSHEET",
                "--gsheet-jira-column": "GSHEET_JIRA_COLUMN",
            },
            config_reader.config.map_env_vars_to_yarn_dev_tools_argument,
        )

    def test_config_reader_if_optional_arg_is_mapped_to_yarndevtools_args_it_becomes_mandatory(self):
        os.environ["GSHEET_CLIENT_SECRET"] = "sshhhh_secret"
        os.environ["GSHEET_SPREADSHEET"] = "test_sheet"
        # "GSHEET_JIRA_COLUMN" is intentionally missing!
        file = self._get_config_file(VALID_CONFIG_FILE)

        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)
        self.assertIn("'GSHEET_JIRA_COLUMN'", exc_msg)

    def test_config_reader_empty_yarndevtools_args(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_empty_yarndevtools_args.json")
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)
        self.assertIn("Empty YARN dev tools arguments", exc_msg)

    def test_config_reader_invalid_format_of_yarndevtools_arg(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_invalid_format_of_yarndevtools_arg.json")
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)
        self.assertIn("Expected a mapped argument in format: <yarndevtools argument name><SPACE><PLACEHOLDER>", exc_msg)

    def test_config_reader_unmapped_yarndevtools_args(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_unmapped_yarndevtools_args.json")
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)
        self.assertIn("The following yarndevtools arguments are unmapped", exc_msg)
        self.assertIn("--gsheet-client-secret2", exc_msg)
        self.assertIn("--gsheet-client-secret3", exc_msg)

    def test_config_reader_variables(self):
        file = self._get_config_file(VALID_CONFIG_FILE)
        self._set_mandatory_env_vars()
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config.variables)
        self.assertEqual("testAlgorithm", config_reader.config.variables["algorithm"])
        # command_data_testAlgorithm_20220105_214629.zip

        self.assertEqual(
            "command_data_$$algorithm$$_$$JOB_START_DATE$$.zip", config_reader.config.variables["commandDataFileName"]
        )
        self._match_env_var_for_regex(
            config_reader.config, "commandDataFileName", r"command_data_testAlgorithm_(.*)\.zip"
        )

    def test_config_reader_using_builtin_variable(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_invalid_using_builtin_variable.json")
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)
        self.assertIn("Cannot use variables with the same name as built-in variables", exc_msg)

    def test_config_reader_malformed_variable_declaration(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_malformed_variable_declaration.json")
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)
        self.assertIn("Malformed variable declaration in", exc_msg)

    def test_config_reader_malformed_variable_declaration_empty_var(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_malformed_variable_declaration_empty_var.json")
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)
        self.assertIn("Found malformed (empty) variable declaration", exc_msg)

    def test_config_reader_transitive_variable_resolution_endless(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_transitive_variable_resolution_endless.json")
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)
        self.assertIn("Cannot resolve variable 'varD' in raw var: $$varD$$", exc_msg)

    def test_config_reader_transitive_variable_resolution_unresolved(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_transitive_variable_resolution_unresolved.json")
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)
        self.assertIn("Cannot resolve variable 'varX' in raw var: $$varX$$", exc_msg)

    def test_config_reader_transitive_variable_resolution_valid(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_transitive_variable_resolution_valid.json")
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config.variables)
        self.assertEqual("testAlgorithm", config_reader.config.variables["algorithm"])

        self.assertEqual("no_var_here", config_reader.config.resolved_variables["varE"])
        self.assertEqual("no_var_here", config_reader.config.resolved_variables["varA"])
        self.assertEqual("no_var_here", config_reader.config.resolved_variables["varB"])
        self.assertEqual("no_var_here", config_reader.config.resolved_variables["varC"])
        self.assertEqual("no_var_here", config_reader.config.resolved_variables["varD"])

    def test_config_reader_transitive_variable_resolution_valid_more_complex(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_transitive_variable_resolution_valid_more_complex.json")
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config.variables)
        self.assertEqual("testAlgorithm", config_reader.config.variables["algorithm"])

        self.assertEqual("no_var_here", config_reader.config.resolved_variables["varE"])
        self.assertEqual("no_var_here", config_reader.config.resolved_variables["varA"])
        self.assertEqual("no_var_here", config_reader.config.resolved_variables["varB"])
        self.assertEqual("no_var_here", config_reader.config.resolved_variables["varC"])
        self.assertEqual("no_var_here", config_reader.config.resolved_variables["varD"])
        self.assertEqual("s", config_reader.config.resolved_variables["varS"])
        self.assertEqual("xy", config_reader.config.resolved_variables["varZ"])
        self.assertEqual("xys", config_reader.config.resolved_variables["varT"])
        self.assertEqual("xys", config_reader.config.resolved_variables["varU"])

    def test_config_reader_transitive_variable_resolution_valid_more_complex2(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_transitive_variable_resolution_valid_more_complex2.json")
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config.variables)
        self.assertEqual("testAlgorithm", config_reader.config.variables["algorithm"])

        self.assertEqual("x", config_reader.config.resolved_variables["varZ"])
        self.assertEqual("xs", config_reader.config.resolved_variables["varT"])
        self.assertEqual("x", config_reader.config.resolved_variables["varX"])
        self.assertEqual("s", config_reader.config.resolved_variables["varS"])

    def test_config_reader_email_settings(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_email_settings_with_vars.json")
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config.email_settings)
        self.assertEqual("testSubject+v2+v1", config_reader.config.email_settings.subject)
        self.assertEqual("attachmentFileName+v3+v4", config_reader.config.email_settings.attachment_file_name)
        self.assertFalse(config_reader.config.email_settings.enabled)
        self.assertTrue(config_reader.config.email_settings.send_attachment)

    def test_config_reader_drive_api_upload_settings(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file(VALID_CONFIG_FILE)
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config.drive_api_upload_settings)
        self.assertEqual("simple", config_reader.config.drive_api_upload_settings.file_name)
        self.assertFalse(config_reader.config.drive_api_upload_settings.enabled)

    def test_config_reader_drive_api_upload_settings_with_vars(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_drive_api_upload_settings_with_vars.json")
        config_reader: CdswJobConfigReader = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config_reader.config.drive_api_upload_settings)
        self.assertEqual(
            "constant1_v1_constant2_v3_constant3", config_reader.config.drive_api_upload_settings.file_name
        )
        self.assertFalse(config_reader.config.drive_api_upload_settings.enabled)

    def _match_env_var_for_regex(self, config, env_name, regex):
        LOG.debug(
            "Matching Env var with name '%s' with resolved value of %s, Original value: %s",
            env_name,
            config.resolved_variables[env_name],
            config.variables[env_name],
        )
        match = re.match(regex, config.resolved_variables[env_name])
        if not match:
            self.fail(
                "Env var with name '{}' with resolved value of {} does not match regex: {}. Original value: {}".format(
                    env_name, config.resolved_variables[env_name], regex, config.variables[env_name]
                )
            )
        LOG.debug("Found date: %s", match.group(1))

    # TODO test unresolved variable

    @classmethod
    def _get_config_file(cls, file_name):
        file = FileUtils.join_path(cls.configfiles_base_dir, file_name)
        return file

    @staticmethod
    def validate_date(date_text):
        try:
            datetime.datetime.strptime(date_text, "%Y%m%d_%H%M%S")
        except ValueError:
            raise ValueError("Incorrect data format, should be YYYY-MM-DD")
