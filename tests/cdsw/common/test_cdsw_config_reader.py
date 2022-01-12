import datetime
import logging
import os
import re
import unittest
from typing import Dict

from pythoncommons.constants import ExecutionMode
from pythoncommons.file_utils import FileUtils
from pythoncommons.logging_setup import SimpleLoggingSetup
from pythoncommons.project_utils import ProjectUtils, ProjectRootDeterminationStrategy

from tests.cdsw.common.testutils.cdsw_testing_common import CdswTestingCommons
from yarndevtools.cdsw.common_python.cdsw_config import CdswJobConfigReader, RegularVariables
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
        self._clear_env_vars()

    def _clear_env_vars(self):
        for var in self.MANDATORY_VARS:
            if var in os.environ:
                del os.environ[var]

    @staticmethod
    def _set_mandatory_env_vars():
        os.environ["GSHEET_CLIENT_SECRET"] = "gsheet client secret"
        os.environ["GSHEET_SPREADSHEET"] = "gsheet spreadsheet"
        os.environ["GSHEET_JIRA_COLUMN"] = "gsheet jira column"
        os.environ["MAIL_ACC_USER"] = "mail account user"

    @staticmethod
    def _set_env_vars_from_dict(dict_of_vars: Dict[str, str]):
        for k, v in dict_of_vars.items():
            os.environ[k] = v

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
        config = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config)
        self.assertEqual("Reviewsync", config.job_name)

    def test_config_reader_valid_command_type(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file(VALID_CONFIG_FILE)
        config = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config)
        self.assertEqual(CommandType.REVIEWSYNC, config.command_type)

    def test_config_reader_email_body_file(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_email_body_file.json")
        config = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config.runs)
        run = config.runs[0]
        self.assertIsNotNone(run)
        self.assertIsNotNone(run.email_settings)
        self.assertEqual(run.email_settings.email_body_file_from_command_data, "report-short.html")

    def test_config_reader_invalid_command_type(self):
        file = self._get_config_file("cdsw_job_config_bad_command_type.json")
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)

    def test_config_reader_valid_mandatory_env_vars(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file(VALID_CONFIG_FILE)
        config = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config)
        self.assertEqual(["GSHEET_CLIENT_SECRET", "GSHEET_SPREADSHEET", "MAIL_ACC_USER"], config.mandatory_env_vars)

    def test_config_reader_invalid_mandatory_env_var(self):
        file = self._get_config_file("cdsw_job_config_invalid_mandatory_env_var.json")
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)

    def test_config_reader_check_if_mandatory_env_vars_are_provided_at_runtime_positive_case(self):
        file = self._get_config_file(VALID_CONFIG_FILE)
        self._set_mandatory_env_vars()
        config = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config)
        self.assertEqual(["GSHEET_CLIENT_SECRET", "GSHEET_SPREADSHEET", "MAIL_ACC_USER"], config.mandatory_env_vars)

    def test_config_reader_check_if_mandatory_env_vars_missing(self):
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
        config = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config)
        self.assertEqual(CommandType.REVIEWSYNC, config.command_type)

    def test_config_reader_valid_optional_env_vars(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file(VALID_CONFIG_FILE)
        config = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config)
        self.assertEqual(["BRANCHES", "GSHEET_JIRA_COLUMN"], config.optional_env_vars)

    def test_config_reader_valid_optional_env_vars_should_be_also_part_of_env_var_class(self):
        file = self._get_config_file("cdsw_job_config_invalid_optional_env_var.json")
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)

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
        self.assertIn("Malformed variable declaration in string: --argument", exc_msg)

    def test_config_reader_variables(self):
        file = self._get_config_file(VALID_CONFIG_FILE)
        self._set_mandatory_env_vars()
        config = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config.global_variables)
        self.assertEqual("testAlgorithm", config.global_variables["algorithm"])
        # command_data_testAlgorithm_20220105_214629.zip

        self.assertEqual(
            "command_data_$$algorithm$$_$$JOB_START_DATE$$.zip",
            config.global_variables["commandDataFileName"],
        )
        self._match_env_var_for_regex(config, "commandDataFileName", r"command_data_testAlgorithm_(.*)\.zip")

    def test_config_reader_using_builtin_variable(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_invalid_using_builtin_variable.json")
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)
        self.assertIn("Cannot use variables with the same name as built-in variables", exc_msg)

    def test_config_reader_using_builtin_variable_in_other_variable(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_using_builtin_variable_in_other_variable.json")
        config = CdswJobConfigReader.read_from_file(file)

        job_start_date = RegularVariables.BUILT_IN_VARIABLES["JOB_START_DATE"]
        expected_subject = f"YARN reviewsync report [start date: {job_start_date}]"
        expected_command_data_file_name = f"command_data_{job_start_date}.zip"
        self.assertIsNotNone(config.runs[0])
        self.assertEqual(
            {
                "JOB_START_DATE": job_start_date,
                "sender": "YARN reviewsync",
                "subject": expected_subject,
                "commandDataFileName": expected_command_data_file_name,
            },
            config.resolved_variables,
        )
        self.assertEqual(expected_command_data_file_name, config.runs[0].email_settings.attachment_file_name)
        self.assertEqual("report-short.html", config.runs[0].email_settings.email_body_file_from_command_data)
        self.assertEqual("YARN reviewsync", config.runs[0].email_settings.sender)
        self.assertEqual(expected_subject, config.runs[0].email_settings.subject)

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
        config = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config.global_variables)
        self.assertEqual("testAlgorithm", config.global_variables["algorithm"])

        self.assertEqual("no_var_here", config.resolved_variables["varE"])
        self.assertEqual("no_var_here", config.resolved_variables["varA"])
        self.assertEqual("no_var_here", config.resolved_variables["varB"])
        self.assertEqual("no_var_here", config.resolved_variables["varC"])
        self.assertEqual("no_var_here", config.resolved_variables["varD"])

    def test_config_reader_transitive_variable_resolution_valid_more_complex(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_transitive_variable_resolution_valid_more_complex.json")
        config = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config.global_variables)
        self.assertEqual("testAlgorithm", config.global_variables["algorithm"])

        self.assertEqual("no_var_here", config.resolved_variables["varE"])
        self.assertEqual("no_var_here", config.resolved_variables["varA"])
        self.assertEqual("no_var_here", config.resolved_variables["varB"])
        self.assertEqual("no_var_here", config.resolved_variables["varC"])
        self.assertEqual("no_var_here", config.resolved_variables["varD"])
        self.assertEqual("s", config.resolved_variables["varS"])
        self.assertEqual("xy", config.resolved_variables["varZ"])
        self.assertEqual("xys", config.resolved_variables["varT"])
        self.assertEqual("xys", config.resolved_variables["varU"])

    def test_config_reader_transitive_variable_resolution_valid_more_complex2(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_transitive_variable_resolution_valid_more_complex2.json")
        config = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config.global_variables)
        self.assertEqual("testAlgorithm", config.global_variables["algorithm"])

        self.assertEqual("x", config.resolved_variables["varZ"])
        self.assertEqual("xs", config.resolved_variables["varT"])
        self.assertEqual("x", config.resolved_variables["varX"])
        self.assertEqual("s", config.resolved_variables["varS"])

    def test_config_reader_email_settings(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_email_settings_with_vars.json")
        config = CdswJobConfigReader.read_from_file(file)

        email_settings_1 = config.runs[0].email_settings
        email_settings_2 = config.runs[1].email_settings
        self.assertIsNotNone(email_settings_1)
        self.assertIsNotNone(email_settings_2)

        self.assertEqual("testSubject+v2+v1_1", email_settings_1.subject)
        self.assertEqual("attachmentFileName+v3+v4", email_settings_1.attachment_file_name)
        self.assertFalse(email_settings_1.enabled)
        self.assertTrue(email_settings_1.send_attachment)

        self.assertEqual("testSubject+v2+v1_2", email_settings_2.subject)
        self.assertEqual("attachmentFileName+v1", email_settings_2.attachment_file_name)
        self.assertFalse(email_settings_2.enabled)
        self.assertTrue(email_settings_2.send_attachment)

    def test_config_reader_drive_api_upload_settings(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file(VALID_CONFIG_FILE)
        config = CdswJobConfigReader.read_from_file(file)

        drive_api_upload_settings = config.runs[0].drive_api_upload_settings
        self.assertIsNotNone(drive_api_upload_settings)
        self.assertEqual("simple", drive_api_upload_settings.file_name)
        self.assertFalse(drive_api_upload_settings.enabled)

    def test_config_reader_drive_api_upload_settings_with_vars(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_drive_api_upload_settings_with_vars.json")
        config = CdswJobConfigReader.read_from_file(file)

        drive_api_upload_settings = config.runs[0].drive_api_upload_settings
        self.assertIsNotNone(drive_api_upload_settings)
        self.assertEqual("constant1_v1_constant2_v3_constant3", drive_api_upload_settings.file_name)
        self.assertFalse(drive_api_upload_settings.enabled)

    def test_config_reader_runconfig_defined_yarn_dev_tools_arguments_env_vars(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_runconfig_defined_yarn_dev_tools_arguments_env_vars.json")
        config = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config.runs[0])
        self.assertIsNotNone(config.runs[0].yarn_dev_tools_arguments)
        self.assertEqual(
            [
                "--debug",
                "REVIEWSYNC",
                "--gsheet",
                "--gsheet-client-secret 'gsheet client secret'",
                "--gsheet-spreadsheet 'gsheet spreadsheet'",
                "--gsheet-jira-column 'gsheet jira column'",
            ],
            config.yarn_dev_tools_arguments,
        )
        self.assertEqual(
            [
                "--debug",
                "REVIEWSYNC",
                "--gsheet",
                "--gsheet-client-secret 'gsheet client secret'",
                "--gsheet-spreadsheet 'gsheet spreadsheet'",
                "--gsheet-jira-column 'gsheet jira column'",
                "--arg1",
                "--arg2 param1 param2",
                "--arg3 param1",
                "--arg4",
            ],
            config.runs[0].yarn_dev_tools_arguments,
        )

    def test_config_reader_runconfig_defined_yarn_dev_tools_arguments_regular_vars(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_runconfig_defined_yarn_dev_tools_arguments_regular_vars.json")
        config = CdswJobConfigReader.read_from_file(file)
        job_start_date = RegularVariables.BUILT_IN_VARIABLES["JOB_START_DATE"]

        self.assertIsNotNone(config.runs[0])
        self.assertEqual(
            [
                "--debug",
                "REVIEWSYNC",
                "--gsheet",
                "--algo testAlgorithm",
                f"--command-data-filename command_data_testAlgorithm_{job_start_date}.zip",
            ],
            config.yarn_dev_tools_arguments,
        )
        self.assertEqual(
            [
                "--debug",
                "REVIEWSYNC",
                "--gsheet",
                "--algo testAlgorithm",
                f"--command-data-filename command_data_testAlgorithm_{job_start_date}.zip",
                "--arg1",
                "--arg2 param1 param2",
                "--arg3 param1",
                "--arg4",
            ],
            config.runs[0].yarn_dev_tools_arguments,
        )

    def test_config_reader_runconfig_defined_yarn_dev_tools_arguments_overrides(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_runconfig_defined_yarn_dev_tools_arguments_overrides.json")
        config = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config.runs[0])
        self.assertEqual(
            [
                "--debug",
                "REVIEWSYNC",
                "--gsheet",
                "--gsheet-client-secret 'gsheet client secret'",
                "--gsheet-spreadsheet 'gsheet spreadsheet'",
                "--gsheet-jira-column 'gsheet jira column'",
            ],
            config.yarn_dev_tools_arguments,
        )
        self.assertEqual(
            [
                "--debug",
                "REVIEWSYNC",
                "--gsheet",
                "--gsheet-client-secret bla",
                "--gsheet-spreadsheet bla2",
                "--gsheet-jira-column 'gsheet jira column'",
                "--arg1",
            ],
            config.runs[0].yarn_dev_tools_arguments,
        )

    def test_config_reader_runconfig_defined_yarn_dev_tools_variable_overrides(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_runconfig_defined_yarn_dev_tools_variable_overrides.json")
        config = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config.runs[0])
        original_yarndevtools_args = [
            "--debug",
            "REVIEWSYNC",
            "--gsheet",
            "--gsheet-client-secret 'gsheet client secret'",
            "--gsheet-spreadsheet 'gsheet spreadsheet'",
            "--gsheet-jira-column 'gsheet jira column'",
        ]
        self.assertEqual(
            original_yarndevtools_args,
            config.yarn_dev_tools_arguments,
        )
        self.assertEqual(
            original_yarndevtools_args
            + [
                "--testArg1 yetAnotherAlgorithm",
                "--testArg2 overriddenCommandData",
                "--testArg3 something+globalValue1",
                "--testArg4 something+globalValue2",
                "--testArg5 a new variable",
            ],
            config.runs[0].yarn_dev_tools_arguments,
        )

    def test_config_reader_two_run_configs_defined_complex(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_two_run_configs_defined_complex.json")
        config = CdswJobConfigReader.read_from_file(file)

        self.assertIsNotNone(config.runs[0])
        self.assertIsNotNone(config.runs[1])
        original_yarndevtools_args = [
            "--debug",
            "REVIEWSYNC",
            "--gsheet",
            "--gsheet-client-secret 'gsheet client secret'",
            "--gsheet-spreadsheet 'gsheet spreadsheet'",
            "--gsheet-jira-column 'gsheet jira column'",
        ]
        self.assertEqual(
            original_yarndevtools_args,
            config.yarn_dev_tools_arguments,
        )
        self.assertEqual(
            original_yarndevtools_args
            + [
                "--testArg1 yetAnotherAlgorithm",
                "--testArg2 overriddenCommandData",
                "--testArg3 something+globalValue1",
                "--testArg4 something+globalValue2",
                "--testArg5 a new variable",
            ],
            config.runs[0].yarn_dev_tools_arguments,
        )

        self.assertEqual(
            original_yarndevtools_args
            + [
                "--testArg1 yetAnotherAlgorithm2",
                "--testArg2 overriddenCommandData2",
                "--testArg3 var1+globalValue3",
                "--testArg4 var2+globalValue4",
                "--testArg5 var3",
            ],
            config.runs[1].yarn_dev_tools_arguments,
        )

    def test_config_reader_two_run_configs_with_same_name_not_allowed(self):
        self._set_mandatory_env_vars()
        file = self._get_config_file("cdsw_job_config_two_run_configs_same_name.json")
        with self.assertRaises(ValueError) as ve:
            CdswJobConfigReader.read_from_file(file)
        exc_msg = ve.exception.args[0]
        LOG.info(exc_msg)
        self.assertIn("Duplicate job name not allowed!", exc_msg)

    def test_config_reader_env_var_sanitize(self):
        self._set_env_vars_from_dict(
            {
                "GSHEET_WORKSHEET": "env1",
                "GSHEET_SPREADSHEET": "env2 env22",
                "GSHEET_JIRA_COLUMN": "env3 'env33' env333",
                "GSHEET_STATUS_INFO_COLUMN": "'env4 env44'",
                "GSHEET_UPDATE_DATE_COLUMN": '"env5 env5555"',
                "BRANCHES": "branch-3.2 branch-3.3",
            }
        )
        file = self._get_config_file("cdsw_job_config_env_var_sanitize_test.json")
        config = CdswJobConfigReader.read_from_file(file)

        self.assertEqual(
            [
                "--debug",
                "REVIEWSYNC",
                "--gsheet",
                "--arg1 env1",
                "--arg2 'env2 env22'",
                "--arg3 env3 'env33' env333",
                "--arg4 'env4 env44'",
                '--arg5 "env5 env5555"',
                "--arg6 branch-3.2 branch-3.3",
            ],
            config.runs[0].yarn_dev_tools_arguments,
        )

    def _match_env_var_for_regex(self, config, env_name, regex):
        LOG.debug(
            "Matching Env var with name '%s' with resolved value of %s, Original value: %s",
            env_name,
            config.resolved_variables[env_name],
            config.global_variables[env_name],
        )
        match = re.match(regex, config.resolved_variables[env_name])
        if not match:
            self.fail(
                "Env var with name '{}' with resolved value of {} does not match regex: {}. Original value: {}".format(
                    env_name, config.resolved_variables[env_name], regex, config.global_variables[env_name]
                )
            )
        LOG.debug("Found date: %s", match.group(1))

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
