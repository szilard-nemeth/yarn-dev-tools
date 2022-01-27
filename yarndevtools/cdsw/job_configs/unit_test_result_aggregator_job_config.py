from yarndevtools.cdsw.cdsw_common import (
    UnitTestResultAggregatorCdswUtils,
    GenericCdswConfigUtils,
)
from yarndevtools.cdsw.cdsw_config import Include
from yarndevtools.cdsw.constants import CdswEnvVar
from yarndevtools.commands.unittestresultaggregator.common import OperationMode
from yarndevtools.common.shared_command_utils import CommandType

config = {
    "job_name": "Unit test result aggregator",
    "command_type": CommandType.UNIT_TEST_RESULT_AGGREGATOR,
    "env_sanitize_exceptions": ["MATCH_EXPRESSION", "GSHEET_COMPARE_WITH_JIRA_TABLE"],
    "mandatory_env_vars": [
        "GSHEET_CLIENT_SECRET",
        "GSHEET_WORKSHEET",
        "GSHEET_SPREADSHEET",
        "MAIL_ACC_USER",
        "MAIL_ACC_PASSWORD",
        "MATCH_EXPRESSION",
    ],
    "optional_env_vars": [
        "REQUEST_LIMIT",
        "ABBREV_TC_PACKAGE",
        "AGGREGATE_FILTERS",
        "GSHEET_COMPARE_WITH_JIRA_TABLE",
        "SKIP_AGGREGATION_RESOURCE_FILE",
        "SKIP_AGGREGATION_RESOURCE_FILE_AUTO_DISCOVERY",
    ],
    "yarn_dev_tools_arguments": [
        lambda conf: f"{Include.when('True' == conf.var('debugMode'), '--debug', '')}",
        f"{CommandType.UNIT_TEST_RESULT_AGGREGATOR.name}",
        lambda conf: f"--{conf.var('execMode')}",
        lambda conf: f"--gsheet-client-secret {conf.env('GSHEET_CLIENT_SECRET')}",
        lambda conf: f"--gsheet-worksheet {conf.env('GSHEET_WORKSHEET')}",
        lambda conf: f"--gsheet-spreadsheet {conf.env('GSHEET_SPREADSHEET')}",
        lambda conf: f"--account-email {conf.env('MAIL_ACC_USER')}",
        lambda conf: f"--request-limit {conf.env('REQUEST_LIMIT')}",
        lambda conf: f"--match-expression {conf.env('MATCH_EXPRESSION')}",
        lambda conf: f"--gmail-query {conf.var('defaultGmailQuery')}",
        lambda conf: f"--summary-mode {conf.var('summaryMode')}",
        lambda conf: f"--skip-lines-starting-with {conf.var('skipLinesStartingWithCLI')}",
        lambda conf: f"{Include.when(conf.var('smartSubjectQuery'), '--smart-subject-query', '')}",
        lambda conf: Include.when(
            conf.var("abbreviateTestcasePackageEnv"), conf.var("abbreviateTestcasePackageVal"), ""
        ),
        lambda conf: Include.when(conf.var("aggregateFiltersEnv"), conf.var("aggregateFiltersVal"), ""),
        lambda conf: Include.when(
            conf.var("gsheetCompareWithJiraTableEnv"), conf.var("gsheetCompareWithJiraTableVal"), ""
        ),
    ],
    "global_variables": {
        "debugMode": lambda conf: conf.env_or_default(CdswEnvVar.DEBUG_ENABLED.value, True),
        "sender": "YARN unit test aggregator",
        "subject": lambda conf: f"YARN unit test aggregator report [start date: {conf.job_start_date()}]",
        "commandDataFileName": lambda conf: f"command_data_{conf.job_start_date()}.zip",
        "smartSubjectQuery": True,
        "summaryMode": "html",
        "execMode": OperationMode.GSHEET.name.lower(),
        "abbreviateTestcasePackageEnv": lambda conf: conf.env("ABBREV_TC_PACKAGE"),
        "abbreviateTestcasePackageVal": lambda conf: f"--abbreviate-testcase-package {conf.var('abbreviateTestcasePackageEnv')}",
        "aggregateFiltersEnv": lambda conf: conf.env("AGGREGATE_FILTERS"),
        "aggregateFiltersVal": lambda conf: f"--aggregate-filters {conf.var('aggregateFiltersEnv')}",
        "gsheetCompareWithJiraTableEnv": lambda conf: conf.env("GSHEET_COMPARE_WITH_JIRA_TABLE"),
        "gsheetCompareWithJiraTableVal": lambda conf: f"--gsheet-compare-with-jira-table {GenericCdswConfigUtils.quote(conf.var('gsheetCompareWithJiraTableEnv'))}",
        "defaultGmailQuery": 'subject:"YARN Daily unit test report"',
        "defaultTruncateSubject": "YARN Daily unit test report: Failed tests with build: ",
        "skipLinesStartingWithCLI": lambda conf: GenericCdswConfigUtils.quote_list_items(
            UnitTestResultAggregatorCdswUtils.determine_lines_to_skip()
        ),
    },
    "runs": [
        {
            "name": "run1",
            "variables": {},
            "email_settings": {
                "enabled": True,
                "send_attachment": False,
                "attachment_file_name": lambda conf: f"{conf.var('commandDataFileName')}",
                "email_body_file_from_command_data": "report-short.html",
                "sender": lambda conf: f"{conf.var('sender')}",
                "subject": lambda conf: f"{conf.var('subject')}",
            },
            "drive_api_upload_settings": {
                "enabled": True,
                "file_name": lambda conf: f"{conf.var('commandDataFileName')}",
            },
            "yarn_dev_tools_arguments": [],
        }
    ],
}
