import logging
from enum import Enum

from pythoncommons.os_utils import OsUtils

from yarndevtools.argparser import CommandType
from yarndevtools.cdsw.common_python.cdsw_common import (
    CdswRunnerBase,
    CdswSetup,
    YARN_DEV_TOOLS_ROOT_DIR,
)
from yarndevtools.cdsw.common_python.constants import CdswEnvVar

LOG = logging.getLogger(__name__)
CMD_LOG = logging.getLogger(__name__)

DEFAULT_GMAIL_QUERY = 'subject:"YARN Daily unit test report"'
DEFAULT_SKIP_LINES_STARTING_WITH = ["Failed testcases:", "FILTER:"]


class DailyUTEnvVar(Enum):
    GSHEET_CLIENT_SECRET = "GSHEET_CLIENT_SECRET"
    GSHEET_SPREADHSHEET = "GSHEET_SPREADHSHEET"
    GSHEET_WORKSHEET = "GSHEET_WORKSHEET"
    REQUEST_LIMIT = "REQUEST_LIMIT"
    MATCH_EXPRESSION = "MATCH_EXPRESSION"


class CdswRunner(CdswRunnerBase):
    def start(self, basedir):
        LOG.info("Starting CDSW runner...")
        self.run_test_result_aggregator_and_send_mail()

    def run_test_result_aggregator_and_send_mail(self):
        self._run_aggregator(
            exec_mode="gsheet",
            gsheet_client_secret=OsUtils.get_env_value(DailyUTEnvVar.GSHEET_CLIENT_SECRET.value),
            gsheet_spreadsheet=OsUtils.get_env_value(DailyUTEnvVar.GSHEET_WORKSHEET.value),
            gsheet_worksheet=OsUtils.get_env_value(DailyUTEnvVar.GSHEET_WORKSHEET.value),
            account_email=OsUtils.get_env_value(CdswEnvVar.MAIL_ACC_USER.value),
            request_limit=OsUtils.get_env_value(DailyUTEnvVar.REQUEST_LIMIT.value),
            match_expression=OsUtils.get_env_value(DailyUTEnvVar.MATCH_EXPRESSION.value),
        )

        self.run_zipper(CommandType.UNIT_TEST_RESULT_AGGREGATOR, debug=True)

        date_str = self.current_date_formatted()
        sender = "YARN unit test aggregator"
        subject = f"YARN unit test aggregator report [start date: {date_str}]"
        attachment_fnname: str = f"command_data_{date_str}.zip"
        self.send_latest_command_data_in_email(sender=sender, subject=subject, attachment_filename=attachment_fnname)

    def _run_aggregator(
        self,
        exec_mode,
        gsheet_client_secret,
        gsheet_spreadsheet,
        gsheet_worksheet,
        account_email,
        request_limit,
        match_expression,
        gmail_query=DEFAULT_GMAIL_QUERY,
        skip_lines_starting_with=None,
        debug=True,
        smart_subject_query=True,
    ):
        if skip_lines_starting_with is None:
            skip_lines_starting_with = DEFAULT_SKIP_LINES_STARTING_WITH
        if exec_mode != "print" and exec_mode != "gsheet":
            raise ValueError(f"Invalid execution mode detected. Valid execution modes are: {['print', 'gsheet']}")

        debug = "--debug" if debug else ""
        smart_subject_query = "--smart-subject-query" if smart_subject_query else ""
        skip_lines_starting_with_val = " ".join(skip_lines_starting_with)
        LOG.info(f"Locals: {locals()}")
        self.execute_yarndevtools_script(
            f"{debug} "
            f"{CommandType.UNIT_TEST_RESULT_AGGREGATOR.val} "
            f"--{exec_mode} "
            f"--gsheet-client-secret {gsheet_client_secret} "
            f"--gsheet-spreadsheet {gsheet_spreadsheet} "
            f"--gsheet-worksheet {gsheet_worksheet} "
            f"--account-email {account_email} "
            f"--request-limit {request_limit} "
            f"--gmail-query {gmail_query} "
            f"--match-expression {match_expression} "
            f"--skip-lines-starting-with {skip_lines_starting_with_val} "
            f"{smart_subject_query} "
        )


if __name__ == "__main__":
    mandatory_env_vars = [CdswEnvVar.MAIL_ACC_USER.value, CdswEnvVar.MAIL_ACC_PASSWORD.value] + [
        e.value for e in DailyUTEnvVar
    ]
    basedir = CdswSetup.initial_setup(mandatory_env_vars=mandatory_env_vars)
    LOG.info(f"YARN Dev tools mirror root dir: {YARN_DEV_TOOLS_ROOT_DIR}")
    runner = CdswRunner()
    runner.start(basedir)
