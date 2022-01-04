#!/usr/bin/env python3

import logging

from googleapiwrapper.google_drive import DriveApiFile
from pythoncommons.os_utils import OsUtils

from yarndevtools.cdsw.common_python.cdsw_common import (
    CdswRunnerBase,
    CdswSetup,
    CdswSetupResult,
)
from yarndevtools.cdsw.common_python.constants import (
    CdswEnvVar,
    ReviewSheetBackportUpdaterEnvVar,
)
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.constants import REPORT_FILE_SHORT_HTML

LOG = logging.getLogger(__name__)
CMD_LOG = logging.getLogger(__name__)


class CdswRunner(CdswRunnerBase):
    def start(self, setup_result: CdswSetupResult, cdsw_runner_script_path: str):
        self.start_common(setup_result, cdsw_runner_script_path)
        self.run_backport_updater_and_send_mail()

    def run_backport_updater_and_send_mail(self):
        self._run_backport_updater(
            gsheet_client_secret=OsUtils.get_env_value(ReviewSheetBackportUpdaterEnvVar.GSHEET_CLIENT_SECRET.value),
            gsheet_spreadsheet=OsUtils.get_env_value(ReviewSheetBackportUpdaterEnvVar.GSHEET_SPREADSHEET.value),
            gsheet_worksheet=OsUtils.get_env_value(ReviewSheetBackportUpdaterEnvVar.GSHEET_WORKSHEET.value),
            gsheet_jira_column=OsUtils.get_env_value(ReviewSheetBackportUpdaterEnvVar.GSHEET_JIRA_COLUMN.value),
            gsheet_update_date_column=OsUtils.get_env_value(
                ReviewSheetBackportUpdaterEnvVar.GSHEET_UPDATE_DATE_COLUMN.value
            ),
            gsheet_status_info_column=OsUtils.get_env_value(
                ReviewSheetBackportUpdaterEnvVar.GSHEET_STATUS_INFO_COLUMN.value
            ),
            branches=OsUtils.get_env_value(ReviewSheetBackportUpdaterEnvVar.BRANCHES.value),
            account_email=OsUtils.get_env_value(CdswEnvVar.MAIL_ACC_USER.value),
        )

        cmd_type = CommandType.REVIEW_SHEET_BACKPORT_UPDATER
        self.run_zipper(cmd_type, debug=True)

        sender = "YARN review sheet backport updater"
        subject = f"YARN review sheet backport updater report [start date: {self.start_date_str}]"
        command_data_filename: str = f"command_data_{self.start_date_str}.zip"
        kwargs = {
            "attachment_filename": command_data_filename,
            "email_body_file": REPORT_FILE_SHORT_HTML,
            "send_attachment": True,
        }
        if self.is_drive_integration_enabled:
            drive_api_file: DriveApiFile = self.upload_command_data_to_drive(cmd_type, command_data_filename)
            link_text = f'<a href="{drive_api_file.link}">Command data file: {command_data_filename}</a>'
            kwargs["prepend_text_to_email_body"] = link_text
            kwargs["send_attachment"] = False
        self.send_latest_command_data_in_email(
            sender=sender,
            subject=subject,
            **kwargs,
        )

    def _run_backport_updater(
        self,
        gsheet_client_secret,
        gsheet_spreadsheet,
        gsheet_worksheet,
        gsheet_jira_column,
        gsheet_update_date_column,
        gsheet_status_info_column,
        branches,
        account_email,
        debug=True,
        summary_mode="html",
    ):
        debug = "--debug" if debug else ""
        LOG.info(f"Locals: {locals()}")
        self.execute_yarndevtools_script(
            f"{debug} "
            f"{CommandType.REVIEW_SHEET_BACKPORT_UPDATER.name} "
            f"--gsheet-client-secret {gsheet_client_secret} "
            f"--gsheet-spreadsheet {gsheet_spreadsheet} "
            f"--gsheet-worksheet {gsheet_worksheet} "
            f"--gsheet-jira-column {gsheet_jira_column} "
            f"--gsheet-update-date-column {gsheet_update_date_column} "
            f"--gsheet-status-info-column {gsheet_status_info_column} "
            f"--branches {branches} "
        )

    @staticmethod
    def _get_cli_switch_value(switch_name, val, quote=False):
        if not val:
            return ""
        if quote:
            val = '"' + val + '"'
        return f"{switch_name} {val}"


if __name__ == "__main__":
    # TODO Check if mandatory env vars are fine (Add more vars to mandatory env vars)
    mandatory_env_vars = [CdswEnvVar.MAIL_ACC_USER.value, CdswEnvVar.MAIL_ACC_PASSWORD.value] + [
        e.value for e in ReviewSheetBackportUpdaterEnvVar
    ]
    setup_result: CdswSetupResult = CdswSetup.initial_setup(mandatory_env_vars=mandatory_env_vars)
    runner = CdswRunner()
    runner.start(setup_result, CdswRunnerBase.get_filename(CommandType.REVIEW_SHEET_BACKPORT_UPDATER.output_dir_name))
