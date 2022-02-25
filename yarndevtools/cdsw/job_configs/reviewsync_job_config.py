from yarndevtools.cdsw.cdsw_config import Include
from yarndevtools.cdsw.constants import CdswEnvVar
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.constants import SummaryFile

config = {
    "job_name": "Reviewsync",
    "command_type": CommandType.REVIEWSYNC,
    "env_sanitize_exceptions": ["BRANCHES"],
    "mandatory_env_vars": [
        "GSHEET_CLIENT_SECRET",
        "GSHEET_WORKSHEET",
        "GSHEET_SPREADSHEET",
        "GSHEET_JIRA_COLUMN",
        "GSHEET_UPDATE_DATE_COLUMN",
        "GSHEET_STATUS_INFO_COLUMN",
        "MAIL_ACC_USER",
        "MAIL_ACC_PASSWORD",
        "BRANCHES",
    ],
    "optional_env_vars": [],
    "yarn_dev_tools_arguments": [
        lambda conf: f"{Include.when(conf.var('debugMode'), '--debug', '')}",
        f"{CommandType.REVIEWSYNC.name}",
        "--gsheet",
        lambda conf: f"--gsheet-client-secret {conf.env('GSHEET_CLIENT_SECRET')}",
        lambda conf: f"--gsheet-worksheet {conf.env('GSHEET_WORKSHEET')}",
        lambda conf: f"--gsheet-spreadsheet {conf.env('GSHEET_SPREADSHEET')}",
        lambda conf: f"--gsheet-jira-column {conf.env('GSHEET_JIRA_COLUMN')}",
        lambda conf: f"--gsheet-update-date-column {conf.env('GSHEET_UPDATE_DATE_COLUMN')}",
        lambda conf: f"--gsheet-status-info-column {conf.env('GSHEET_STATUS_INFO_COLUMN')}",
        lambda conf: f"--branches {conf.env('BRANCHES')}",
    ],
    "global_variables": {
        "debugMode": lambda conf: conf.env_or_default(CdswEnvVar.DEBUG_ENABLED.value, True),
        "sender": "YARN reviewsync",
        "subject": lambda conf: f"YARN reviewsync report [start date: {conf.job_start_date()}]",
        "commandDataFileName": lambda conf: f"command_data_{conf.job_start_date()}.zip",
    },
    "runs": [
        {
            "name": "run1",
            "variables": {},
            "email_settings": {
                "enabled": True,
                "send_attachment": True,
                "attachment_file_name": lambda conf: f"{conf.var('commandDataFileName')}",
                "email_body_file_from_command_data": SummaryFile.HTML.value,
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
