from yarndevtools.cdsw.common.cdsw_config import Include
from yarndevtools.common.shared_command_utils import CommandType

config = {
    "job_name": "Reviewsync",
    "command_type": CommandType.REVIEWSYNC,
    "mandatory_env_vars": ["GSHEET_CLIENT_SECRET", "GSHEET_SPREADSHEET", "MAIL_ACC_USER"],
    "optional_env_vars": ["BRANCHES", "GSHEET_JIRA_COLUMN"],
    "yarn_dev_tools_arguments": [
        "--debug",
        "REVIEWSYNC",
        "--gsheet",
        lambda conf: f"--gsheet-client-secret {conf.env('GSHEET_CLIENT_SECRET')}",
        lambda conf: f"--gsheet-spreadsheet {conf.env('GSHEET_SPREADSHEET')}",
        lambda conf: f"--gsheet-jira-column {conf.env('GSHEET_JIRA_COLUMN')}",
        lambda conf: f"--arg1 {conf.var('param1')}",
        lambda conf: f"--arg2 {conf.var('param2')}",
        lambda conf: f"--arg3 {conf.var('param3')}",
        lambda conf: f"--arg4 {conf.var('param4')}",
    ],
    "global_variables": {
        "algorithm": "testAlgorithm",
        "commandDataFileName": lambda conf: f"command_data_{conf.var('algorithm')}_{conf.job_start_date()}.zip",
        "omitJobSummary": False,
        "downloadUncachedJobData": False,
        "useGoogleDriveCache": True,
        "forceSendingMail": True,
        "defaultParam1": False,
        "defaultParam2": False,
        "defaultParam3": 999,
        "defaultParam4": 1999,
        "param1": lambda conf: f"{conf.env_or_default('ENV1', conf.var('defaultParam1'))}",
        "param2": lambda conf: f"{conf.env_or_default('ENV2', conf.var('defaultParam2'))}",
        "param3": lambda conf: f"{conf.env_or_default('ENV3', conf.var('defaultParam3'))}",
        "param4": lambda conf: f"{conf.env_or_default('ENV4', conf.var('defaultParam4'))}",
    },
    "runs": [
        {
            "name": "dummy",
            "email_settings": {
                "enabled": False,
                "send_attachment": True,
                "email_body_file_from_command_data": "report-short.html",
                "attachment_file_name": "attachment_file_name",
                "subject": "testSubject",
                "sender": "testSender",
            },
            "drive_api_upload_settings": {"enabled": False, "file_name": "simple"},
            "variables": {},
            "yarn_dev_tools_arguments": [],
        }
    ],
}
