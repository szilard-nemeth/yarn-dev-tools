from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.constants import ReportFile

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
    ],
    "global_variables": {
        "algorithm": "testAlgorithm",
        "commandDataFileName": lambda conf: f"command_data_{conf.var('algorithm')}_{conf.job_start_date()}.zip",
        "global1": "globalValue1",
        "global2": "globalValue2",
    },
    "runs": [
        {
            "name": "dummy",
            "email_settings": {
                "enabled": False,
                "send_attachment": True,
                "email_body_file_from_command_data": ReportFile.SHORT_HTML.value,
                "attachment_file_name": "attachment_file_name",
                "subject": "testSubject",
                "sender": "testSender",
            },
            "drive_api_upload_settings": {"enabled": False, "file_name": "simple"},
            "variables": {
                "algorithm": "yetAnotherAlgorithm",
                "commandDataFileName": "overriddenCommandData",
                "testGlobal1": lambda conf: f"something+{conf.var('global1')}",
                "testGlobal2": lambda conf: f"something+{conf.var('global2')}",
                "testNewVar1": "a new variable",
            },
            "yarn_dev_tools_arguments": [
                lambda conf: f"--testArg1 {conf.var('algorithm')}",
                lambda conf: f"--testArg2 {conf.var('commandDataFileName')}",
                lambda conf: f"--testArg3 {conf.var('testGlobal1')}",
                lambda conf: f"--testArg4 {conf.var('testGlobal2')}",
                lambda conf: f"--testArg5 {conf.var('testNewVar1')}",
            ],
        }
    ],
}
