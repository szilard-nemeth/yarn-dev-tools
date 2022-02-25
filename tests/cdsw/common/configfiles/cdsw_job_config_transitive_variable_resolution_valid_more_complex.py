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
        "varA": lambda conf: f"{conf.var('varB')}",
        "varB": lambda conf: f"{conf.var('varC')}",
        "varC": lambda conf: f"{conf.var('varD')}",
        "varD": lambda conf: f"{conf.var('varE')}",
        "varE": "no_var_here",
        "varU": lambda conf: f"{conf.var('varT')}",
        "varT": lambda conf: f"{conf.var('varZ')}{conf.var('varS')}",
        "varS": "s",
        "varZ": lambda conf: f"{conf.var('varX')}{conf.var('varY')}",
        "varX": "x",
        "varY": "y",
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
            "variables": {},
            "yarn_dev_tools_arguments": [],
        }
    ],
}
