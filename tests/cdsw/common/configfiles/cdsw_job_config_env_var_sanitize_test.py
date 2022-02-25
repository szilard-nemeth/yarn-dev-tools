from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.constants import ReportFile

config = {
    "job_name": "Reviewsync",
    "command_type": CommandType.REVIEWSYNC,
    "env_sanitize_exceptions": ["BRANCHES"],
    "mandatory_env_vars": [
        "GSHEET_WORKSHEET",
        "GSHEET_SPREADSHEET",
        "GSHEET_JIRA_COLUMN",
        "GSHEET_STATUS_INFO_COLUMN",
        "GSHEET_UPDATE_DATE_COLUMN",
        "BRANCHES",
    ],
    "optional_env_vars": [],
    "yarn_dev_tools_arguments": [
        "--debug",
        "REVIEWSYNC",
        "--gsheet",
        lambda conf: f"--arg1 {conf.env('GSHEET_WORKSHEET')}",
        lambda conf: f"--arg2 {conf.env('GSHEET_SPREADSHEET')}",
        lambda conf: f"--arg3 {conf.env('GSHEET_JIRA_COLUMN')}",
        lambda conf: f"--arg4 {conf.env('GSHEET_STATUS_INFO_COLUMN')}",
        lambda conf: f"--arg5 {conf.env('GSHEET_UPDATE_DATE_COLUMN')}",
        lambda conf: f"--arg6 {conf.env('BRANCHES')}",
    ],
    "global_variables": {},
    "runs": [
        {
            "name": "run1",
            "variables": {},
            "email_settings": {
                "enabled": False,
                "send_attachment": False,
                "attachment_file_name": "test1",
                "email_body_file_from_command_data": ReportFile.SHORT_HTML.value,
                "sender": "sender",
                "subject": "subject",
            },
            "drive_api_upload_settings": {"enabled": True, "file_name": "test2"},
            "yarn_dev_tools_arguments": [],
        }
    ],
}
