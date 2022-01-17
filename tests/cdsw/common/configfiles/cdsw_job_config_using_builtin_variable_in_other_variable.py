from yarndevtools.common.shared_command_utils import CommandType

config = {
    "job_name": "Reviewsync",
    "command_type": CommandType.REVIEWSYNC,
    "mandatory_env_vars": [],
    "optional_env_vars": [],
    "yarn_dev_tools_arguments": ["--debug", "REVIEWSYNC", "--gsheet"],
    "global_variables": {
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
                "send_attachment": False,
                "attachment_file_name": lambda conf: f"{conf.var('commandDataFileName')}",
                "email_body_file_from_command_data": "report-short.html",
                "sender": "YARN reviewsync",
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
