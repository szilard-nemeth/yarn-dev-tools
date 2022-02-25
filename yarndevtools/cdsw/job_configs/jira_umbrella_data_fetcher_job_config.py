from yarndevtools.cdsw.cdsw_common import JiraUmbrellaDataFetcherCdswUtils, GenericCdswConfigUtils
from yarndevtools.cdsw.cdsw_config import Include
from yarndevtools.cdsw.constants import CdswEnvVar
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.constants import SummaryFile


def generate_runs(conf):
    runs = [
        {
            "name": f"run-{umbrella_id}",
            "variables": {},
            "email_settings": {
                "enabled": True,
                "send_attachment": True,
                "attachment_file_name": f"{conf.var('commandDataFileName')}",
                "email_body_file_from_command_data": SummaryFile.HTML.value,
                "sender": f"{conf.var('sender')}",
                "subject": f"YARN Upstream umbrella checker report: [UMBRELLA: {umbrella_id} ({title}), start date: {conf.job_start_date()}]",
            },
            "drive_api_upload_settings": {
                "enabled": True,
                "file_name": f"{conf.var('commandDataFileName')}",
            },
            "yarn_dev_tools_arguments": [umbrella_id],
        }
        for umbrella_id, title in JiraUmbrellaDataFetcherCdswUtils.fetch_umbrella_titles(
            GenericCdswConfigUtils.unquote(conf.var("jiraUmbrellaIds")).split(" ")
        ).items()
    ]
    return runs


config = {
    "job_name": "Jira umbrella data fetcher",
    "command_type": CommandType.JIRA_UMBRELLA_DATA_FETCHER,
    "env_sanitize_exceptions": ["UMBRELLA_IDS"],
    "mandatory_env_vars": [
        "MAIL_ACC_USER",
        "MAIL_ACC_PASSWORD",
        # OLD: umbrella_ids = ["YARN-10496", "YARN-6223", "YARN-8820"]
        # NEW: umbrella_ids = ["YARN-10888", "YARN-10889"]
        "UMBRELLA_IDS",
    ],
    "optional_env_vars": [],
    "yarn_dev_tools_arguments": [
        lambda conf: f"{Include.when(conf.var('debugMode'), '--debug', '')}",
        f"{CommandType.JIRA_UMBRELLA_DATA_FETCHER.name}",
        lambda conf: f"--branches {conf.var('branches')}",
        lambda conf: f"{Include.when(conf.var('forceMode'), '--force', '')}",
        lambda conf: f"{Include.when(conf.var('ignoreChanges'), '--ignore-changes', '')}",
    ],
    "global_variables": {
        "debugMode": lambda conf: conf.env_or_default(CdswEnvVar.DEBUG_ENABLED.value, True),
        "sender": "YARN upstream umbrella checker",
        "branches": "origin/CDH-7.1-maint origin/cdpd-master origin/CDH-7.1.6.x",
        "commandDataFileName": lambda conf: f"command_data_{conf.job_start_date()}.zip",
        "jiraUmbrellaIds": lambda conf: conf.env("UMBRELLA_IDS"),
        "forceMode": True,
        "ignoreChanges": True,
    },
    "runs": lambda conf: generate_runs(conf),
}
