from yarndevtools.cdsw.cdsw_config import Include
from yarndevtools.cdsw.constants import CdswEnvVar
from yarndevtools.common.shared_command_utils import CommandType, RepoType

algorithms = ["simple", "grouped"]


def generate_runs(conf):
    runs = [
        {
            "name": f"run-{algorithm}",
            # TODO Limitation: Variables resolution does not work with generated runs!
            #  Example: variables.commandDataFileName is not resolved in email_settings.attachment_file_name
            #  As field spec is cannot be defined and there's no way to get a reference to the current CdswRun object.
            # Workaround: Define global variable with indexed names
            "variables": {},
            "email_settings": {
                "enabled": True,
                "send_attachment": True,
                "attachment_file_name": f"{conf.var('commandDataFileName' + str(idx + 1))}",
                "email_body_file_from_command_data": "summary.html",
                "sender": f"{conf.var('sender')}",
                "subject": f"YARN branch diff report [{algorithm} algorithm, start date: {conf.job_start_date()}]",
            },
            "drive_api_upload_settings": {
                "enabled": True,
                "file_name": f"{conf.var('commandDataFileName' + str(idx + 1))}",
            },
            "yarn_dev_tools_arguments": [
                lambda conf: f"{Include.when(conf.var('debugMode'), '--debug', '')}",
                f"{CommandType.BRANCH_COMPARATOR.name}",
                algorithm,
                lambda conf: f"--repo-type {conf.var('repoType')}",
                lambda conf: conf.var("featureBranch"),
                lambda conf: conf.var("masterBranch"),
                lambda conf: f"--commit_author_exceptions {conf.var('authorsToFilter')}",
                lambda conf: f"{Include.when(conf.var('runLegacyScript'), '--run-legacy-script', '')}",
            ],
        }
        for idx, algorithm in enumerate(algorithms)
    ]
    return runs


config = {
    "job_name": "Branch comparator",
    "command_type": CommandType.BRANCH_COMPARATOR,
    "env_sanitize_exceptions": [],
    "mandatory_env_vars": [
        "MAIL_ACC_USER",
        "MAIL_ACC_PASSWORD",
    ],
    "optional_env_vars": ["BRANCH_COMP_REPO_TYPE", "BRANCH_COMP_FEATURE_BRANCH", "BRANCH_COMP_MASTER_BRANCH"],
    "yarn_dev_tools_arguments": [],
    "global_variables": {
        "sender": "YARN branch diff reporter",
        "debugMode": lambda conf: conf.env_or_default(CdswEnvVar.DEBUG_ENABLED.value, True),
        "runLegacyScript": False,
        "repoType": lambda conf: f"{conf.env_or_default('BRANCH_COMP_REPO_TYPE', RepoType.DOWNSTREAM.value.lower())}",
        "authorsToFilter": "rel-eng@cloudera.com",
        "defaultMasterBranch": "origin/cdpd-master",
        "masterBranchEnv": lambda conf: conf.env_or_default("BRANCH_COMP_MASTER_BRANCH", ""),
        "masterBranch": lambda conf: conf.var("masterBranchEnv")
        if conf.var("masterBranchEnv")
        else conf.var("defaultMasterBranch"),
        "defaultFeatureBranch": "origin/CDH-7.1-maint",
        "featureBranchEnv": lambda conf: conf.env_or_default("BRANCH_COMP_FEATURE_BRANCH", ""),
        "featureBranch": lambda conf: conf.var("featureBranchEnv")
        if conf.var("featureBranchEnv")
        else conf.var("defaultFeatureBranch"),
        "commandDataFileName1": lambda conf: f"command_data_{algorithms[0]}_{conf.job_start_date()}.zip",
        "commandDataFileName2": lambda conf: f"command_data_{algorithms[1]}_{conf.job_start_date()}.zip",
    },
    "runs": lambda conf: generate_runs(conf),
}
