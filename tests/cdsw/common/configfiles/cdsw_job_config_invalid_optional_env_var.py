from yarndevtools.common.shared_command_utils import CommandType

config = {
    "job_name": "Reviewsync",
    "command_type": CommandType.REVIEWSYNC,
    "mandatory_env_vars": ["GSHEET_CLIENT_S"],
    "optional_env_vars": ["WRONG_VAR"],
    "runs": [{"name": "dummy", "variables": {}, "yarn_dev_tools_arguments": []}],
}
