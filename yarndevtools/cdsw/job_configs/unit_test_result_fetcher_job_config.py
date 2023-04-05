from yarndevtools.cdsw.cdsw_common import MAIL_ADDR_YARN_ENG_BP, GenericCdswConfigUtils
from yarndevtools.cdsw.cdsw_config import Include
from yarndevtools.cdsw.constants import CdswEnvVar
from yarndevtools.commands.unittestresultfetcher.unit_test_result_fetcher import (
    UnitTestResultFetcherCacheType,
    DEFAULT_REQUEST_LIMIT,
    UnitTestResultFetcherMode,
    JENKINS_BUILDS_EXAMINE_UNLIMITIED_VAL,
)
from yarndevtools.common.shared_command_utils import CommandType

config = {
    "job_name": "Unit test result fetcher",
    "command_type": CommandType.UNIT_TEST_RESULT_FETCHER,
    "env_sanitize_exceptions": [],
    "mandatory_env_vars": ["MAIL_ACC_USER", "MAIL_ACC_PASSWORD", "JENKINS_USER", "JENKINS_PASSWORD"],
    "optional_env_vars": ["BUILD_PROCESSING_LIMIT", "FORCE_SENDING_MAIL", "RESET_JOB_BUILD_DATA"],
    "yarn_dev_tools_arguments": [
        lambda conf: f"{Include.when(conf.var('debugMode'), '--debug', '')}",
        f"{CommandType.UNIT_TEST_RESULT_FETCHER.name}",
        lambda conf: f"--smtp_server {conf.var('smtp_server')}",
        lambda conf: f"--smtp_port {conf.var('smtp_port')}",
        lambda conf: f"--account_user {conf.env('MAIL_ACC_USER')}",
        lambda conf: f"--account_password {conf.env('MAIL_ACC_PASSWORD')}",
        lambda conf: f"--jenkins-user {conf.env('JENKINS_USER')}",
        lambda conf: f"--jenkins-password {conf.env('JENKINS_PASSWORD')}",
        lambda conf: f"--sender {conf.var('sender')}",
        lambda conf: f"--recipients {conf.var('recipients')}",
        lambda conf: f"--mode {conf.var('mode')}",
        lambda conf: f"--testcase-filter {conf.var('testcase_filters')}",
        lambda conf: f"--request-limit {conf.var('buildProcessingLimit')}",
        lambda conf: f"--num-builds {JENKINS_BUILDS_EXAMINE_UNLIMITIED_VAL}",
        lambda conf: f"{Include.when(conf.var('omitJobSummary'), '--omit-job-summary', '')}",
        lambda conf: f"{Include.when(conf.var('downloadUncachedJobData'), '--download-uncached-job-data', '')}",
        lambda conf: f"{Include.when(conf.var('forceSendingEmail'), '--force-sending-email', '')}",
        lambda conf: f"{Include.when(conf.var('useGoogleDriveCache'), f'--cache-type {UnitTestResultFetcherCacheType.GOOGLE_DRIVE.value.lower()}', '')}",
        lambda conf: f"{Include.when(conf.var('resetJobBuildData'), '{}'.format(conf.var('resetJobBuildDataVal')), '')}",
    ],
    "global_variables": {
        "debugMode": lambda conf: conf.env_or_default(CdswEnvVar.DEBUG_ENABLED.value, True),
        "sender": GenericCdswConfigUtils.quote("YARN unit test result fetcher"),
        "subject": lambda conf: f"YARN unit test result fetcher report [start date: {conf.job_start_date()}]",
        "commandDataFileName": lambda conf: f"command_data_{conf.job_start_date()}.zip",
        "defaultForceSendingEmail": False,
        "defaultResetJobBuildData": False,
        "omitJobSummary": False,
        "downloadUncachedJobData": False,
        "useGoogleDriveCache": True,
        "defaultBuildProcessingLimit": DEFAULT_REQUEST_LIMIT,
        "allJobNames": f'{" ".join(UnitTestResultFetcherMode.JENKINS_MASTER.job_names)}',
        "defaultEmailRecipients": MAIL_ADDR_YARN_ENG_BP,
        "mode": UnitTestResultFetcherMode.JENKINS_MASTER.mode_name,
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 465,
        "tc_filter_yarn": "YARN:org.apache.hadoop.yarn",
        "tc_filter_mr": "MAPREDUCE:org.apache.hadoop.mapreduce",
        "tc_filter_hdfs": "HDFS:org.apache.hadoop.hdfs",
        "tc_filter_hadoop_common": "HADOOP_COMMON:org.apache.hadoop",
        "testcase_filters": lambda conf: f"{conf.var('tc_filter_yarn')} {conf.var('tc_filter_mr')} {conf.var('tc_filter_hdfs')} {conf.var('tc_filter_hadoop_common')}",
        "recipients": lambda conf: f"{conf.env_or_default('MAIL_RECIPIENTS', conf.var('defaultEmailRecipients'))}",
        "forceSendingEmail": lambda conf: conf.env_or_default(
            "FORCE_SENDING_MAIL", conf.var("defaultForceSendingEmail")
        ),
        "resetJobBuildData": lambda conf: conf.env_or_default(
            "RESET_JOB_BUILD_DATA", conf.var("defaultResetJobBuildData")
        ),
        "resetJobBuildDataArg": lambda conf: f"--reset-job-build-data-for-jobs {conf.var('allJobNames')}",
        "resetJobBuildDataVal": lambda conf: f"{conf.var('resetJobBuildDataArg')}",
        "buildProcessingLimit": lambda conf: f"{conf.env_or_default('BUILD_PROCESSING_LIMIT', conf.var('defaultBuildProcessingLimit'))}",
    },
    "runs": [
        {
            "name": "run1",
            "variables": {},
            "email_settings": {
                "enabled": False,
                "send_attachment": False,
                "attachment_file_name": lambda conf: f"{conf.var('commandDataFileName')}",
                "email_body_file_from_command_data": "N/A",
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
