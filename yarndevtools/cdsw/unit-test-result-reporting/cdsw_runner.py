#!/usr/bin/env python3

import logging
from typing import List

from pythoncommons.os_utils import OsUtils

from yarndevtools.argparser import CommandType
from yarndevtools.cdsw.common_python.cdsw_common import (
    CdswRunnerBase,
    CdswSetup,
)
from yarndevtools.cdsw.common_python.constants import CdswEnvVar, JenkinsTestReporterEnvVar
from yarndevtools.commands.jenkinstestreporter.jenkins_test_reporter import JenkinsTestReporterMode

LOG = logging.getLogger(__name__)
CMD_LOG = logging.getLogger(__name__)
TC_FILTER_YARN = "YARN:org.apache.hadoop.yarn"
TC_FILTER_MR = "MAPREDUCE:org.apache.hadoop.mapreduce"
TC_FILTER_HDFS = "HDFS:org.apache.hadoop.hdfs"
TC_FILTER_HADOOP_COMMON = "HADOOP COMMON:org.apache.hadoop"
TC_FILTER_ALL = f"{TC_FILTER_MR} {TC_FILTER_YARN} {TC_FILTER_HDFS} {TC_FILTER_HADOOP_COMMON}"


class CdswRunner(CdswRunnerBase):
    def start(self, basedir):
        self.start_common(basedir)
        self.run_clone_downstream_repos_script(basedir)
        self.run_test_reporter(mode=JenkinsTestReporterMode.MAWO)

    def run_test_reporter(self, mode: JenkinsTestReporterMode, recipients=None, testcase_filter: str = TC_FILTER_ALL):
        if not mode:
            raise ValueError("Jenkins job mode should be specified!")

        if not recipients:
            recipients = self.determine_recipients()
        process_builds: int = OsUtils.get_env_value(JenkinsTestReporterEnvVar.BUILD_PROCESSING_LIMIT.value, 1)
        LOG.info(f"Processing {process_builds} builds...")
        sender = "YARN jenkins test reporter"
        tc_filter_param = f"--testcase-filter {testcase_filter}"
        self.execute_yarndevtools_script(
            f"--debug {CommandType.JENKINS_TEST_REPORTER.name} "
            f"--mode {mode} "
            f"{self.common_mail_config.as_arguments()}"
            f'--sender "{sender}" '
            f'--recipients "{recipients}" '
            f"{tc_filter_param} "
            f"--request-limit {process_builds}"
        )


if __name__ == "__main__":
    basedir = CdswSetup.initial_setup(
        mandatory_env_vars=[CdswEnvVar.MAIL_ACC_USER.value, CdswEnvVar.MAIL_ACC_PASSWORD.value]
    )
    runner = CdswRunner()
    runner.start(basedir)
