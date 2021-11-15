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

LOG = logging.getLogger(__name__)
CMD_LOG = logging.getLogger(__name__)
TC_FILTER_YARN = "YARN:org.apache.hadoop.yarn"
TC_FILTER_MR = "MAPREDUCE:org.apache.hadoop.mapreduce"
TC_FILTER_HDFS = "HDFS:org.apache.hadoop.hdfs"
TC_FILTER_ALL = f"{TC_FILTER_MR} {TC_FILTER_YARN} {TC_FILTER_HDFS}"
MAWO_JOBS = ["Mawo-UT-hadoop-CDPD-7.x", "Mawo-UT-hadoop-CDPD-7.1.x"]


class CdswRunner(CdswRunnerBase):
    def start(self, basedir):
        self.start_common(basedir)
        self.run_clone_downstream_repos_script(basedir)
        self.run_test_reporter(job_names=MAWO_JOBS)

    def run_test_reporter(self, job_names: List[str], recipients=None, testcase_filter: str = TC_FILTER_ALL):
        if not job_names:
            raise ValueError("Jenkins job names should be specified in a list!")

        if not recipients:
            recipients = self.determine_recipients()
        process_builds: int = OsUtils.get_env_value(JenkinsTestReporterEnvVar.BUILD_PROCESSING_LIMIT.value, 1)
        LOG.info(f"Processing {process_builds} builds...")
        sender = "YARN jenkins test reporter"
        tc_filter_param = f"--testcase-filter {testcase_filter}"
        job_names_param = ",".join(job_names)
        self.execute_yarndevtools_script(
            f"--debug {CommandType.JENKINS_TEST_REPORTER.name} "
            f"--job-names {job_names_param} "
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
