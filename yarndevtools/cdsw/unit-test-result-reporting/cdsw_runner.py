import logging
from pythoncommons.os_utils import OsUtils

from yarndevtools.cdsw.common_python.cdsw_common import (
    CdswRunnerBase,
    MAIL_ADDR_YARN_ENG_BP,
    CdswSetup,
)
from yarndevtools.cdsw.common_python.constants import CdswEnvVar, JenkinsTestReporterEnvVar

LOG = logging.getLogger(__name__)
CMD_LOG = logging.getLogger(__name__)
TC_FILTER_YARN = "YARN:org.apache.hadoop.yarn"
TC_FILTER_MR = "MAPREDUCE:org.apache.hadoop.mapreduce"
TC_FILTER_HDFS = "HDFS:org.apache.hadoop.hdfs"
TC_FILTER_ALL = f"{TC_FILTER_MR} {TC_FILTER_YARN} {TC_FILTER_HDFS}"


class CdswRunner(CdswRunnerBase):
    def start(self, basedir):
        LOG.info("Starting CDSW runner...")
        self.run_clone_downstream_repos_script(basedir)

        cdpd_master_job = "Mawo-UT-hadoop-CDPD-7.x"
        cdh_71_maint_job = "Mawo-UT-hadoop-CDPD-7.1.x"
        self.run_test_reporter(job_name=cdpd_master_job)
        self.run_test_reporter(job_name=cdh_71_maint_job)

    def run_test_reporter(self, job_name: str, recipients=MAIL_ADDR_YARN_ENG_BP, testcase_filter: str = TC_FILTER_ALL):
        if not job_name:
            raise ValueError("Jenkins job name should be specified")

        process_builds: int = OsUtils.get_env_value(JenkinsTestReporterEnvVar.BUILD_PROCESSING_LIMIT.value, 1)
        LOG.info(f"Processing {process_builds} builds...")
        sender = "YARN jenkins test reporter"
        tc_filter_param = f"--testcase-filter {testcase_filter}"
        self.execute_yarndevtools_script(
            f"--debug jenkins_test_reporter "
            f"--job-name {job_name} "
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
