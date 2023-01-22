from dataclasses import dataclass
from typing import Dict, List

from pythoncommons.date_utils import DateUtils
from pythoncommons.string_utils import auto_str

from yarndevtools.common.common_model import JobBuildData
import logging

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class CachedBuildKey:
    job_name: str
    build_number: int


@dataclass
class CachedBuild:
    build_key: CachedBuildKey
    full_report_file_path: str


@auto_str
class JenkinsJobResult:
    def __init__(self, job_build_datas, all_failing_tests, total_no_of_builds: int, num_builds_per_config: int):
        self.job_build_datas: List[JobBuildData] = job_build_datas
        self.all_failing_tests: Dict[str, int] = all_failing_tests
        self.total_no_of_builds: int = total_no_of_builds
        self.num_builds_per_config: int = num_builds_per_config

        # Projected fields
        self._jobs_by_url: Dict[str, JobBuildData] = {job.build_url: job for job in self.job_build_datas}
        self._job_urls = list(sorted(self._jobs_by_url.keys(), reverse=True))  # Sort by URL, descending
        self._actual_num_builds = self._determine_actual_number_of_builds(self.num_builds_per_config)
        self._index = 0

    def start_processing(self):
        LOG.info(f"Jenkins job result contains the following results: {self._job_urls}")
        LOG.info(f"Processing {self._actual_num_builds} builds..")

    def __len__(self):
        return self._actual_num_builds

    def __iter__(self):
        self._index = 0
        return self

    def __next__(self):
        if self._index == self._actual_num_builds:
            raise StopIteration
        result = self._jobs_by_url[self._job_urls[self._index]]
        self._index += 1
        return result

    def _determine_actual_number_of_builds(self, num_builds_per_config):
        build_data_count = len(self._jobs_by_url)
        total_no_of_builds = self.total_no_of_builds
        if build_data_count < total_no_of_builds:
            LOG.warning(
                "Jenkins job result contains less builds than total number of builds. " "Actual: %d, Total: %d",
                build_data_count,
                total_no_of_builds,
            )
            actual_num_builds = min(num_builds_per_config, build_data_count)
        else:
            actual_num_builds = min(num_builds_per_config, self.total_no_of_builds)
        return actual_num_builds

    @property
    def known_build_urls(self):
        return self._jobs_by_url.keys()

    def are_all_mail_sent(self):
        return all(job_data.mail_sent for job_data in self._jobs_by_url.values())

    def reset_mail_sent_state(self):
        for job_data in self._jobs_by_url.values():
            job_data.sent_date = None
            job_data.mail_sent = False

    def mark_sent(self, build_url):
        job_data = self._jobs_by_url[build_url]
        job_data.sent_date = DateUtils.get_current_datetime()
        job_data.mail_sent = True

    def get_job_data(self, build_url: str):
        return self._jobs_by_url[build_url]

    def print(self, build_data):
        LOG.info(f"\nPRINTING JOB RESULT: \n\n{build_data}")
        LOG.info(f"\nAmong {self.total_no_of_builds} runs examined, all failed tests <#failedRuns: testName>:")
        # Print summary section: all failed tests sorted by how many times they failed
        LOG.info("TESTCASE SUMMARY:")
        for tn in sorted(self.all_failing_tests, key=self.all_failing_tests.get, reverse=True):
            LOG.info(f"{self.all_failing_tests[tn]}: {tn}")
