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
    NUM_BUILDS_UNLIMITED = 999999
    num_builds_per_config = NUM_BUILDS_UNLIMITED

    def __init__(
        self,
        builds: List[JobBuildData],
        total_num_builds: int = NUM_BUILDS_UNLIMITED,
        num_builds_per_config: int = NUM_BUILDS_UNLIMITED,
    ):
        JenkinsJobResult.num_builds_per_config = num_builds_per_config

        self.builds: List[JobBuildData] = builds
        self.total_num_builds: int = total_num_builds
        self.failure_count_by_testcase: Dict[str, int] = {}
        self._index = 0

        # Computed fields
        self._builds_by_url = None
        self._job_urls = None
        self._actual_num_builds = JenkinsJobResult.NUM_BUILDS_UNLIMITED
        self._compute_dynamic_fields()
        self._finalized = False

    @staticmethod
    def create_empty(total_no_of_builds: int = NUM_BUILDS_UNLIMITED, num_builds_per_config: int = NUM_BUILDS_UNLIMITED):
        return JenkinsJobResult([], total_no_of_builds, num_builds_per_config)

    def _compute_dynamic_fields(self):
        self._builds_by_url: Dict[str, JobBuildData] = {job.build_url: job for job in self.builds}
        self._job_urls = list(sorted(self._builds_by_url.keys(), reverse=True))  # Sort by URL, descending
        self._actual_num_builds = self._determine_actual_number_of_builds()
        if self.total_num_builds == JenkinsJobResult.NUM_BUILDS_UNLIMITED:
            self.total_num_builds = self._actual_num_builds

    def finalize(self):
        self._compute_dynamic_fields()
        self._create_testcase_to_fail_count_dict()
        self._finalized = True

    def start_processing(self):
        LOG.info(f"Jenkins job result contains the following results: {self._job_urls}")
        LOG.info(f"Processing {self._actual_num_builds} builds..")

    def add_build(self, job_data):
        if self._finalized:
            raise ValueError("Cannot add build, object is already finalized!")
        self.builds.append(job_data)

    def merge_with(self, job_result):
        a = self
        b = job_result
        builds = self._merge_build_data_list(a, b)
        job_result = JenkinsJobResult(builds)
        job_result.finalize()
        return job_result

    @staticmethod
    def _merge_build_data_list(a, b):
        a_builds: Dict[str, JobBuildData] = {job.build_url: job for job in a.builds}
        b_builds: Dict[str, JobBuildData] = {job.build_url: job for job in b.builds}
        all_builds_dict = dict(a_builds)
        for url, build_data in b_builds.items():
            if url in all_builds_dict:
                LOG.warning(
                    "[MERGE JENKINS RESULTS] Overwriting old build data with newer build data from Jenkins, URL: %s",
                    url,
                )
            else:
                LOG.info("[MERGE JENKINS RESULTS] Found new build data from Jenkins, URL: %s", url)
            all_builds_dict[url] = build_data
        return list(all_builds_dict.values())

    def __len__(self):
        return self._actual_num_builds

    def __iter__(self):
        self._index = 0
        return self

    def __next__(self):
        if self._index == self._actual_num_builds:
            raise StopIteration
        result = self._builds_by_url[self._job_urls[self._index]]
        self._index += 1
        return result

    def _determine_actual_number_of_builds(self):
        num_builds_per_config = JenkinsJobResult.num_builds_per_config
        build_data_count = len(self._builds_by_url)
        if 0 < build_data_count < self.total_num_builds:
            LOG.warning(
                "Jenkins job result contains less builds than total number of builds. Actual: %d, Total: %d",
                build_data_count,
                self.total_num_builds,
            )
            actual_num_builds = min(num_builds_per_config, build_data_count)
        else:
            actual_num_builds = min(num_builds_per_config, self.total_num_builds)
        return actual_num_builds

    def _create_testcase_to_fail_count_dict(self):
        for job_data in self.builds:
            if job_data.has_failed_testcases():
                for failed_testcase in job_data.failed_testcases:
                    LOG.debug(f"Detected failed testcase: {failed_testcase}")
                    self.failure_count_by_testcase[failed_testcase] = (
                        self.failure_count_by_testcase.get(failed_testcase, 0) + 1
                    )

    @property
    def known_build_urls(self):
        return self._builds_by_url.keys()

    def are_all_mail_sent(self):
        return all(job_data.mail_sent for job_data in self._builds_by_url.values())

    def reset_mail_sent_state(self):
        for job_data in self._builds_by_url.values():
            job_data.sent_date = None
            job_data.mail_sent = False

    def mark_sent(self, build_url):
        job_data = self._builds_by_url[build_url]
        job_data.sent_date = DateUtils.get_current_datetime()
        job_data.mail_sent = True

    def get_job_data(self, build_url: str):
        return self._builds_by_url[build_url]

    def print(self, build_data):
        LOG.info(f"\nPRINTING JOB RESULT: \n\n{build_data}")
        LOG.info(f"\nAmong {self.total_num_builds} runs examined, all failed tests <#failedRuns: testName>:")
        # Print summary section: all failed tests sorted by how many times they failed
        LOG.info("TESTCASE SUMMARY:")
        for tn in sorted(self.failure_count_by_testcase, key=self.failure_count_by_testcase.get, reverse=True):
            LOG.info(f"{self.failure_count_by_testcase[tn]}: {tn}")
