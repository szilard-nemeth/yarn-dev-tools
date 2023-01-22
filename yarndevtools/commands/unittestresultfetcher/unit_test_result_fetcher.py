#!/usr/local/bin/python3
import logging
import sys
import traceback
from typing import List, Dict

from pythoncommons.constants import ExecutionMode
from pythoncommons.date_utils import DateUtils
from pythoncommons.file_utils import JsonFileUtils
from pythoncommons.logging_setup import SimpleLoggingSetup
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils

from yarndevtools.commands.unittestresultfetcher.cache import (
    Cache,
    GoogleDriveCache,
    FileCache,
    UnitTestResultFetcherCacheType,
    CacheConfig,
)
from yarndevtools.commands.unittestresultfetcher.common import UnitTestResultFetcherMode, FileNameUtils
from yarndevtools.commands.unittestresultfetcher.db import JenkinsJobResults, UTResultFetcherDatabase
from yarndevtools.commands.unittestresultfetcher.email import Email, EmailConfig
from yarndevtools.commands.unittestresultfetcher.jenkins import JenkinsJobUrls, JenkinsApi, DownloadProgress
from yarndevtools.commands.unittestresultfetcher.model import JenkinsJobResult, CachedBuildKey
from yarndevtools.commands.unittestresultfetcher.parser import UnitTestResultFetcherParser
from yarndevtools.commands_common import CommandAbs
from yarndevtools.common.common_model import (
    JobBuildData,
    FailedJenkinsBuild,
    JobBuildDataStatus,
    JenkinsTestcaseFilter,
)
from yarndevtools.common.db import MongoDbConfig
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME

LOG = logging.getLogger(__name__)
DEFAULT_REQUEST_LIMIT = 999
JENKINS_BUILDS_EXAMINE_UNLIMITIED_VAL = "jenkins_examine_unlimited_builds"


class UnitTestResultFetcherConfig:
    def __init__(self, output_dir: str, args):
        self.args = args
        self.force_download_mode = args.force_download_mode if hasattr(args, "force_download_mode") else False
        self.load_cached_reports_to_db: bool = (
            args.load_cached_reports_to_db if hasattr(args, "load_cached_reports_to_db") else False
        )
        self.cache: CacheConfig = CacheConfig(
            args,
            output_dir,
            force_download_mode=self.force_download_mode,
            load_cached_reports_to_db=self.load_cached_reports_to_db,
        )
        self.email: EmailConfig = EmailConfig(args)
        self.request_limit = args.req_limit if hasattr(args, "req_limit") and args.req_limit else 1
        self.jenkins_mode: UnitTestResultFetcherMode = (
            UnitTestResultFetcherMode[args.jenkins_mode.upper()]
            if hasattr(args, "jenkins_mode") and args.jenkins_mode
            else None
        )
        self.jenkins_base_url = args.jenkins_url
        self.job_names: List[str] = args.job_names.split(",")
        self.num_builds: int = self._determine_number_of_builds_to_examine(args.num_builds, self.request_limit)
        tc_filters_raw = args.tc_filters if hasattr(args, "tc_filters") and args.tc_filters else []
        self.tc_filters: List[JenkinsTestcaseFilter] = [
            JenkinsTestcaseFilter(*tcf.split(":")) for tcf in tc_filters_raw
        ]
        self.full_cmd: str = OsUtils.determine_full_command_filtered(filter_password=True)
        self.omit_job_summary: bool = args.omit_job_summary if hasattr(args, "omit_job_summary") else False
        self.fail_on_all_green_job_result: bool = False  # TODO hardcoded
        self.fail_on_empty_job_result: bool = False  # TODO hardcoded
        self.fail_on_no_data_job_result: bool = False  # TODO hardcoded
        self.reset_job_build_data_for_jobs: List[str] = (
            args.reset_job_build_data_for_jobs if hasattr(args, "reset_job_build_data_for_jobs") else []
        )
        self.mongo_config = MongoDbConfig(args)

        # Validation
        if not self.tc_filters:
            LOG.warning("TESTCASE FILTER IS NOT SET!")
        if self.jenkins_mode and (self.jenkins_base_url or self.job_names):
            LOG.warning(
                "Jenkins mode is set to %s. \n"
                "Specified values for Jenkins URL: %s\n"
                "Specified values for job names: %s\n"
                "Jenkins mode will take precedence!",
                self.jenkins_mode,
                self.jenkins_base_url,
                self.job_names,
            )
            self.jenkins_base_url = self.jenkins_mode.jenkins_base_url
            self.job_names = self.jenkins_mode.job_names

        if not all([reset in self.job_names for reset in self.reset_job_build_data_for_jobs]):
            raise ValueError(
                "Not all jobs are recognized while trying to reset job build data for jobs! "
                "Valid job names: {}, Current job names: {}".format(self.job_names, self.reset_job_build_data_for_jobs)
            )

        self.email.validate(self.job_names)

    @staticmethod
    def _determine_number_of_builds_to_examine(config_value, request_limit) -> int:
        if config_value == JENKINS_BUILDS_EXAMINE_UNLIMITIED_VAL:
            return sys.maxsize

        no_of_builds = int(config_value)
        if request_limit < no_of_builds:
            LOG.warning("Limiting the number of builds to fetch by the request limit: %s", request_limit)
        return min(no_of_builds, request_limit)

    def __str__(self):
        # TODO Add all config properties
        return (
            f"Full command was: {self.full_cmd}\n"
            f"Jenkins URL: {self.jenkins_base_url}\n"
            f"Jenkins job names: {self.job_names}\n"
            f"Number of builds to check: {self.num_builds}\n"
            f"Testcase filters: {self.tc_filters}\n"
            f"Force download mode: {self.force_download_mode}\n"
            f"Force email send mode: {self.email.force_send_email}\n"
        )


# TODO Separate all download functionality: Progress of downloads, code that fetches API, etc.
class UnitTestResultFetcher(CommandAbs):
    def __init__(self, args, output_dir):
        super().__init__()
        self.config = UnitTestResultFetcherConfig(output_dir, args)
        self.job_results: JenkinsJobResults = None
        self.cache: Cache = self._create_cache(self.config)
        self.email: Email = Email(self.config.email)
        self._database = UTResultFetcherDatabase(self.config.mongo_config)

    @staticmethod
    def create_parser(subparsers):
        UnitTestResultFetcherParser.create(subparsers, UnitTestResultFetcher.execute)

    @staticmethod
    def execute(args, parser=None):
        output_dir = ProjectUtils.get_output_child_dir(CommandType.UNIT_TEST_RESULT_FETCHER.output_dir_name)
        jenkins_test_reporter = UnitTestResultFetcher(args, output_dir)
        jenkins_test_reporter.run()

    @staticmethod
    def _convert_to_cache_build_key(failed_build: FailedJenkinsBuild):
        # Cached build data is stored in dirs with dots replaced by underscores,
        # make CachedBuildKey to follow the dir name pattern, so job_names are always consistent when used in
        # CachedBuildKey.
        job_name = FileNameUtils.escape_job_name(failed_build.job_name)
        return CachedBuildKey(job_name, failed_build.build_number)

    @staticmethod
    def _create_cache(config: UnitTestResultFetcherConfig):
        if config.cache.cache_type == UnitTestResultFetcherCacheType.FILE:
            LOG.info("Using file cache")
            return FileCache(config.cache)
        elif config.cache.cache_type == UnitTestResultFetcherCacheType.GOOGLE_DRIVE:
            LOG.info("Using Google Drive cache")
            return GoogleDriveCache(config.cache)

    def run(self):
        LOG.info("Starting Jenkins test reporter. Details: %s", str(self.config))
        SimpleLoggingSetup.init_logger(
            project_name=CommandType.UNIT_TEST_RESULT_FETCHER.output_dir_name,
            logger_name_prefix=YARNDEVTOOLS_MODULE_NAME,
            execution_mode=ExecutionMode.PRODUCTION,
            console_debug=self.config.args.logging_debug,
            postfix=None,
            repos=None,
            verbose_git_log=self.config.args.verbose,
            enable_logging_setup_debug_details=False,
            with_trace_level=True,
        )
        if self.config.force_download_mode:
            LOG.info("FORCE DOWNLOAD MODE is on")

        self.job_results: JenkinsJobResults = self._database.load_job_results()
        if self.config.cache.enabled:
            self.cache.initialize()

        # TODO yarndevtoolsv2 self.job_results does not contain job_build_datas loaded from Google Drive
        if self.config.load_cached_reports_to_db:
            reports: Dict[str, FailedJenkinsBuild] = self.cache.download_reports()
            for file_path, failed_build in reports.items():
                # TODO yarndevtoolsv2 Implement force mode to always save everything to DB
                if not self._database.has_build_data(failed_build.url):
                    report_json = JsonFileUtils.load_data_from_json_file(file_path)
                    if not report_json:
                        LOG.error("Cannot load report as its JSON is empty. Job URL: %s", failed_build.url)
                        continue
                    build_data: JobBuildData = JenkinsApi.parse_job_data(report_json, failed_build)
                    self._database.save_build_data(build_data)

        for reset_job in self.config.reset_job_build_data_for_jobs:
            LOG.info("Reset job results for job: %s", reset_job)
            if reset_job in self.job_results:
                del self.job_results[reset_job]
                # TODO Remove from DB as well

        self.email.initialize(self.job_results)

        for job_name in self.config.job_names:
            job_result: JenkinsJobResult = self._create_jenkins_job_result(job_name)
            self.job_results[job_name] = job_result
            self.process_job_result(job_result)
        self._database.save_job_results(self.job_results)

    def _get_job_result_by_job_name(self, job_name) -> JenkinsJobResult:
        return self.job_results[job_name]

    def get_failed_tests(self, job_name) -> List[str]:
        result = self._get_job_result_by_job_name(job_name)
        if not result:
            raise ValueError("Job result is not queried yet or it is None!")
        return list(result.failure_count_by_testcase.keys())

    def get_num_build_data(self, job_name):
        return len(self._get_job_result_by_job_name(job_name)._builds_by_url)

    @property
    def testcase_filters(self) -> List[str]:
        return [tcf.as_filter_spec for tcf in self.config.tc_filters]

    def get_filtered_testcases_from_build(self, build_url: str, package: str, job_name: str):
        return [
            tc
            for filtered_res in self._get_job_result_by_job_name(job_name).get_job_data(build_url).filtered_testcases
            for tc in filtered_res.failed_testcases
            if package in tc
        ]

    def process_job_result(self, job_result: JenkinsJobResult):
        job_result.start_processing()
        for i, build_data in enumerate(job_result):
            self._process_build_data_from_job_result(build_data)
            self._print_job_result(build_data, job_result)
            self._invoke_job_result_processors(build_data, job_result)

    def _process_build_data_from_job_result(self, build_data: JobBuildData):
        LOG.info(f"Processing job result of build: {build_data.build_url}")
        should_exit: bool = False
        if build_data.status == JobBuildDataStatus.ALL_GREEN:
            LOG.error(
                "Job result with URL %s does exist but does not contain any failed tests as all testcases are green. ",
                build_data.build_url,
            )
            if self.config.fail_on_all_green_job_result:
                should_exit = True
        elif build_data.status == JobBuildDataStatus.EMPTY:
            LOG.info(
                "Job result with URL %s is valid but does not contain any testcase data, A.K.A. empty.",
                build_data.build_url,
            )
            if self.config.fail_on_empty_job_result:
                should_exit = True
        elif build_data.status in (JobBuildDataStatus.NO_JSON_DATA_FOUND, JobBuildDataStatus.CANNOT_FETCH):
            LOG.info("Job result with URL %s but couldn't fetch build or JSON data.", build_data.build_url)
            if self.config.fail_on_no_data_job_result:
                should_exit = True

        if should_exit:
            LOG.info("Will not process more job results, exiting...")
            raise SystemExit(0)

    def _print_job_result(self, build_data: JobBuildData, job_result: JenkinsJobResult):
        if build_data.is_valid:
            LOG.info("Job result of build %s contains failed tests!", build_data.build_url)
        else:
            LOG.info("Job result of build %s is not valid! Details: %s", build_data.build_url, build_data.status.value)
        if not self.config.omit_job_summary and build_data.is_valid:
            job_result.print(build_data)

    def _invoke_job_result_processors(self, build_data: JobBuildData, job_result: JenkinsJobResult):
        self.email.process(build_data, job_result)

    def fetch_and_parse_data(self, failed_build: FailedJenkinsBuild) -> JobBuildData:
        """Find the names of any tests which failed in the given build output URL."""
        try:
            data = self.fetch_raw_data_for_build(failed_build)
        except Exception:
            traceback.print_exc()
            LOG.error(
                "Could not fetch / load Jenkins test report, check %s for reason why it was reported as failed",
                failed_build.urls.job_console_output_url,
            )
            return JobBuildData(failed_build, None, set(), status=JobBuildDataStatus.CANNOT_FETCH)
        # TODO If data was loaded from cache and it is still None or len(data) == 0 (e.g. empty or corrupt file)
        #  script will think that report is empty. This case a file download is required.
        if not data or len(data) == 0:
            return JobBuildData(failed_build, None, [], status=JobBuildDataStatus.NO_JSON_DATA_FOUND)

        return JenkinsApi.parse_job_data(data, failed_build)

    def fetch_raw_data_for_build(self, failed_build: FailedJenkinsBuild):
        if self.config.cache.enabled:
            cache_build_key = self._convert_to_cache_build_key(failed_build)
            cache_hit = self.cache.is_build_data_in_cache(cache_build_key)
            if cache_hit:
                return self.cache.load_report(cache_build_key)
            else:
                return self._fetch_build_data(failed_build)
        else:
            return self._fetch_build_data(failed_build)

    def _fetch_build_data(self, failed_build):
        fmt_timestamp: str = DateUtils.format_unix_timestamp(failed_build.timestamp)
        LOG.debug(f"Downloading job data from URL: {failed_build.urls.test_report_url}, timestamp: ({fmt_timestamp})")
        data = JenkinsApi.download_job_result(failed_build, self.download_progress)
        self.download_progress.incr_sent_requests()
        if self.config.cache.enabled:
            self.cache.save_report(data, self._convert_to_cache_build_key(failed_build))
        return data

    def _create_jenkins_job_result(self, job_name: str) -> JenkinsJobResult:
        """Iterate runs of specified job within num_builds and collect results"""
        # TODO Discrepancy: request limit vs. days parameter
        jenkins_urls: JenkinsJobUrls = JenkinsJobUrls(self.config.jenkins_base_url, job_name)
        failed_builds, total_no_of_builds = JenkinsApi.list_builds_for_job(
            job_name, jenkins_urls, days=DEFAULT_REQUEST_LIMIT
        )
        # TODO This seems to be wrong, len(failed_builds) is not the same number of builds that should be downloaded
        #  as some of the builds can be cached. TODO: Take the cache into account
        self.download_progress = DownloadProgress(len(failed_builds), self.config.request_limit)
        job_result: JenkinsJobResult = JenkinsJobResult.create_empty(total_no_of_builds, self.config.num_builds)

        for failed_build in failed_builds:
            if not self.download_progress.check_limits():
                break

            download_build = False
            job_added_from_cache = False
            if self.config.cache.download_uncached_job_data and not self.cache.is_build_data_in_cache(
                self._convert_to_cache_build_key(failed_build)
            ):
                download_build = True

            # Try to get build data from cache, if found, jump to next build URL
            if self._should_load_build_data_from_cache(failed_build):
                LOG.info("Found build in cache, skipping download: %s", failed_build.url)
                # If job build data was intentionally reset by config option 'reset_job_build_data_for_jobs',
                # build data for job is already removed from the dict 'self.job_results'
                if job_name in self.job_results:
                    job_data = self.job_results.get_by_job_and_url(job_name, failed_build.url)
                    job_data.filter_testcases(self.config.tc_filters)
                    job_result.add_build(job_data)
                    job_added_from_cache = True

            # We would like to download job data if:
            # 1. job is not found in cache and config.download_uncached_job_data is True OR
            # 2. when job data is not found in file cache.
            if download_build or not job_added_from_cache:
                job_data = self.fetch_and_parse_data(failed_build)
                if not job_added_from_cache:
                    job_data.filter_testcases(self.config.tc_filters)
                    job_result.add_build(job_data)
                self.download_progress.process_next_build()
        job_result.finalize()

        return job_result

    def _should_load_build_data_from_cache(self, failed_build: FailedJenkinsBuild):
        return (
            not self.config.force_download_mode
            and failed_build.job_name in self.job_results
            and failed_build.url in self.job_results[failed_build.job_name].known_build_urls
        )
