#!/usr/local/bin/python3
import logging
import sys
import time
import traceback
from typing import List, Dict, Tuple, Any

from pythoncommons.constants import ExecutionMode
from pythoncommons.date_utils import DateUtils
from pythoncommons.file_utils import JsonFileUtils
from pythoncommons.logging_setup import SimpleLoggingSetup
from pythoncommons.network_utils import NetworkUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils
from pythoncommons.string_utils import auto_str

from yarndevtools.commands.unittestresultfetcher.cache import (
    Cache,
    GoogleDriveCache,
    FileCache,
    UnitTestResultFetcherCacheType,
    CacheConfig,
)
from yarndevtools.commands.unittestresultfetcher.common import UnitTestResultFetcherMode, FileNameUtils
from yarndevtools.commands.unittestresultfetcher.db import JenkinsJobReports, UTResultFetcherDatabase
from yarndevtools.commands.unittestresultfetcher.email import Email, EmailConfig
from yarndevtools.commands.unittestresultfetcher.model import JenkinsJobReport, CachedBuildKey
from yarndevtools.commands.unittestresultfetcher.parser import UnitTestResultFetcherParser
from yarndevtools.commands_common import CommandAbs
from yarndevtools.common.common_model import (
    JobBuildData,
    FailedJenkinsBuild,
    JobBuildDataStatus,
    JenkinsTestcaseFilter,
    JobBuildDataCounters,
)
from yarndevtools.common.db import MongoDbConfig
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME

LOG = logging.getLogger(__name__)
SECONDS_PER_DAY = 86400
DEFAULT_REQUEST_LIMIT = 999
JENKINS_BUILDS_EXAMINE_UNLIMITIED_VAL = "jenkins_examine_unlimited_builds"


@auto_str
class DownloadProgress:
    # TODO Store awaiting download / awaiting cache load separately
    # TODO Decide on startup: What build need to be downloaded, what is in the cache, etc.
    def __init__(self, number_of_failed_builds):
        self.all_builds: int = number_of_failed_builds
        self.current_build_idx = 0

    def process_next_build(self):
        self.current_build_idx += 1

    def short_str(self):
        return f"{self.current_build_idx + 1}/{self.all_builds}"


class JenkinsJobUrls:
    def __init__(self, jenkins_base_url, job_name):
        self.jenkins_base_url = jenkins_base_url
        self.list_builds = self._get_jenkins_list_builds_url(job_name)

    def _get_jenkins_list_builds_url(self, job_name: str) -> str:
        jenkins_url = self.jenkins_base_url
        if jenkins_url.endswith("/"):
            jenkins_url = jenkins_url[:-1]
        return f"{jenkins_url}/job/{job_name}/api/json?tree=builds[url,result,timestamp]"


class JenkinsApiConverter:
    @staticmethod
    def convert(job_name: str, jenkins_urls: JenkinsJobUrls, days: int):
        all_builds: List[Dict[str, str]] = JenkinsApiConverter._list_builds(jenkins_urls)
        last_n_builds: List[Dict[str, str]] = JenkinsApiConverter._filter_builds_last_n_days(all_builds, days=days)
        last_n_failed_build_tuples: List[Tuple[str, int]] = JenkinsApiConverter._get_failed_build_urls_with_timestamps(
            last_n_builds
        )
        failed_build_data: List[Tuple[str, int]] = sorted(
            last_n_failed_build_tuples, key=lambda tup: tup[1], reverse=True
        )
        failed_builds = [
            FailedJenkinsBuild(
                full_url_of_job=tup[0],
                timestamp=JenkinsApiConverter._convert_to_unix_timestamp(tup[1]),
                job_name=job_name,
            )
            for tup in failed_build_data
        ]

        total_no_of_builds = len(all_builds)
        LOG.info(
            f"There are {len(failed_build_data)} builds "
            f"(out of {total_no_of_builds}) that have failed tests "
            f"in the past {days} days. "
            f"Listing builds: {failed_build_data}"
        )
        failed_urls = [fb.url for fb in failed_builds]
        LOG.debug("Detected failed builds for Jenkins job '%s': %s", job_name, failed_urls)
        return failed_builds, total_no_of_builds

    @staticmethod
    def _list_builds(urls: JenkinsJobUrls) -> List[Any]:
        """List all builds of the target project."""
        url = urls.list_builds
        try:
            LOG.info("Fetching builds from Jenkins in url: %s", url)
            data = JenkinsApiConverter.safe_fetch_json(url)
            # In case job does not exist (HTTP 404), data will be None
            if data:
                return data["builds"]
            return []
        except Exception:
            LOG.error(f"Could not fetch: {url}")
            raise

    @staticmethod
    def _filter_builds_last_n_days(builds, days):
        # Select only those in the last N days
        min_time = int(time.time()) - SECONDS_PER_DAY * days
        return [b for b in builds if (JenkinsApiConverter._convert_to_unix_timestamp_from_json(b)) > min_time]

    @staticmethod
    def _get_failed_build_urls_with_timestamps(builds):
        return [(b["url"], b["timestamp"]) for b in builds if (b["result"] in ("UNSTABLE", "FAILURE"))]

    @staticmethod
    def _convert_to_unix_timestamp_from_json(build_json):
        timestamp_str = build_json["timestamp"]
        return JenkinsApiConverter._convert_to_unix_timestamp(int(timestamp_str))

    @staticmethod
    def _convert_to_unix_timestamp(ts: int):
        # Jenkins' uses milliseconds format to store the timestamp, divide it by 1000
        # See: https://stackoverflow.com/a/24308978/1106893
        return int(ts / 1000)

    @staticmethod
    def parse_job_data(json_data, failed_build: FailedJenkinsBuild) -> JobBuildData:
        failed_testcases = set()
        found_testcases: int = 0
        for suite in json_data["suites"]:
            for tc in suite["cases"]:
                found_testcases += 1
                status = tc["status"]
                err_details = tc["errorDetails"]
                if status == "REGRESSION" or status == "FAILED" or (err_details is not None):
                    failed_testcases.add(f"{tc['className']}.{tc['name']}")
        if len(failed_testcases) == 0:
            if found_testcases:
                LOG.info(
                    f"No failed tests in test report, check {failed_build.urls.job_console_output_url} for why it was reported failed."
                )
                return JobBuildData(failed_build, None, failed_testcases, status=JobBuildDataStatus.ALL_GREEN)
            else:
                return JobBuildData(failed_build, None, failed_testcases, status=JobBuildDataStatus.EMPTY)
        else:
            counters = JobBuildDataCounters(json_data["failCount"], json_data["passCount"], json_data["skipCount"])
            return JobBuildData(
                failed_build, counters, failed_testcases, status=JobBuildDataStatus.HAVE_FAILED_TESTCASES
            )

    @staticmethod
    def download_test_report(failed_build: FailedJenkinsBuild, download_progress: DownloadProgress):
        url = failed_build.urls.test_report_api_json_url
        LOG.info(f"Loading test report from URL: {url}. Download progress: {download_progress.short_str()}")
        return JenkinsApiConverter.safe_fetch_json(url)

    @staticmethod
    def safe_fetch_json(url):
        def retry_fetch(url):
            LOG.error("URL '%s' cannot be fetched (HTTP 502 Proxy Error):", url)
            JenkinsApiConverter.safe_fetch_json(url)

        # HTTP 404 should be logged
        # HTTP Error 502: Proxy Error is just calls this function again (retry) with the same args, indefinitely
        data = NetworkUtils.fetch_json(
            url,
            do_not_raise_http_statuses={404, 502},
            http_callbacks={
                404: lambda: LOG.error("URL '%s' cannot be fetched (HTTP 404):", url),
                502: lambda: retry_fetch(url),
            },
        )
        return data


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
        self.fail_on_all_green_report: bool = False  # TODO hardcoded
        self.fail_on_empty_report: bool = False  # TODO hardcoded
        self.fail_reports_with_no_data: bool = False  # TODO hardcoded
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
        self.reports: JenkinsJobReports = None
        self.cache: Cache = self._create_cache(self.config)
        self.email: Email = Email(self.config.email)
        self.sent_requests: int = 0
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
            LOG.info("Using file cache.")
            return FileCache(config.cache)
        elif config.cache.cache_type == UnitTestResultFetcherCacheType.GOOGLE_DRIVE:
            LOG.info("Using Google Drive cache.")
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

        self.reports: JenkinsJobReports = self._database.load_reports()
        if self.config.cache.enabled:
            self.cache.initialize()
        if self.config.load_cached_reports_to_db:
            reports: Dict[str, FailedJenkinsBuild] = self.cache.download_reports()
            # TODO yarndevtoolsv2 Implement force mode to always save everything to DB
            for file_path, failed_build in reports.items():
                if not self._database.has_build_data(failed_build.url):
                    report_json = JsonFileUtils.load_data_from_json_file(file_path)
                    if not report_json:
                        LOG.error("Cannot load report as its JSON is empty. Job URL: %s", failed_build.url)
                        continue
                    build_data = JenkinsApiConverter.parse_job_data(report_json, failed_build)
                    self._database.save_build_data(build_data)

        for reset_job in self.config.reset_job_build_data_for_jobs:
            LOG.info("Reset job build data for job: %s", reset_job)
            if reset_job in self.reports:
                del self.reports[reset_job]

        self.email.initialize(self.reports)

        self.sent_requests = 0
        for job_name in self.config.job_names:
            report: JenkinsJobReport = self._create_jenkins_report(job_name)
            # TODO yarndevtoolsv2 self.reports does not contain job_build_datas loaded from Google Drive
            self.reports[job_name] = report
            self.process_jenkins_report(report)
        self._database.save_reports(self.reports)

    def _get_report_by_job_name(self, job_name) -> JenkinsJobReport:
        return self.reports[job_name]

    def get_failed_tests(self, job_name) -> List[str]:
        report = self._get_report_by_job_name(job_name)
        if not report:
            raise ValueError("Report is not queried yet or it is None!")
        return list(report.all_failing_tests.keys())

    def get_num_build_data(self, job_name):
        return len(self._get_report_by_job_name(job_name)._jobs_by_url)

    @property
    def testcase_filters(self) -> List[str]:
        return [tcf.as_filter_spec for tcf in self.config.tc_filters]

    def get_filtered_testcases_from_build(self, build_url: str, package: str, job_name: str):
        return [
            tc
            for filtered_res in self._get_report_by_job_name(job_name).get_job_data(build_url).filtered_testcases
            for tc in filtered_res.testcases
            if package in tc
        ]

    def process_jenkins_report(self, report: JenkinsJobReport):
        report.start_processing()
        for i, build_data in enumerate(report):
            self._process_build_data_from_report(build_data)
            self._print_report(build_data, report)
            self._invoke_report_processors(build_data, report)

            key = list(report._jobs_by_url.keys())[0]
            build_data = report._jobs_by_url[key]
            self._database.save_build_data(build_data)

    def _process_build_data_from_report(self, build_data: JobBuildData):
        LOG.info(f"Processing report of build: {build_data.build_url}")
        should_exit: bool = False
        if build_data.status == JobBuildDataStatus.ALL_GREEN:
            LOG.error(
                "Report with URL %s does exist but does not contain any failed tests as all testcases are green. ",
                build_data.build_url,
            )
            if self.config.fail_on_all_green_report:
                should_exit = True
        elif build_data.status == JobBuildDataStatus.EMPTY:
            LOG.info(
                "Report with URL %s is valid but does not contain any testcase data, A.K.A. empty.",
                build_data.build_url,
            )
            if self.config.fail_on_empty_report:
                should_exit = True
        elif build_data.status in (JobBuildDataStatus.NO_JSON_DATA_FOUND, JobBuildDataStatus.CANNOT_FETCH):
            LOG.info("Report with URL %s but couldn't fetch build or JSON data.", build_data.build_url)
            if self.config.fail_reports_with_no_data:
                should_exit = True

        if should_exit:
            LOG.info("Will not process more reports, exiting...")
            raise SystemExit(0)

    def _print_report(self, build_data: JobBuildData, report: JenkinsJobReport):
        if build_data.is_valid:
            LOG.info("Report of build %s contains failed tests!", build_data.build_url)
        else:
            LOG.info("Report of build %s is not valid! Details: %s", build_data.build_url, build_data.status.value)
        if not self.config.omit_job_summary and build_data.is_valid:
            report.print_report(build_data)

    def _invoke_report_processors(self, build_data: JobBuildData, report: JenkinsJobReport):
        self.email.process(build_data, report)

    def create_job_build_data(self, failed_build: FailedJenkinsBuild) -> JobBuildData:
        """Find the names of any tests which failed in the given build output URL."""
        try:
            data = self.gather_raw_data_for_build(failed_build)
        except Exception:
            traceback.print_exc()
            LOG.error(
                "Could not open test report, check %s for reason why it was reported failed",
                failed_build.urls.job_console_output_url,
            )
            return JobBuildData(failed_build, None, set(), status=JobBuildDataStatus.CANNOT_FETCH)
        # TODO If data was loaded from cache and it is still None or len(data) == 0 (e.g. empty or corrupt file)
        #  script will think that report is empty. This case a file download is required.
        if not data or len(data) == 0:
            return JobBuildData(failed_build, None, [], status=JobBuildDataStatus.NO_JSON_DATA_FOUND)

        return JenkinsApiConverter.parse_job_data(data, failed_build)

    def gather_raw_data_for_build(self, failed_build: FailedJenkinsBuild):
        if self.config.cache.enabled:
            cache_build_key = self._convert_to_cache_build_key(failed_build)
            cache_hit = self.cache.is_build_data_in_cache(cache_build_key)
            if cache_hit:
                return self.cache.load_report(cache_build_key)
            else:
                return self._download_build_data(failed_build)
        else:
            return self._download_build_data(failed_build)

    def _download_build_data(self, failed_build):
        fmt_timestamp: str = DateUtils.format_unix_timestamp(failed_build.timestamp)
        LOG.debug(f"Downloading job data from URL: {failed_build.urls.test_report_url}, timestamp: ({fmt_timestamp})")
        data = JenkinsApiConverter.download_test_report(failed_build, self.download_progress)
        self.sent_requests += 1
        if self.config.cache.enabled:
            self.cache.save_report(data, self._convert_to_cache_build_key(failed_build))
        return data

    def _create_jenkins_report(self, job_name: str) -> JenkinsJobReport:
        """Iterate runs of specified job within num_builds and collect results"""
        # TODO Discrepancy: request limit vs. days parameter
        jenkins_urls: JenkinsJobUrls = JenkinsJobUrls(self.config.jenkins_base_url, job_name)
        self.failed_builds, self.total_no_of_builds = JenkinsApiConverter.convert(
            job_name, jenkins_urls, days=DEFAULT_REQUEST_LIMIT
        )
        job_datas: List[JobBuildData] = []
        tc_to_fail_count: Dict[str, int] = {}
        # TODO This seems to be wrong, len(failed_builds) is not the same number of builds that should be downloaded
        #  as some of the builds can be cached. TODO: Take the cache into account
        self.download_progress = DownloadProgress(len(self.failed_builds))
        for failed_build in self.failed_builds:
            if self.sent_requests >= self.config.request_limit:
                LOG.error(f"Reached request limit: {self.sent_requests}")
                break

            download_build = False
            job_added_from_cache = False
            if self.config.cache.download_uncached_job_data and not self.cache.is_build_data_in_cache(
                self._convert_to_cache_build_key(failed_build)
            ):
                download_build = True

            # Try to get build data from cache, if found, jump to next build URL
            if self._should_load_build_data_from_cache(failed_build):
                LOG.info("Found build in cache, skipping: %s", failed_build.url)
                # If job build data was intentionally reset by config option 'reset_job_build_data_for_jobs',
                # build data for job is already removed from the dict 'self.reports'
                if job_name in self.reports:
                    job_data = self.reports[job_name]._jobs_by_url[failed_build.url]
                    job_datas.append(job_data)
                    self._create_testcase_to_fail_count_dict(job_data, tc_to_fail_count)
                    job_added_from_cache = True

            # We would like to download job data if:
            # 1. job is not found in cache and config.download_uncached_job_data is True OR
            # 2. when job data is not found in file cache.
            if download_build or not job_added_from_cache:
                job_data = self.create_job_build_data(failed_build)
                if not job_added_from_cache:
                    job_data.filter_testcases(self.config.tc_filters)
                    job_datas.append(job_data)
                    self._create_testcase_to_fail_count_dict(job_data, tc_to_fail_count)
                self.download_progress.process_next_build()

        return JenkinsJobReport(job_datas, tc_to_fail_count, self.total_no_of_builds, self.config.num_builds)

    def _should_load_build_data_from_cache(self, failed_build: FailedJenkinsBuild):
        return (
            not self.config.force_download_mode
            and failed_build.job_name in self.reports
            and failed_build.url in self.reports[failed_build.job_name].known_build_urls
        )

    @staticmethod
    def _create_testcase_to_fail_count_dict(job_data, tc_to_fail_count: Dict[str, int]):
        if job_data.has_failed_testcases():
            for failed_testcase in job_data.testcases:
                LOG.debug(f"Detected failed testcase: {failed_testcase}")
                tc_to_fail_count[failed_testcase] = tc_to_fail_count.get(failed_testcase, 0) + 1
