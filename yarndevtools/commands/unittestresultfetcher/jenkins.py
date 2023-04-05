import json
import time
from typing import List, Dict, Tuple, Any, Callable, Set
import urllib
import urllib.request
import urllib.error as url_error
from pythoncommons.network_utils import NetworkUtils
from pythoncommons.string_utils import auto_str

from yarndevtools.commands.unittestresultfetcher.common import JobNameUtils, UnitTestResultFetcherMode
from yarndevtools.common.common_model import FailedJenkinsBuild, JobBuildData, JobBuildDataStatus, JobBuildDataCounters

import logging

LOG = logging.getLogger(__name__)
SECONDS_PER_DAY = 86400


@auto_str
class DownloadProgress:
    # TODO Store awaiting download / awaiting cache load separately
    # TODO Decide on startup: What build need to be downloaded, what is in the cache, etc.
    def __init__(self, number_of_failed_builds, request_limit):
        self.all_builds: int = number_of_failed_builds
        self.current_build_idx = 0
        self.performed_requests = 0
        self._request_limit = request_limit

    def process_next_build(self):
        self.current_build_idx += 1

    def incr_performed_requests(self):
        self.performed_requests += 1

    def check_limits(self):
        if self.performed_requests >= self._request_limit:
            LOG.error(f"Reached request limit: {self.performed_requests}")
            return False
        return True

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


class JenkinsApi:
    def __init__(self, user, password):
        self.user = user
        self.password = password

    def list_builds_for_job(
        self, job_name: str, jenkins_urls: JenkinsJobUrls, days: int
    ) -> Tuple[List[FailedJenkinsBuild], int]:
        all_builds: List[Dict[str, str]] = self._list_builds(jenkins_urls)
        last_n_builds: List[Dict[str, str]] = JenkinsApi._filter_builds_last_n_days(all_builds, days=days)
        last_n_failed_build_tuples: List[Tuple[str, int]] = JenkinsApi._get_failed_build_urls_with_timestamps(
            last_n_builds
        )
        failed_build_data: List[Tuple[str, int]] = sorted(
            last_n_failed_build_tuples, key=lambda tup: tup[1], reverse=True
        )
        failed_builds = [
            FailedJenkinsBuild(
                full_url_of_job=tup[0],
                timestamp=JenkinsApi._convert_to_unix_timestamp(tup[1]),
                job_name=JobNameUtils.escape_job_name(job_name),
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

    def _list_builds(self, urls: JenkinsJobUrls) -> List[Any]:
        """List all builds of the target project."""
        url = urls.list_builds
        try:
            LOG.info("Fetching builds from Jenkins in url: %s", url)
            data = self.safe_fetch_json(url)
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
        return [b for b in builds if (JenkinsApi._convert_to_unix_timestamp_from_json(b)) > min_time]

    @staticmethod
    def _get_failed_build_urls_with_timestamps(builds):
        return [(b["url"], b["timestamp"]) for b in builds if (b["result"] in ("UNSTABLE", "FAILURE"))]

    @staticmethod
    def _convert_to_unix_timestamp_from_json(build_json):
        timestamp_str = build_json["timestamp"]
        return JenkinsApi._convert_to_unix_timestamp(int(timestamp_str))

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

    def download_job_result(self, failed_build: FailedJenkinsBuild, download_progress: DownloadProgress):
        url = failed_build.urls.test_report_api_json_url
        LOG.info(f"Loading job result from URL: {url}. Download progress: {download_progress.short_str()}")
        return self.safe_fetch_json(url)

    def safe_fetch_json(self, url):
        def retry_fetch(url):
            LOG.error("URL '%s' cannot be fetched (HTTP 502 Proxy Error):", url)
            self.safe_fetch_json(url)

        # HTTP 404 should be logged
        # HTTP Error 502: Proxy Error is just calls this function again (retry) with the same args, indefinitely
        data = self.fetch_json(
            url,
            self.user,
            self.password,
            do_not_raise_http_statuses={404, 502},
            http_callbacks={
                404: lambda: LOG.error("URL '%s' cannot be fetched (HTTP 404):", url),
                502: lambda: retry_fetch(url),
            },
        )
        return data

    # TODO Could be moved to network_utils (pythoncommons)
    @staticmethod
    def fetch_json(
        url,
        user,
        password,
        strict=False,
        do_not_raise_http_statuses: Set[int] = None,
        http_callbacks: Dict[int, Callable] = None,
    ):
        """Load data from specified url"""
        try:
            LOG.debug("Making request to URL: %s", url)
            auth_handler = urllib.request.HTTPBasicAuthHandler(
                password_mgr=urllib.request.HTTPPasswordMgrWithPriorAuth()
            )
            auth_handler.add_password(
                realm="test",
                uri=UnitTestResultFetcherMode.JENKINS_MASTER.jenkins_base_url,
                user=user,
                passwd=password,
                is_authenticated=True,
            )
            opener = urllib.request.build_opener(auth_handler)
            ourl = opener.open(url)
            codec = ourl.info().get_param("charset")
            content = ourl.read().decode(codec)
        except urllib.error.HTTPError as e:
            if do_not_raise_http_statuses and e.code in do_not_raise_http_statuses:
                if http_callbacks and e.code in http_callbacks:
                    http_callbacks[e.code]()
                return {}
            else:
                raise e
        return json.loads(content, strict=strict)
