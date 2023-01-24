import logging
import os
import re
import tempfile
from abc import abstractmethod, ABC
from collections import defaultdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, List, Dict, Set

from googleapiwrapper.common import ServiceType
from googleapiwrapper.google_auth import GoogleApiAuthorizer
from googleapiwrapper.google_drive import (
    FileFindMode,
    DriveApiWrapper,
    DriveApiWrapperSingleOperationSettings,
    DuplicateFileWriteResolutionMode,
    SearchResultHandlingMode,
    DriveApiWrapperSessionSettings,
    DriveApiScope,
    DriveApiFile,
)
from pythoncommons.date_utils import DateUtils, DATEFORMAT_GOOGLE_DRIVE
from pythoncommons.file_utils import FileUtils, JsonFileUtils, FindResultType
from pythoncommons.project_utils import PROJECTS_BASEDIR_NAME
from pythoncommons.string_utils import StringUtils
from pythoncommons.url_utils import UrlUtils

from yarndevtools.cdsw.constants import SECRET_PROJECTS_DIR
from yarndevtools.commands.unittestresultfetcher.common import (
    UnitTestResultFetcherMode,
    CACHED_DATA_DIRNAME,
    JobNameUtils,
)
from yarndevtools.commands.unittestresultfetcher.model import CachedBuild, JenkinsJobResult, CachedBuildKey
from yarndevtools.common.common_model import FailedJenkinsBuild
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME

LOG = logging.getLogger(__name__)


class UnitTestResultFetcherCacheType(Enum):
    FILE = "FILE"
    GOOGLE_DRIVE = "GOOGLE_DRIVE"


class Cache(ABC):
    @abstractmethod
    def initialize(self) -> Dict[str, JenkinsJobResult]:
        pass

    @abstractmethod
    def is_build_data_in_cache(self, key: CachedBuildKey):
        pass

    @abstractmethod
    def save_report(self, data, key: CachedBuildKey):
        pass

    @abstractmethod
    def load_report(self, key: CachedBuildKey) -> Dict[Any, Any]:
        pass

    @abstractmethod
    def remove_report(self, key: CachedBuildKey):
        pass

    @staticmethod
    def generate_job_dirname(key: CachedBuildKey):
        return JobNameUtils.escape_job_name(key.job_name)

    @staticmethod
    def generate_report_filename(key: CachedBuildKey):
        return f"{key.build_number}-testreport.json"

    @abstractmethod
    def get_all_reports(self) -> List[Any]:
        pass

    @abstractmethod
    def download_reports(self):
        pass


class FileCache(Cache):
    def __init__(self, config):
        self.config: CacheConfig = config

    def get_all_reports(self) -> Dict[CachedBuildKey, CachedBuild]:
        """
        Returns all report file paths
        :return:
        """
        self._reload_all_cached_builds()
        return self.cached_builds

    def download_reports(self):
        # TODO yarndevtoolsv2
        pass

    def initialize(self):
        self._reload_all_cached_builds()

    def _reload_all_cached_builds(self):
        report_files = FileUtils.find_files(
            self.config.reports_dir,
            find_type=FindResultType.FILES,
            single_level=False,
            full_path_result=True,
            extension="json",
        )
        self.cached_builds: Dict[CachedBuildKey, CachedBuild] = self._load_cached_builds_from_fs(report_files)
        LOG.info("Loaded cached builds: %s", self.cached_builds)

    def _load_cached_builds_from_fs(self, report_files):
        cached_builds: Dict[CachedBuildKey, CachedBuild] = {}
        for report_file in report_files:
            orig_file_path = report_file
            # Example file name: CDH-7_1-maint-Hadoop-Common-Unit/1-testreport.json
            if report_file.startswith(self.config.reports_dir):
                report_file = report_file[len(self.config.reports_dir) :]
            comps = report_file.split(os.sep)
            comps = [c for c in comps if c]
            job_name = comps[0]
            report_filename = comps[1]
            # TODO yarndevtoolsv2 regex matching here!! ++ Parsing logic duplicated, find: GoogleDriveCache.TEST_REPORT_PATTERN
            build_number = int(report_filename.split("-")[0])
            key = CachedBuildKey(job_name, build_number)
            cached_builds[key] = CachedBuild(key, orig_file_path)
        return cached_builds

    def _generate_file_name_for_report(self, key: CachedBuildKey):
        job_dir_path = FileUtils.join_path(self.config.reports_dir, self.generate_job_dirname(key))
        job_dir_path = FileUtils.ensure_dir_created(job_dir_path)
        return FileUtils.join_path(job_dir_path, self.generate_report_filename(key))

    def is_build_data_in_cache(self, key: CachedBuildKey):
        if key in self.cached_builds:
            LOG.debug(
                "Build found in cache. Job name: %s, Build number: %s",
                key.job_name,
                key.build_number,
            )
            return True
        return False

    def save_report(self, data, key: CachedBuildKey):
        report_file_path = self._generate_file_name_for_report(key)
        LOG.info(f"Saving test report response JSON to file cache: {report_file_path}")
        JsonFileUtils.write_data_to_file_as_json(report_file_path, data)
        return report_file_path

    def load_report(self, key: CachedBuildKey) -> Dict[Any, Any]:
        report_file_path = self._generate_file_name_for_report(key)
        LOG.info(f"Loading cached test report from file: {report_file_path}")
        return JsonFileUtils.load_data_from_json_file(report_file_path)

    def remove_report(self, key: CachedBuildKey):
        report_file_path = self._generate_file_name_for_report(key)
        LOG.info(f"Removing test report from file cache: {report_file_path}")
        FileUtils.remove_file(report_file_path)

    def get_filename_for_report(self, key: CachedBuildKey):
        return self._generate_file_name_for_report(key)


class GoogleDriveCache(Cache):
    DRIVE_FINAL_CACHE_DIR = CommandType.UNIT_TEST_RESULT_FETCHER.output_dir_name + "_" + CACHED_DATA_DIRNAME
    TEST_REPORT_REGEX = "^[0-9]+-testreport.json$"
    TEST_REPORT_PATTERN = re.compile(TEST_REPORT_REGEX)
    # TODO implement throttling: Too many requests to Google Drive?

    def __init__(self, config):
        self.config: CacheConfig = config
        self.file_cache: FileCache = FileCache(config)
        self.authorizer = GoogleApiAuthorizer(
            ServiceType.DRIVE,
            project_name=CommandType.UNIT_TEST_RESULT_FETCHER.output_dir_name,
            secret_basedir=SECRET_PROJECTS_DIR,
            account_email="snemeth@cloudera.com",
            scopes=[DriveApiScope.DRIVE_PER_FILE_ACCESS.value],
        )
        session_settings = DriveApiWrapperSessionSettings(
            FileFindMode.JUST_UNTRASHED, DuplicateFileWriteResolutionMode.ADD_NEW_REVISION, enable_path_cache=True
        )
        self.drive_wrapper = DriveApiWrapper(self.authorizer, session_settings=session_settings)
        self.drive_reports_basedir = FileUtils.join_path(
            PROJECTS_BASEDIR_NAME, YARNDEVTOOLS_MODULE_NAME, self.DRIVE_FINAL_CACHE_DIR, "reports"
        )
        self.all_report_files = []
        self.downloader = GoogleFileDownloader(self.drive_wrapper, self.file_cache)

    def initialize(self):
        reports = self.file_cache.initialize()
        self.all_report_files = self._download_all_reports()
        if self.config.enable_sync_from_fs_to_drive:
            self._sync_from_file_cache()
        return reports

    @staticmethod
    def create_cached_build_key(drive_file) -> CachedBuildKey:
        job_name = drive_file._parent.name
        components = drive_file.name.split("-")
        if len(components) != 2:
            LOG.error("Found test report with unexpected name: %s", job_name)
            return None
        return CachedBuildKey(job_name, int(components[0]))

    def _sync_from_file_cache(self):
        found_builds: Set[CachedBuildKey] = set()
        for report_drive_file in self.all_report_files:
            build_key = self.create_cached_build_key(report_drive_file)
            if build_key:
                found_builds.add(build_key)
        LOG.debug("Found %d builds from Google Drive: %s", len(found_builds), found_builds)
        builds_to_check_from_drive = {
            key: value for (key, value) in self.file_cache.cached_builds.items() if key not in found_builds
        }
        LOG.debug("Will check these builds in Google Drive: %s", builds_to_check_from_drive)

        # TODO yarndevtoolsv2 Implement sync from GDrive -> Filesystem (other way around)
        # TODO Create progressTracker object to show current status of Google Drive uploads / queries
        for key, cached_build in builds_to_check_from_drive.items():
            drive_report_file_path = self._generate_file_name_for_report(key)
            settings: DriveApiWrapperSingleOperationSettings = DriveApiWrapperSingleOperationSettings(
                file_find_mode=None,
                duplicate_file_handling_mode=DuplicateFileWriteResolutionMode.FAIL_FAST,
                search_result_handling_mode=SearchResultHandlingMode.SINGLE_FILE_PER_SEARCH_RESULT,
            )
            exist = self.drive_wrapper.does_file_exist(drive_report_file_path, op_settings=settings)
            if not exist:
                settings: DriveApiWrapperSingleOperationSettings = DriveApiWrapperSingleOperationSettings(
                    file_find_mode=None, duplicate_file_handling_mode=DuplicateFileWriteResolutionMode.FAIL_FAST
                )
                self.drive_wrapper.upload_file(
                    cached_build.full_report_file_path, drive_report_file_path, op_settings=settings
                )

    def get_all_reports(self):
        if not self.all_report_files:
            raise ValueError("Please call initialize on the cache, first!")
        return self.all_report_files

    def _download_all_reports(self):
        all_report_files: List[DriveApiFile] = self.drive_wrapper.get_files("*-testreport.json")
        return all_report_files

    def download_reports(self) -> Dict[str, FailedJenkinsBuild]:
        drive_api_files = self.get_all_reports()
        return self.downloader.download_reports(drive_api_files)

    def remove_report(self, key: CachedBuildKey):
        raise NotImplementedError("Remove report is not supported by GoogleDriveCache")

    @staticmethod
    def determine_creation_date(drive_api_file, file):
        # The only way to tell the timestamp of the build that is in file cache is to use creation date of the file
        creation_date_drive_file = DateUtils.convert_to_datetime(drive_api_file.created_date, DATEFORMAT_GOOGLE_DRIVE)
        creation_seconds = FileUtils.get_creation_time(file)
        creation_date_file = datetime.fromtimestamp(creation_seconds, tz=timezone.utc)
        if creation_date_drive_file < creation_date_file:
            return creation_date_drive_file
        return creation_date_file

    @staticmethod
    def get_build_number(filename):
        if not GoogleDriveCache.TEST_REPORT_PATTERN.match(filename):
            raise ValueError(
                "Expected report file name to be in the following format: {} but actual value was: {}".format(
                    GoogleDriveCache.TEST_REPORT_REGEX, filename
                )
            )

        return int(filename.split("-")[0])

    @staticmethod
    def create_failed_build(filename, creation_date, key: CachedBuildKey):
        build_number = GoogleDriveCache.get_build_number(filename)
        timestamp = creation_date.timestamp()
        fetcher_mode = UnitTestResultFetcherMode.get_mode_by_job_name(key.job_name)
        build_url = f"{fetcher_mode.jenkins_base_url}/job/{key.job_name}/{build_number}"
        return FailedJenkinsBuild(
            full_url_of_job=UrlUtils.sanitize_url(build_url),
            timestamp=timestamp,
            job_name=key.job_name,
        )

    def _generate_file_name_for_report(self, key: CachedBuildKey):
        return FileUtils.join_path(
            self.drive_reports_basedir,
            self.generate_job_dirname(key),
            self.generate_report_filename(key),
        )

    def is_build_data_in_cache(self, key: CachedBuildKey):
        # TODO yarndevtoolsv2 Check in Drive and if not successful, decide based on local file cache
        return self.file_cache.is_build_data_in_cache(key)

    def save_report(self, data, key: CachedBuildKey):
        saved_report_file_path = self.file_cache.save_report(data, key)
        drive_path = self._generate_file_name_for_report(key)
        self.drive_wrapper.upload_file(saved_report_file_path, drive_path)

    def load_report(self, key: CachedBuildKey) -> Dict[Any, Any]:
        cache_hit = self.file_cache.is_build_data_in_cache(key)
        if cache_hit:
            return self.file_cache.load_report(key)
        else:
            filename = self._generate_file_name_for_report(key)
            self.drive_wrapper.get_file(filename)
            # TODO missing return
        # TODO yarndevtoolsv2 Load from Drive and if not successful, load from local file cache
        # TODO If report.json is only found in local cache, save it to Drive


class CacheConfig:
    def __init__(self, args, output_dir, force_download_mode=False, load_cached_reports_to_db=False):
        self.cache_type: UnitTestResultFetcherCacheType = (
            UnitTestResultFetcherCacheType(args.cache_type.upper())
            if hasattr(args, "cache_type") and args.cache_type
            else UnitTestResultFetcherCacheType.FILE
        )
        self._explicitly_disable_file_cache = args.disable_file_cache if hasattr(args, "disable_file_cache") else False
        self.enabled = not self._explicitly_disable_file_cache

        self.enable_sync_from_fs_to_drive: bool = (
            not args.disable_sync_from_fs_to_drive if hasattr(args, "disable_sync_from_fs_to_drive") else True
        )

        self.enabled = self._verify_cache_enabled(force_download_mode, load_cached_reports_to_db)

        self.reports_dir = FileUtils.ensure_dir_created(FileUtils.join_path(output_dir, "reports"))
        self.cached_data_dir = FileUtils.ensure_dir_created(FileUtils.join_path(output_dir, CACHED_DATA_DIRNAME))
        self.download_uncached_job_data: bool = (
            args.download_uncached_job_data if hasattr(args, "download_uncached_job_data") else False
        )

    def _verify_cache_enabled(self, force_download_mode, load_cached_reports_to_db):
        orig_val = self.enabled

        if force_download_mode:
            reason = "force download mode is enabled"
            new_val = True
        if self.cache_type:
            reason = "cache type is set to: " + str(self.cache_type)
            # TODO do not change because of cache type
            # new_val = True
            new_val = orig_val
        if load_cached_reports_to_db:
            reason = "load cached reports to db is enabled"
            new_val = True
            self.cache_type = UnitTestResultFetcherCacheType.GOOGLE_DRIVE

        if orig_val != new_val:
            raise ValueError(
                "Conflicting cache settings! Original enabled value: {}, new enabled value: {}, reason: {}".format(
                    orig_val, new_val, reason
                )
            )

        return new_val


class FileSizeCheckerResult(Enum):
    NORMAL_SIZE = "normal"
    SMALL_SIZE_REDOWNLOAD = "small_size_redownloaded"
    SMALL_SIZE_AFTER_REDOWNLOAD = "small_size_cannot_download"


class GoogleFileDownloader:
    def __init__(self, drive_wrapper, file_cache):
        self.drive_wrapper = drive_wrapper
        self.file_cache = file_cache
        self.file_size_checker = FileSizeChecker()

    def download_reports(self, drive_api_files):
        LOG.debug("Found %d reports from Google Drive: %s", len(drive_api_files), drive_api_files)

        reports = {}
        # Sum up sizes
        sum_bytes = sum([int(f.size) for f in drive_api_files])
        downloaded_bytes = 0
        LOG.info(
            "Size of %d report files from Google Drive: %s", len(drive_api_files), StringUtils.format_bytes(sum_bytes)
        )
        for idx, drive_api_file in enumerate(drive_api_files):
            LOG.info("Processing file [ %d / %d ]", idx + 1, len(drive_api_files))
            LOG.info("Downloaded bytes [ %d / %d ]", downloaded_bytes, sum_bytes)
            key = GoogleDriveCache.create_cached_build_key(drive_api_file)
            file_name = drive_api_file.name
            if not self.file_cache.is_build_data_in_cache(key):
                LOG.info("Report '%s' is not cached for job '%s', downloading...", file_name, key.job_name)

                creation_date, report_file_path = self.download_and_write_to_file_cache(key, drive_api_file)
            else:
                LOG.info("Report '%s' for job '%s' found in cache", file_name, key.job_name)
                report_file_path = self.file_cache.get_filename_for_report(key)
                report_file_path = self._check_file_size(key, drive_api_file, report_file_path)
                creation_date = GoogleDriveCache.determine_creation_date(drive_api_file, report_file_path)
            file_name = os.path.basename(report_file_path)
            reports[report_file_path] = GoogleDriveCache.create_failed_build(file_name, creation_date, key)
            downloaded_bytes += int(drive_api_file.size)

        return reports

    def _check_file_size(self, key: CachedBuildKey, drive_api_file, report_file_path):
        check_result = self.file_size_checker.check_file_size(drive_api_file, key, report_file_path)
        if check_result == FileSizeCheckerResult.SMALL_SIZE_REDOWNLOAD:
            creation_date, report_file_path = self.download_and_write_to_file_cache(key, drive_api_file)
            check_result = self.file_size_checker.check_file_size(drive_api_file, key, report_file_path)
            if check_result == FileSizeCheckerResult.SMALL_SIZE_AFTER_REDOWNLOAD:
                self.file_cache.remove_report(key)
        return report_file_path

    def download_and_write_to_file_cache(self, key, drive_api_file):
        with tempfile.TemporaryDirectory() as tmp:
            downloaded_file = self.drive_wrapper.download_file(drive_api_file.id)
            report_file_tmp_path = os.path.join(tmp, "report.json")
            FileUtils.write_bytesio_to_file(report_file_tmp_path, downloaded_file)
            report_json = JsonFileUtils.load_data_from_json_file(report_file_tmp_path)
            report_file_path = self.file_cache.save_report(report_json, key)
            creation_date = DateUtils.convert_to_datetime(drive_api_file.created_date, DATEFORMAT_GOOGLE_DRIVE)
        return creation_date, report_file_path


class FileSizeChecker:
    def __init__(self):
        self._check_count = defaultdict(int)

    def check_file_size(self, drive_api_file, key: CachedBuildKey, report_file_path):
        check_count = self._check_count[key]
        file_size = self._get_local_file_size(report_file_path)
        file_name = drive_api_file.name
        if file_size >= 10:
            return FileSizeCheckerResult.NORMAL_SIZE

        if check_count == 0:
            LOG.debug(
                "File size is too small, re-downloading report '%s' for job '%s'. File path: %s",
                file_name,
                key.job_name,
                report_file_path,
            )
            return FileSizeCheckerResult.SMALL_SIZE_REDOWNLOAD
        elif check_count == 1:
            LOG.debug(
                "REMOVING FILE FROM CACHE, as file size is too small after re-downloading report '%s' for job '%s'. File path: %s.",
                file_name,
                key.job_name,
                report_file_path,
            )
            return FileSizeCheckerResult.SMALL_SIZE_AFTER_REDOWNLOAD
        else:
            raise ValueError("Unexpected state! Encountered cached_build_key more than twice: {}".format(key))

    @staticmethod
    def _get_local_file_size(report_file_path):
        file_size = os.stat(report_file_path).st_size
        return file_size
