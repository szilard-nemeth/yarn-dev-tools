from yarndevtools.commands.unittestresultfetcher.cache import UnitTestResultFetcherCacheType
from yarndevtools.commands.unittestresultfetcher.unit_test_result_fetcher import UnitTestResultFetcherMode
from yarndevtools.commands_common import EmailArguments, MongoArguments
from yarndevtools.common.shared_command_utils import CommandType


class UnitTestResultFetcherParser:
    @staticmethod
    def create(subparsers, func):
        parser = subparsers.add_parser(
            CommandType.UNIT_TEST_RESULT_FETCHER.name,
            help="Fetches, parses and sends unit test result reports from Jenkins in email."
            "Example: "
            "--mode jenkins_master "
            "--jenkins-url {jenkins_base_url} "
            "--job-names {job_names} "
            "--testcase-filter org.apache.hadoop.yarn "
            "--smtp_server smtp.gmail.com "
            "--smtp_port 465 "
            "--account_user someuser@somemail.com "
            "--account_password somepassword "
            "--sender 'YARN jenkins test reporter' "
            "--recipients snemeth@cloudera.com "
            "--testcase-filter YARN:org.apache.hadoop.yarn MAPREDUCE:org.apache.hadoop.mapreduce HDFS:org.apache.hadoop.hdfs "
            "--num-builds jenkins_examine_unlimited_builds "
            "--omit-job-summary "
            "--download-uncached-job-data",
        )
        EmailArguments.add_email_arguments(parser, add_subject=False, add_attachment_filename=False)
        MongoArguments.add_mongo_arguments(parser)

        parser.add_argument(
            "--omit-job-summary",
            action="store_true",
            default=False,
            help="Do not print job summaries to the console or the log file",
        )

        parser.add_argument(
            "--force-download-jobs",
            action="store_true",
            dest="force_download_mode",
            help="Force downloading data from all builds. "
            "If this is set to true, all job data will be downloaded, regardless if they are already in the cache",
        )

        parser.add_argument(
            "--download-uncached-job-data",
            action="store_true",
            dest="download_uncached_job_data",
            help="Download data for all builds that are not in cache yet or was removed from the cache, for any reason.",
        )

        parser.add_argument(
            "--force-sending-email",
            action="store_true",
            dest="force_send_email",
            help="Force sending email report for all builds.",
        )

        parser.add_argument(
            "-s",
            "--skip-sending-email",
            dest="skip_email",
            type=bool,
            help="Skip sending email report for all builds.",
        )

        parser.add_argument(
            "--reset-send-state-for-jobs",
            nargs="+",
            type=str,
            dest="reset_send_state_for_jobs",
            default=[],
            help="Reset email send state for these jobs.",
        )

        parser.add_argument(
            "--reset-job-build-data-for-jobs",
            nargs="+",
            type=str,
            dest="reset_job_build_data_for_jobs",
            default=[],
            help="Reset job build data for these jobs. Useful when job build data is corrupted.",
        )

        parser.add_argument(
            "-m",
            "--mode",
            type=str,
            dest="jenkins_mode",
            choices=[m.mode_name.lower() for m in UnitTestResultFetcherMode],
            help="Jenkins mode. Used to pre-configure --jenkins-url and --job-names. "
            "Will take precendence over URL and job names, if they are also specified!",
        )

        parser.add_argument(
            "-J",
            "--jenkins-url",
            type=str,
            dest="jenkins_url",
            help="Jenkins URL to fetch results from",
            default="http://build.infra.cloudera.com/",
        )
        parser.add_argument(
            "--jenkins-user",
            type=str,
            help="Jenkins user for API authentication",
        )
        parser.add_argument(
            "--jenkins-password",
            type=str,
            help="Jenkins password for API authentication",
        )
        parser.add_argument(
            "-j",
            "--job-names",
            type=str,
            dest="job_names",
            help="Jenkins job name to fetch results from",
            default="Mawo-UT-hadoop-CDPD-7.x",
        )

        # TODO Rationalize this vs. request-limit:
        #  Num builds is intended to be used for determining to process the builds that are not yet processed / sent in mail
        #  Request limit is to limit the number of builds processed for each Jenkins job
        parser.add_argument(
            "-n",
            "--num-builds",
            type=str,
            dest="num_builds",
            help="Number of days of Jenkins jobs to examine. "
            "Special value of 'jenkins_examine_unlimited_builds' will examine all unknown builds.",
            default="14",
        )
        parser.add_argument(
            "-rl",
            "--request-limit",
            type=int,
            dest="req_limit",
            help="Request limit",
            default=999,
        )

        def tc_filter_validator(value):
            strval = str(value)
            if ":" not in strval:
                raise ValueError("Filter specification should be in this format: '<project>:<filter statement>'")
            return strval

        parser.add_argument(
            "-t",
            "--testcase-filter",
            dest="tc_filters",
            nargs="+",
            type=tc_filter_validator,
            help="Testcase filters in format: <project:filter statement>",
        )

        # TODO change this to disable cache
        parser.add_argument(
            "-d",
            "--disable-file-cache",
            dest="disable_file_cache",
            type=bool,
            help="Whether to disable Jenkins report file cache",
        )

        parser.add_argument(
            "-ct",
            "--cache-type",
            type=str,
            dest="cache_type",
            choices=[ct.name.lower() for ct in UnitTestResultFetcherCacheType],
            help="The type of the cache. Either file or google_drive",
        )

        parser.add_argument(
            "--load-cached-reports-to-db",
            dest="load_cached_reports_to_db",
            action="store_true",
            help="Whether to save all cached reports from Google Drive to MongoDB",
        )

        parser.add_argument(
            "--disable-sync-from-fs-to-drive",
            dest="disable_sync_from_fs_to_drive",
            action="store_true",
            default=False,
            help="Whether to sync reports from filesystem to Google Drive",
        )

        parser.add_argument(
            "--remove-small-reports",
            dest="remove_small_reports",
            action="store_true",
            default=False,
            help="Whether to remove small reports from FS and Google Drive caches",
        )

        parser.set_defaults(func=func)
