import argparse
import logging
import re
import sys
from yarndevtools.commands.branchcomparator.branch_comparator import CommitMatchingAlgorithm
from yarndevtools.commands.jenkinstestreporter.jenkins_test_reporter import (
    JenkinsTestReporterMode,
    JenkinsTestReporterCacheType,
)
from yarndevtools.commands.unittestresultaggregator.common import SummaryMode, MATCH_EXPRESSION_PATTERN
from yarndevtools.commands.unittestresultaggregator.unit_test_result_aggregator import (
    DEFAULT_LINE_SEP,
)
from yarndevtools.common.shared_command_utils import RepoType, CommandType
from yarndevtools.constants import TRUNK, SUMMARY_FILE_HTML

LOG = logging.getLogger(__name__)

if sys.version_info[:2] >= (3, 7):
    from argparse import ArgumentParser
else:
    LOG.info("Detected python version: " + str(sys.version_info[:2]))
    LOG.info("Replacing ArgumentParser with DelegatedArgumentParser for compatibility reasons.")
    from cdsw_compat import DelegatedArgumentParser as ArgumentParser

# TODO Move all parser static methods to individual commands (maybe abstract base class?)


class ArgParser:
    @staticmethod
    def parse_args(yarn_dev_tools):
        """This function parses and return arguments passed in"""

        # Top-level parser
        parser = ArgumentParser()

        # Subparsers
        subparsers = parser.add_subparsers(
            title="subcommands",
            description="valid subcommands",
            help="Available subcommands",
            required=True,
            dest="command",
        )
        # TODO Pass functions here instead of yarn_dev_tools
        ArgParser.add_save_patch_parser(subparsers, yarn_dev_tools)
        ArgParser.add_create_review_branch_parser(subparsers, yarn_dev_tools)
        ArgParser.add_backport_c6_parser(subparsers, yarn_dev_tools)
        ArgParser.add_upstream_pull_request_fetcher(subparsers, yarn_dev_tools)
        ArgParser.add_save_diff_as_patches(subparsers, yarn_dev_tools)
        ArgParser.diff_patches_of_jira(subparsers, yarn_dev_tools)
        ArgParser.add_fetch_jira_umbrella_data(subparsers, yarn_dev_tools)
        ArgParser.add_branch_comparator(subparsers, yarn_dev_tools)
        ArgParser.add_zip_latest_command_data(subparsers, yarn_dev_tools)
        ArgParser.add_send_latest_command_data(subparsers, yarn_dev_tools)
        ArgParser.add_jenkins_test_reporter(subparsers, yarn_dev_tools)
        ArgParser.add_review_sheet_backport_updater(subparsers, yarn_dev_tools)
        ArgParser.add_reviewsync(subparsers, yarn_dev_tools)
        ArgParser.add_test_result_aggregator(subparsers, yarn_dev_tools)

        # Normal arguments
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            dest="verbose",
            default=None,
            required=False,
            help="More verbose log (including gitpython verbose logs)",
        )
        parser.add_argument(
            "-d",
            "--debug",
            action="store_true",
            dest="debug",
            default=None,
            required=False,
            help="Turn on console debug level logs",
        )

        args = parser.parse_args()
        if args.verbose:
            print("Args: " + str(args))
        return args, parser

    @staticmethod
    def add_save_patch_parser(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.SAVE_PATCH.name, help="Saves patch from upstream repository to yarn patches dir"
        )
        parser.set_defaults(func=yarn_dev_tools.save_patch)

    @staticmethod
    def add_create_review_branch_parser(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.CREATE_REVIEW_BRANCH.name, help="Creates review branch from upstream patch file"
        )
        parser.add_argument("patch_file", type=str, help="Path to patch file")
        parser.set_defaults(func=yarn_dev_tools.create_review_branch)

    @staticmethod
    def add_backport_c6_parser(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.BACKPORT_C6.name,
            help="Backports upstream commit to C6 branch, " "Example usage: <command> YARN-7948 CDH-64201 cdh6.x",
        )
        parser.add_argument("upstream_jira_id", type=str, help="Upstream jira id. Example: YARN-4567")
        parser.add_argument("--upstream_branch", type=str, required=False, default=TRUNK, help="Upstream branch name")

        parser.add_argument("downstream_jira_id", type=str, help="Downstream jira id. Example: CDH-4111")
        parser.add_argument("downstream_branch", type=str, help="Downstream branch name")
        parser.add_argument(
            "--downstream_base_ref",
            type=str,
            required=False,
            help="Downstream commit to base the new downstream branch on",
        )
        parser.add_argument(
            "--no-fetch", action="store_true", required=False, default=False, help="Whether to fetch repositories"
        )
        parser.set_defaults(func=yarn_dev_tools.backport_c6)

    @staticmethod
    def add_upstream_pull_request_fetcher(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.UPSTREAM_PR_FETCH.name,
            help="Fetches upstream changes from a repo then cherry-picks single commit."
            "Example usage: <command> szilard-nemeth YARN-9999",
        )
        parser.add_argument("github_username", type=str, help="Github username")
        parser.add_argument("remote_branch", type=str, help="Name of the remote branch.")
        parser.set_defaults(func=yarn_dev_tools.upstream_pr_fetch)

    @staticmethod
    def add_save_diff_as_patches(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.SAVE_DIFF_AS_PATCHES.name,
            help="Diffs branches and creates patch files with "
            "git format-patch and saves them to a directory."
            "Example: <command> master gpu",
        )
        parser.add_argument("base_refspec", type=str, help="Git base refspec to diff with.")
        parser.add_argument("other_refspec", type=str, help="Git other refspec to diff with.")
        parser.add_argument("dest_basedir", type=str, help="Destination basedir.")
        parser.add_argument("dest_dir_prefix", type=str, help="Directory as prefix to export the patch files to.")
        parser.set_defaults(func=yarn_dev_tools.save_patches)

    @staticmethod
    def diff_patches_of_jira(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.DIFF_PATCHES_OF_JIRA.name,
            help="Diffs patches of a particular jira, for the provided branches."
            "Example: YARN-7913 trunk branch-3.2 branch-3.1",
        )
        parser.add_argument("jira_id", type=str, help="Upstream Jira ID.")
        parser.add_argument("branches", type=str, nargs="+", help="Check all patches on theese branches.")
        parser.set_defaults(func=yarn_dev_tools.diff_patches_of_jira)

    @staticmethod
    def add_fetch_jira_umbrella_data(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.FETCH_JIRA_UMBRELLA_DATA.name,
            help="Fetches jira umbrella data for a provided Jira ID." "Example: fetch_jira_umbrella_data YARN-5734",
        )
        parser.add_argument("jira_id", type=str, help="Upstream Jira ID.")
        parser.add_argument(
            "--force-mode",
            action="store_true",
            dest="force_mode",
            help="Force fetching data from jira and use git log commands to find all changes.",
        )
        parser.add_argument(
            "--ignore-changes",
            dest="ignore_changes",
            action="store_true",
            help="If specified, changes of individual files won't be tracked and written to file.",
        )
        parser.add_argument(
            "--add-common-upstream-branches",
            dest="add_common_upstream_branches",
            action="store_true",
            help="If specified, add common upstream branches to result table.",
        )
        parser.add_argument(
            "--branches", required=False, type=str, nargs="+", help="Check backports against these branches"
        )
        parser.set_defaults(func=yarn_dev_tools.fetch_jira_umbrella_data)

    @staticmethod
    def add_branch_comparator(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.BRANCH_COMPARATOR.name,
            help="Branch comparator."
            "Usage: <algorithm> <feature branch> <master branch>"
            "Example: simple CDH-7.1-maint cdpd-master"
            "Example: grouped CDH-7.1-maint cdpd-master",
        )

        parser.add_argument(
            "algorithm",
            type=CommitMatchingAlgorithm.argparse,
            choices=list(CommitMatchingAlgorithm),
            help="Matcher algorithm",
        )
        parser.add_argument("feature_branch", type=str, help="Feature branch")
        parser.add_argument("master_branch", type=str, help="Master branch")
        parser.add_argument(
            "--commit_author_exceptions",
            type=str,
            nargs="+",
            help="Commits with these authors will be ignored while comparing branches",
        )
        parser.add_argument(
            "--console-mode",
            action="store_true",
            help="Console mode: Instead of writing output files, print everything to the console",
        )
        parser.add_argument(
            "--run-legacy-script",
            action="store_true",
            default=False,
            help="Console mode: Instead of writing output files, print everything to the console",
        )

        repo_types = [rt.value for rt in RepoType]
        parser.add_argument(
            "--repo-type",
            default=RepoType.DOWNSTREAM.value,
            choices=repo_types,
            help=f"Repo type, can be one of: {repo_types}",
        )
        parser.set_defaults(func=yarn_dev_tools.branch_comparator)

    @staticmethod
    def add_zip_latest_command_data(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.ZIP_LATEST_COMMAND_DATA.name,
            help="Zip latest command data." "Example: --dest_dir /tmp",
        )
        parser.add_argument(
            "cmd_type",
            type=str,
            choices=[e.name for e in CommandType if e.session_based],
            help="Type of command. The Command itself should be session-based.",
        )
        parser.add_argument("--dest_dir", required=False, type=str, help="Directory to create the zip file into")
        parser.add_argument("--dest_filename", required=False, type=str, help="Zip filename")
        parser.add_argument(
            "--ignore-filetypes",
            required=False,
            type=str,
            nargs="+",
            help="Filetype to ignore so they won't be added to the resulted zip file.",
        )
        parser.set_defaults(func=yarn_dev_tools.zip_latest_command_data)

    @staticmethod
    def add_send_latest_command_data(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.SEND_LATEST_COMMAND_DATA.name,
            help="Sends latest command data in email." "Example: --dest_dir /tmp",
        )
        parser.add_argument(
            "--file-as-email-body-from-zip",
            dest="email_body_file",
            required=False,
            type=str,
            help="The specified file from the latest command data zip will be added to the email body.",
            default=SUMMARY_FILE_HTML,
        )

        parser.add_argument(
            "--prepend_email_body_with_text",
            dest="prepend_email_body_with_text",
            required=False,
            type=str,
            help="Prepend the specified text to the email's body.",
            default=SUMMARY_FILE_HTML,
        )

        parser.add_argument(
            "-s",
            "--send-attachment",
            dest="send_attachment",
            action="store_true",
            default=False,
            help="Send command data as email attachment",
        )
        ArgParser.add_email_arguments(parser)
        parser.set_defaults(func=yarn_dev_tools.send_latest_command_data)

    @staticmethod
    def add_jenkins_test_reporter(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.JENKINS_TEST_REPORTER.name,
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
        ArgParser.add_email_arguments(parser, add_subject=False, add_attachment_filename=False)

        # TODO seems to be unused
        parser.add_argument(
            "--console-mode",
            action="store_true",
            help="Console mode: Instead of writing output files, print everything to the console",
        )

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
            "--reset-sent-state-for-jobs",
            nargs="+",
            type=str,
            dest="reset_sent_state_for_jobs",
            default=[],
            help="Reset email sent state for these jobs.",
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
            choices=[m.mode_name.lower() for m in JenkinsTestReporterMode],
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
            "-j",
            "--job-names",
            type=str,
            dest="job_names",
            help="Jenkins job name to fetch results from",
            default="Mawo-UT-hadoop-CDPD-7.x",
        )

        # TODO Rationalize this vs. request-limit:
        # Num builds is intended to be used for determining to process the builds that are not yet processed / sent in mail
        # Request limit is to limit the number of builds processed for each Jenkins job
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
            choices=[ct.name.lower() for ct in JenkinsTestReporterCacheType],
            help="The type of the cache. Either file or google_drive",
        )

        parser.set_defaults(func=yarn_dev_tools.fetch_send_jenkins_test_report)

    @staticmethod
    def add_test_result_aggregator(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.UNIT_TEST_RESULT_AGGREGATOR.name,
            help="Aggregates unit test results from a gmail account."
            "Example: "
            "--gsheet "
            "--gsheet-client-secret /Users/snemeth/.secret/dummy.json "
            "--gsheet-spreadsheet 'Failed testcases parsed from emails [generated by script]' "
            "--gsheet-worksheet 'Failed testcases'",
        )
        gsheet_group = ArgParser.add_gsheet_arguments(parser)

        gsheet_group.add_argument(
            "--ghseet-compare-with-jira-table",
            dest="gsheet_compare_with_jira_table",
            type=str,
            help="This should be provided if comparison of failed testcases with reported jira table must be performed. "
            "The value is a name to a worksheet, for example 'testcases with jiras'.",
        )

        parser.add_argument(
            "--account-email",
            required=True,
            type=str,
            help="Email address of Gmail account that will be used to Gmail API authentication and fetching data.",
        )

        parser.add_argument(
            "-q",
            "--gmail-query",
            required=True,
            type=str,
            help="Gmail query string that will be used to get emails to parse.",
        )

        parser.add_argument(
            "--smart-subject-query",
            action="store_true",
            default=False,
            help="Whether to fix Gmail queries like: 'Subject: YARN Daily unit test report', "
            "where the subject should have been between quotes.",
        )

        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            dest="verbose",
            default=None,
            required=False,
            help="More verbose log",
        )

        parser.add_argument(
            "-m",
            "--match-expression",
            required=False,
            type=ArgParser.matches_match_expression_pattern,
            nargs="+",
            help="Line matcher expression, this will be converted to a regex. "
            "For example, if expression is org.apache, the regex will be .*org\\.apache\\.* "
            "Only lines in the mail content matching for this expression will be considered as a valid line.",
        )

        parser.add_argument(
            "-s",
            "--skip-lines-starting-with",
            required=False,
            type=str,
            nargs="+",
            help="If lines starting with these strings, they will not be considered as a line to parse",
        )

        parser.add_argument(
            "-l",
            "--request-limit",
            dest="request_limit",
            type=int,
            help="Limit the number of API requests",
        )

        parser.add_argument("--email-content-line-separator", type=str, default=DEFAULT_LINE_SEP)

        parser.add_argument(
            "--truncate-subject",
            dest="truncate_subject",
            type=str,
            help="Whether to truncate subject in outputs. The specified string will be cropped "
            "from the full value of subject strings when printing them to any destination.",
        )

        parser.add_argument(
            "--abbreviate-testcase-package",
            dest="abbrev_testcase_package",
            type=str,
            help="Whether to abbreviate testcase package names in outputs in order to screen space. "
            "The specified string will be abbreviated with the starting letters."
            "For example, specifying 'org.apache.hadoop.yarn' will be converted to 'o.a.h.y' "
            "when printing testcase names to any destination.",
        )

        parser.add_argument(
            "--summary-mode",
            dest="summary_mode",
            type=str,
            choices=[sm.value for sm in SummaryMode],
            default=SummaryMode.HTML.value,
            help="Summary file(s) will be written in this mode. Defaults to HTML.",
        )

        parser.add_argument(
            "--aggregate-filters",
            dest="aggregate_filters",
            required=True,
            type=str,
            nargs="+",
            help="Execute some post filters on the email results. "
            "The resulted emails and testcases for each filter will be aggregated to "
            "a separate worksheet with name <WS>_aggregated_<aggregate-filter> where WS is equal to the "
            "value specified by the --gsheet-worksheet argument.",
        )

        exclusive_group = parser.add_mutually_exclusive_group(required=True)
        exclusive_group.add_argument(
            "-p", "--print", action="store_true", dest="do_print", help="Print results to console", required=False
        )
        exclusive_group.add_argument(
            "-g",
            "--gsheet",
            action="store_true",
            dest="gsheet",
            default=False,
            required=False,
            help="Export values to Google sheet. Additional gsheet arguments need to be specified!",
        )

        parser.set_defaults(func=yarn_dev_tools.unit_test_result_aggregator)

    @staticmethod
    def add_review_sheet_backport_updater(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.REVIEW_SHEET_BACKPORT_UPDATER.name,
            help="Writes backport status to the Review sheet."
            "Example: "
            "--gsheet "
            "--gsheet-client-secret /Users/snemeth/.secret/dummy.json "
            "--gsheet-spreadsheet 'Failed testcases parsed from emails [generated by script]' "
            "--gsheet-worksheet 'Failed testcases'",
        )
        gsheet_group = ArgParser.add_gsheet_arguments(parser)

        gsheet_group.add_argument(
            "--gsheet-jira-column",
            dest="gsheet_jira_column",
            required=False,
            help="Name of the column that contains Jira issue IDs in the GSheet spreadsheet",
        )

        gsheet_group.add_argument(
            "--gsheet-update-date-column",
            dest="gsheet_update_date_column",
            required=False,
            help="Name of the column where this script will store last updated date in the GSheet spreadsheet",
        )

        gsheet_group.add_argument(
            "--gsheet-status-info-column",
            dest="gsheet_status_info_column",
            required=False,
            help="Name of the column where this script will store patch status info in the GSheet spreadsheet",
        )

        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            dest="verbose",
            default=None,
            required=False,
            help="More verbose log",
        )
        parser.add_argument(
            "--branches", required=True, type=str, nargs="+", help="Check backports against these branches"
        )

        parser.set_defaults(func=yarn_dev_tools.review_sheet_backport_updater)

    @staticmethod
    def add_reviewsync(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.REVIEWSYNC.name,
            help="This script retrieves patches for specified jiras and generates input file for conflict checker script"
            "Example: "
            "--gsheet "
            "--gsheet-client-secret /Users/snemeth/.secret/dummy.json "
            "--gsheet-spreadsheet 'YARN/MR Reviews' "
            "--gsheet-worksheet 'Incoming'",
        )

        parser.add_argument(
            "-b",
            "--branches",
            nargs="+",
            type=str,
            help="List of branches to apply patches that are targeted to trunk (default is trunk only)",
            required=False,
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            dest="verbose",
            default=None,
            required=False,
            help="More verbose log",
        )

        exclusive_group = parser.add_mutually_exclusive_group()
        exclusive_group.add_argument(
            "-i", "--issues", nargs="+", type=str, help="List of Jira issues to check", required=False
        )
        exclusive_group.add_argument(
            "-g",
            "--gsheet",
            action="store_true",
            dest="gsheet_enable",
            default=False,
            required=False,
            help="Enable reading values from Google Sheet API. " "Additional gsheet arguments need to be specified!",
        )

        # Arguments for Google sheet integration
        gsheet_group = parser.add_argument_group("google-sheet", "Arguments for Google sheet integration")

        gsheet_group.add_argument(
            "--gsheet-client-secret",
            dest="gsheet_client_secret",
            required=False,
            help="Client credentials for accessing Google Sheet API",
        )

        gsheet_group.add_argument(
            "--gsheet-spreadsheet", dest="gsheet_spreadsheet", required=False, help="Name of the GSheet spreadsheet"
        )

        gsheet_group.add_argument(
            "--gsheet-worksheet",
            dest="gsheet_worksheet",
            required=False,
            help="Name of the worksheet in the GSheet spreadsheet",
        )

        gsheet_group.add_argument(
            "--gsheet-jira-column",
            dest="gsheet_jira_column",
            required=False,
            help="Name of the column that contains jira issue IDs in the GSheet spreadsheet",
        )

        gsheet_group.add_argument(
            "--gsheet-update-date-column",
            dest="gsheet_update_date_column",
            required=False,
            help="Name of the column where this script will store last updated date in the GSheet spreadsheet",
        )

        gsheet_group.add_argument(
            "--gsheet-status-info-column",
            dest="gsheet_status_info_column",
            required=False,
            help="Name of the column where this script will store patch status info in the GSheet spreadsheet",
        )

        parser.set_defaults(func=yarn_dev_tools.reviewsync)

    # TODO Use dash notation instead of underscore
    @staticmethod
    def add_email_arguments(parser, add_subject=True, add_attachment_filename=True):
        parser.add_argument("--smtp_server", required=True, type=str, help="SMPT server")
        parser.add_argument("--smtp_port", required=True, type=str, help="SMTP port")
        parser.add_argument("--account_user", required=True, type=str, help="Email account's user")
        parser.add_argument("--account_password", required=True, type=str, help="Email account's password")
        if add_subject:
            parser.add_argument("--subject", required=True, type=str, help="Subject of the email")
        parser.add_argument("--sender", required=True, type=str, help="Sender of the email [From]")
        parser.add_argument("--recipients", required=True, type=str, nargs="+", help="List of email recipients [To]")
        if add_attachment_filename:
            parser.add_argument("--attachment-filename", required=False, type=str, help="Override attachment filename")

    @staticmethod
    def add_gsheet_arguments(parser):
        # Arguments for Google sheet integration
        gsheet_group = parser.add_argument_group("google-sheet", "Arguments for Google sheet integration")

        gsheet_group.add_argument(
            "--gsheet-client-secret",
            dest="gsheet_client_secret",
            required=False,
            help="Client credentials for accessing Google Sheet API",
        )

        gsheet_group.add_argument(
            "--gsheet-spreadsheet",
            dest="gsheet_spreadsheet",
            required=False,
            help="Name of the Google Sheet spreadsheet",
        )

        gsheet_group.add_argument(
            "--gsheet-worksheet",
            dest="gsheet_worksheet",
            required=False,
            help="Name of the worksheet in the Google Sheet spreadsheet",
        )
        return gsheet_group

    @staticmethod
    def matches_match_expression_pattern(value):
        if not re.match(MATCH_EXPRESSION_PATTERN, value):
            raise argparse.ArgumentTypeError(
                f"Must conform to this format: <alias>::<pattern>. Provided value: {value}"
            )
        return value
