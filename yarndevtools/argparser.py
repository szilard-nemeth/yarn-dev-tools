import logging
import sys
from enum import Enum
from yarndevtools.commands.branchcomparator.branch_comparator import CommitMatchingAlgorithm
from yarndevtools.commands.unittestresultaggregator.unit_test_result_aggregator import (
    UnitTestResultAggregator,
    DEFAULT_LINE_SEP,
)
from yarndevtools.common.shared_command_utils import RepoType
from yarndevtools.constants import TRUNK, DEFAULT_COMMAND_DATA_FILE_NAME, SUMMARY_FILE_HTML

LOG = logging.getLogger(__name__)

if sys.version_info[:2] >= (3, 7):
    from argparse import ArgumentParser
else:
    LOG.info("Detected python version: " + str(sys.version_info[:2]))
    LOG.info("Replacing ArgumentParser with DelegatedArgumentParser for compatibility reasons.")
    from cdsw_compat import DelegatedArgumentParser as ArgumentParser


class CommandType(Enum):
    SAVE_PATCH = ("save_patch", False)
    CREATE_REVIEW_BRANCH = ("create_review_branch", False)
    BACKPORT_C6 = ("backport_c6", False)
    UPSTREAM_PR_FETCH = ("upstream_pr_fetch", False)
    SAVE_DIFF_AS_PATCHES = ("save_diff_as_patches", False)
    DIFF_PATCHES_OF_JIRA = ("diff_patches_of_jira", False)
    FETCH_JIRA_UMBRELLA_DATA = ("fetch_jira_umbrella_data", True, "latest-session-upstream-umbrella-fetcher")
    BRANCH_COMPARATOR = ("branch_comparator", True, "latest-session-branchcomparator")
    ZIP_LATEST_COMMAND_DATA = ("zip_latest_command_data", False)
    SEND_LATEST_COMMAND_DATA = ("send_latest_command_data", False)
    JENKINS_TEST_REPORTER = ("jenkins_test_reporter", False)
    UNIT_TEST_RESULT_AGGREGATOR = ("unit_test_result_aggregator", True)

    def __init__(self, value, session_based: bool = False, session_link_name: str = ""):
        self.val = value
        self.session_based = session_based

        if session_link_name:
            self.session_link_name = session_link_name
        else:
            self.session_link_name = f"latest-session-{value}"

    @staticmethod
    def from_str(val):
        val_to_enum = {ct.val: ct for ct in CommandType}
        if val in val_to_enum:
            return val_to_enum[val]
        else:
            raise NotImplementedError


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
            CommandType.SAVE_PATCH.val, help="Saves patch from upstream repository to yarn patches dir"
        )
        parser.set_defaults(func=yarn_dev_tools.save_patch)

    @staticmethod
    def add_create_review_branch_parser(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.CREATE_REVIEW_BRANCH.val, help="Creates review branch from upstream patch file"
        )
        parser.add_argument("patch_file", type=str, help="Path to patch file")
        parser.set_defaults(func=yarn_dev_tools.create_review_branch)

    @staticmethod
    def add_backport_c6_parser(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.BACKPORT_C6.val,
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
        parser.set_defaults(func=yarn_dev_tools.backport_c6)

    @staticmethod
    def add_upstream_pull_request_fetcher(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.UPSTREAM_PR_FETCH.val,
            help="Fetches upstream changes from a repo then cherry-picks single commit."
            "Example usage: <command> szilard-nemeth YARN-9999",
        )
        parser.add_argument("github_username", type=str, help="Github username")
        parser.add_argument("remote_branch", type=str, help="Name of the remote branch.")
        parser.set_defaults(func=yarn_dev_tools.upstream_pr_fetch)

    @staticmethod
    def add_save_diff_as_patches(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.SAVE_DIFF_AS_PATCHES.val,
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
            CommandType.DIFF_PATCHES_OF_JIRA.val,
            help="Diffs patches of a particular jira, for the provided branches."
            "Example: YARN-7913 trunk branch-3.2 branch-3.1",
        )
        parser.add_argument("jira_id", type=str, help="Upstream Jira ID.")
        parser.add_argument("branches", type=str, nargs="+", help="Check all patches on theese branches.")
        parser.set_defaults(func=yarn_dev_tools.diff_patches_of_jira)

    @staticmethod
    def add_fetch_jira_umbrella_data(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.FETCH_JIRA_UMBRELLA_DATA.val,
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
            "--branches", required=False, type=str, nargs="+", help="Check backports againtst these branches"
        )
        parser.set_defaults(func=yarn_dev_tools.fetch_jira_umbrella_data)

    @staticmethod
    def add_branch_comparator(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.BRANCH_COMPARATOR.val,
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
            CommandType.ZIP_LATEST_COMMAND_DATA.val,
            help="Zip latest command data." "Example: --dest_dir /tmp",
        )
        parser.add_argument(
            "cmd_type",
            type=str,
            choices=[e.val for e in CommandType if e.session_based],
            help="Type of command. The Command itself should be session-based.",
        )
        parser.add_argument("--dest_dir", required=False, type=str, help="Directory to create the zip file into")
        parser.add_argument(
            "--dest_filename", required=False, type=str, default=DEFAULT_COMMAND_DATA_FILE_NAME, help="Zip filename"
        )
        parser.add_argument(
            "--ignore-filetypes",
            required=False,
            type=str,
            nargs="+",
            help="Filetype to ignore so they won't be added to the resulted zip file.",
        )
        parser.set_defaults(func=yarn_dev_tools.zip_latest_command_results)

    @staticmethod
    def add_send_latest_command_data(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.SEND_LATEST_COMMAND_DATA.val,
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
        ArgParser.add_email_arguments(parser)
        parser.set_defaults(func=yarn_dev_tools.send_latest_command_results)

    @staticmethod
    def add_jenkins_test_reporter(subparsers, yarn_dev_tools):
        parser = subparsers.add_parser(
            CommandType.JENKINS_TEST_REPORTER.val,
            help="Fetches, parses and sends unit test result reports from Jenkins in email."
            "Example: "
            "--job-name {job_name} "
            "--testcase-filter org.apache.hadoop.yarn "
            "--smtp_server smtp.gmail.com "
            "--smtp_port 465 "
            "--account_user someuser@somemail.com "
            "--account_password somepassword "
            "--sender 'YARN jenkins test reporter' "
            "--recipients snemeth@cloudera.com"
            #
        )
        ArgParser.add_email_arguments(parser, add_subject=False, add_attachment_filename=False)

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
            "--job-name",
            type=str,
            dest="job_name",
            help="Jenkins job name to fetch results from",
            default="Mawo-UT-hadoop-CDPD-7.x",
        )
        parser.add_argument(
            "-n",
            "--num-days",
            type=int,
            dest="num_prev_days",
            help="Number of days to examine",
            default=14,
        )
        parser.add_argument(
            "-rl",
            "--request-limit",
            type=int,
            dest="req_limit",
            help="Request limit",
            default=1,
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

        parser.add_argument(
            "-s",
            "--skip-sending-mail",
            dest="skip_mail",
            type=bool,
            help="Whether to skip sending email report",
        )

        parser.add_argument(
            "-d",
            "--disable-file-cache",
            dest="disable_file_cache",
            type=bool,
            help="Whether to disable Jenkins report file cache",
        )

        parser.set_defaults(func=yarn_dev_tools.fetch_send_jenkins_test_report)

    @staticmethod
    def add_test_result_aggregator(subparsers, yarn_dev_tools):
        """This function parses and return arguments passed in"""

        parser = subparsers.add_parser(
            CommandType.UNIT_TEST_RESULT_AGGREGATOR.val,
            help="Aggregates unit test results from a gmail account."
            "Example: "
            "--gsheet "
            "--gsheet-client-secret /Users/snemeth/.secret/dummy.json "
            "--gsheet-spreadsheet 'Failed testcases parsed from emails [generated by script]' "
            "--gsheet-worksheet 'Failed testcases'",
        )
        ArgParser.add_gsheet_arguments(parser)

        parser.add_argument(
            "-q",
            "--gmail-query",
            required=True,
            type=str,
            help="Gmail query string that will be used to get emails to parse.",
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
            "-l",
            "--request-limit",
            dest="request_limit",
            type=int,
            help="Limit the number of API requests",
        )

        parser.add_argument("--email-content-line-separator", type=str, default=DEFAULT_LINE_SEP)

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
            help="Export values to Google sheet. " "Additional gsheet arguments need to be specified!",
        )

        parser.set_defaults(func=yarn_dev_tools.unit_test_result_aggregator)

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
