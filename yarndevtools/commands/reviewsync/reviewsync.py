#!/usr/bin/python

import logging
from typing import Dict

from googleapiwrapper.google_sheet import GSheetWrapper, GSheetOptions, GenericCellUpdate
from pythoncommons.file_utils import FileUtils
from pythoncommons.git_wrapper import GitWrapper
from pythoncommons.github_utils import GitHubUtils, GitHubRepoIdentifier, GithubPRMergeStatus

from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils
from pythoncommons.jira_wrapper import JiraFetchMode, PatchOverallStatus, PatchApply, JiraPatchStatus
import datetime
import time

from yarndevtools.commands.reviewsync.common import ReviewsyncData
from yarndevtools.commands.reviewsync.jira_wrapper import HadoopJiraWrapper
from yarndevtools.commands.reviewsync.representation import ReviewSyncOutputManager
from yarndevtools.commands_common import CommandAbs
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.constants import TRUNK, ORIGIN_TRUNK, UPSTREAM_JIRA_SERVER_URL
from yarndevtools.yarn_dev_tools_config import YarnDevToolsConfig

DEFAULT_BRANCH = "trunk"
BRANCH_PREFIX = "reviewsync"
APACHE_HADOOP_REPO_IDENTIFIER = GitHubRepoIdentifier("apache", "hadoop")
LOG = logging.getLogger(__name__)

__author__ = "Szilard Nemeth"


class ReviewSyncConfig:
    def __init__(self, parser, args, output_dir: str):
        self._validate_args(parser, args)
        self.output_dir = output_dir
        self.session_dir = ProjectUtils.get_session_dir_under_child_dir(FileUtils.basename(output_dir))
        self.full_cmd: str = OsUtils.determine_full_command_filtered(filter_password=True)
        self.downstream_branches = args.branches if hasattr(args, "branches") else []
        self.issues = args.issues if hasattr(args, "issues") else []

    @staticmethod
    def _validate_args(parser, args):
        if not args.issues and not args.gsheet_enable:
            parser.error(
                "Either list of jira issues (--issues) or Google Sheet integration (--gsheet) need to be provided!"
            )

        if args.gsheet_enable and (
            args.gsheet_client_secret is None
            or args.gsheet_spreadsheet is None
            or args.gsheet_worksheet is None
            or args.gsheet_jira_column is None
        ):
            parser.error(
                "--gsheet requires --gsheet-client-secret, --gsheet-spreadsheet, --gsheet-worksheet and --gsheet-jira-column."
            )

        if args.issues and len(args.issues) > 0:
            LOG.info("Using fetch mode: issues")
            args.fetch_mode = JiraFetchMode.ISSUES_CMDLINE
        elif args.gsheet_enable:
            LOG.info("Using fetch mode: gsheet")
            args.fetch_mode = JiraFetchMode.GSHEET
            args.gsheet_options = GSheetOptions(
                args.gsheet_client_secret,
                args.gsheet_spreadsheet,
                args.gsheet_worksheet,
                args.gsheet_jira_column,
                update_date_column=args.gsheet_update_date_column,
                status_column=args.gsheet_status_info_column,
            )
        else:
            raise ValueError("Unknown fetch mode!")

    def __str__(self):
        return (
            f"Full command was: {self.full_cmd}\n"
            f"Output dir: {self.output_dir}\n"
            f"Session dir: {self.session_dir}\n"
        )

    def get_file_path_from_basedir(self, file_name):
        return FileUtils.join_path(self.output_dir, file_name)

    @property
    def patches_dir(self):
        return self.get_file_path_from_basedir("patches")


class ReviewSync(CommandAbs):
    def __init__(self, args, parser, output_dir: str, upstream_repo):
        self.config = ReviewSyncConfig(parser, args, output_dir)
        self.output_dir = output_dir
        self.branches = self.get_branches(args)
        self.upstream_repo: GitWrapper = upstream_repo
        self.jira_wrapper = HadoopJiraWrapper(
            UPSTREAM_JIRA_SERVER_URL, DEFAULT_BRANCH, self.config.patches_dir, self.upstream_repo
        )
        self.issue_fetch_mode = args.fetch_mode
        if self.issue_fetch_mode == JiraFetchMode.GSHEET:
            self.gsheet_wrapper: GSheetWrapper = GSheetWrapper(args.gsheet_options)
        self.data = ReviewsyncData()

    @staticmethod
    def create_parser(subparsers):
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

        parser.set_defaults(func=ReviewSync.execute)

    @staticmethod
    def execute(args, parser=None):
        output_dir = ProjectUtils.get_output_child_dir(CommandType.REVIEWSYNC.output_dir_name)
        reviewsync = ReviewSync(args, parser, output_dir, YarnDevToolsConfig.UPSTREAM_REPO)
        FileUtils.create_symlink_path_dir(
            CommandType.REVIEWSYNC.session_link_name,
            reviewsync.config.session_dir,
            YarnDevToolsConfig.PROJECT_OUT_ROOT,
        )
        reviewsync.run()

    def run(self):
        start_time = time.time()
        self.sync()
        if self.data.patch_applies_for_issues:
            output_manager = ReviewSyncOutputManager(self.config)
            output_manager.print_summary(self.data)
            if self.issue_fetch_mode == JiraFetchMode.GSHEET:
                LOG.info("Updating GSheet with results...")
                self.update_gsheet()
        end_time = time.time()
        LOG.info("Execution of script took %d seconds", end_time - start_time)

        # Check out trunk when finished execution
        self.upstream_repo.cleanup()
        self.upstream_repo.checkout_branch(TRUNK)

    def get_or_fetch_issues(self):
        if self.issue_fetch_mode == JiraFetchMode.ISSUES_CMDLINE:
            LOG.info("Using Jira fetch mode from issues specified from command line.")
            if not self.config.issues or len(self.config.issues) == 0:
                raise ValueError("Jira issues should be specified!")
            return self.config.issues
        elif self.issue_fetch_mode == JiraFetchMode.GSHEET:
            LOG.info("Using Jira fetch mode from GSheet.")
            return self.gsheet_wrapper.fetch_jira_data()
        else:
            raise ValueError(
                "Unknown state! Jira fetch mode should be either "
                "{} or {} but it is {}".format(
                    JiraFetchMode.ISSUES_CMDLINE, JiraFetchMode.GSHEET, self.issue_fetch_mode
                )
            )

    @staticmethod
    def get_branches(args):
        branches = [DEFAULT_BRANCH]
        if args.branches and len(args.branches) > 0:
            if DEFAULT_BRANCH in args.branches:
                args.branches.remove(DEFAULT_BRANCH)
            branches = branches + args.branches
        return branches

    def sync(self):
        self.data.issues = self.get_or_fetch_issues()
        if not self.data.issues or len(self.data.issues) == 0:
            LOG.info("No Jira issues found using fetch mode: %s", self.issue_fetch_mode)
            return

        LOG.info("Jira issues will be review-synced: %s", self.data.issues)
        LOG.info("Branches specified: %s", self.branches)

        self.upstream_repo.fetch(all=True)
        self.upstream_repo.validate_branches(self.branches)

        # key: jira issue ID
        # value: list of PatchApply objects
        # For non-applicable patches (e.g. jira is already Resolved, patch object is None)

        for issue_id in self.data.issues:
            if not issue_id:
                LOG.warning("Found issue with empty issue ID! One reason could be an empty row of a Google sheet!")
                continue
            if "-" not in issue_id:
                LOG.warning("Found issue with suspicious issue ID: %s", issue_id)
                continue

            self.data.commit_branches_for_issues[issue_id] = self.get_remote_branches_committed_for_issue(issue_id)
            LOG.info("Issue %s is committed on branches: %s", issue_id, self.data.commit_branches_for_issues[issue_id])
            self.data.patches_for_issues[issue_id] = self.download_latest_patches(
                issue_id, self.data.commit_branches_for_issues[issue_id]
            )
            self._add_patch_apply_objs_based_on_patches(issue_id)

            if self.no_patch_available_for_issue(issue_id):
                LOG.warning("No patch file found for Jira issue %s!", issue_id)
            gh_pr_statuses: Dict[str, GithubPRMergeStatus] = GitHubUtils.is_pull_request_of_jira_mergeable(
                APACHE_HADOOP_REPO_IDENTIFIER, issue_id, self.branches, use_cache=True
            )
            jira_patch_statuses = JiraPatchStatus.translate_from_github_pr_merge_statuses(gh_pr_statuses)
            self._add_patch_apply_objs_for_each_branch(issue_id, jira_patch_statuses)

        self.set_overall_status_for_results()
        LOG.info("List of Patch applies: %s", str(self.data.patch_applies_for_issues))

    def _add_patch_apply_objs_based_on_patches(self, issue_id):
        for patch in self.data.patches_for_issues[issue_id]:
            patch_applies = self.upstream_repo.apply_patch_advanced(patch, branch_prefix=BRANCH_PREFIX)
            if patch.issue_id not in self.data.patch_applies_for_issues:
                self.data.patch_applies_for_issues[patch.issue_id] = []
            self.data.patch_applies_for_issues[patch.issue_id] += patch_applies

    def _add_patch_apply_objs_for_each_branch(self, issue_id, jira_patch_statuses):
        if issue_id not in self.data.patch_applies_for_issues:
            self.data.patch_applies_for_issues[issue_id] = []
        for branch in self.branches:
            jira_patch_status = jira_patch_statuses[branch]
            self.data.patch_applies_for_issues[issue_id].append(PatchApply(None, branch, jira_patch_status))

    def set_overall_status_for_results(self):
        for issue_id, patch_applies in self.data.patch_applies_for_issues.items():
            statuses = set(map(lambda pa: pa.result, patch_applies))
            if len(statuses) == 1 and next(iter(statuses)) == JiraPatchStatus.PATCH_ALREADY_COMMITTED:
                self._set_overall_status_for_patches(issue_id, patch_applies, PatchOverallStatus("ALL COMMITTED"))
                continue

            statuses = []
            for patch_apply in patch_applies:
                status = self._translate_patch_apply_status_to_str(patch_apply)
                statuses.append(status)

            self._set_overall_status_for_patches(issue_id, patch_applies, PatchOverallStatus(", ".join(statuses)))

    @classmethod
    def _translate_patch_apply_status_to_str(cls, patch_apply):
        status_str = "N/A"
        if patch_apply.result == JiraPatchStatus.CONFLICT:
            status_str = "CONFLICT"
        elif patch_apply.result == JiraPatchStatus.PATCH_ALREADY_COMMITTED:
            status_str = "COMMITTED"
        elif patch_apply.result == JiraPatchStatus.APPLIES_CLEANLY:
            status_str = "OK"
        status = "{}: {}".format(patch_apply.branch, status_str)
        return status

    @classmethod
    def _set_overall_status_for_patches(cls, issue_id, patch_applies, overall_status):
        # As patch object can be different for each PatchApply object, we need to set the overall status for each
        LOG.debug("[%s] Setting overall status %s", issue_id, str(overall_status))
        for pa in patch_applies:
            if pa.patch:
                pa.patch.set_overall_status(overall_status)

    def download_latest_patches(self, issue_id, committed_on_branches):
        patches = self.jira_wrapper.get_patches_per_branch(issue_id, self.branches, committed_on_branches)
        for patch in patches:
            if patch.is_applicable():
                # TODO possible optimization: Just download required files based on branch applicability
                self.jira_wrapper.download_patch_file(patch)
            else:
                LOG.info("Skipping download of non-applicable patch: %s", patch)

        return patches

    def update_gsheet(self):
        update_date_str = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        status_per_jira = self._get_status_for_jira_ids()
        cell_updates = [
            GenericCellUpdate(jira_id, {"status": status, "update_date": update_date_str})
            for jira_id, status in status_per_jira.items()
        ]
        self.gsheet_wrapper.update_issues_with_results(cell_updates)

    def _get_status_for_jira_ids(self) -> Dict[str, str]:
        status_per_jira: Dict[str, str] = {}
        for issue_id, patch_applies in self.data.patch_applies_for_issues.items():
            if len(patch_applies) > 0:
                patch = patch_applies[0].patch
                if patch:
                    overall_status = patch.overall_status
                else:
                    # We only have the PatchApply object here, not the Patch
                    overall_status = PatchOverallStatus(patch_applies[0].result)
                status_per_jira[issue_id] = overall_status.status
        return status_per_jira

    def get_remote_branches_committed_for_issue(self, issue_id):
        commit_hashes = self.upstream_repo.get_commit_hashes(issue_id, branch=ORIGIN_TRUNK)
        remote_branches = self.upstream_repo.get_remote_branches_for_commits(commit_hashes)
        return set(remote_branches)

    def no_patch_available_for_issue(self, issue_id: str):
        return len(self.data.patches_for_issues[issue_id]) == 0
