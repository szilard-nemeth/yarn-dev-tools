#!/usr/bin/python

import logging
from collections import OrderedDict
from typing import Dict, List

from googleapiwrapper.google_sheet import GSheetWrapper, GSheetOptions, GenericCellUpdate
from pythoncommons.file_utils import FileUtils
from pythoncommons.git_wrapper import GitWrapper
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils
from pythoncommons.result_printer import BasicResultPrinter
from pythoncommons.jira_wrapper import JiraFetchMode, PatchOverallStatus, PatchApply, JiraPatchStatus
import datetime
import time

from yarndevtools.commands.reviewsync.jira_wrapper import HadoopJiraWrapper

DEFAULT_BRANCH = "trunk"
JIRA_URL = "https://issues.apache.org/jira"
BRANCH_PREFIX = "reviewsync"
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

    def _validate_args(self, parser, args):
        if not args.issues and not args.gsheet_enable:
            parser.error(
                "Either list of jira issues (--issues) or Google Sheet integration (--gsheet) need to be provided!"
            )

        # TODO check existence + readability on secret file!!
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


class ReviewSync:
    def __init__(self, args, parser, output_dir: str, upstream_repo):
        self.config = ReviewSyncConfig(parser, args, output_dir)
        self.output_dir = output_dir
        self.branches = self.get_branches(args)
        self.upstream_repo: GitWrapper = upstream_repo
        self.jira_wrapper = HadoopJiraWrapper(JIRA_URL, DEFAULT_BRANCH, self.config.patches_dir, self.upstream_repo)
        self.issue_fetch_mode = args.fetch_mode
        if self.issue_fetch_mode == JiraFetchMode.GSHEET:
            self.gsheet_wrapper: GSheetWrapper = GSheetWrapper(args.gsheet_options)

    def run(self):
        start_time = time.time()
        results = self.sync()
        if results:
            self.print_results_table(results)
            if self.issue_fetch_mode == JiraFetchMode.GSHEET:
                LOG.info("Updating GSheet with results...")
                self.update_gsheet(results)
        end_time = time.time()
        LOG.info("Execution of script took %d seconds", end_time - start_time)

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
        issues = self.get_or_fetch_issues()
        if not issues or len(issues) == 0:
            LOG.info("No Jira issues found using fetch mode: %s", self.issue_fetch_mode)
            return

        LOG.info("Jira issues will be review-synced: %s", issues)
        LOG.info("Branches specified: %s", self.branches)

        self.upstream_repo.fetch(all=True)
        self.upstream_repo.validate_branches(self.branches)

        # key: jira issue ID
        # value: list of PatchApply objects
        # For non-applicable patches (e.g. jira is already Resolved, patch object is None)

        results = OrderedDict()
        for issue_id in issues:
            if not issue_id:
                LOG.warning("Found issue with empty issue ID! One reason could be an empty row of a Google sheet!")
                continue
            if "-" not in issue_id:
                LOG.warning("Found issue with suspicious issue ID: %s", issue_id)
                continue

            committed_on_branches = self.get_remote_branches_committed_for_issue(issue_id)
            LOG.info("Issue %s is committed on branches: %s", issue_id, committed_on_branches)
            patches = self.download_latest_patches(issue_id, committed_on_branches)
            if len(patches) == 0:
                results[issue_id] = []
                for branch in self.branches:
                    results[issue_id].append(PatchApply(None, branch, JiraPatchStatus.CANNOT_FIND_PATCH))
                LOG.warning("No patch found for Jira issue %s!", issue_id)
                continue

            for patch in patches:
                patch_applies = self.upstream_repo.apply_patch_advanced(patch, branch_prefix=BRANCH_PREFIX)
                if patch.issue_id not in results:
                    results[patch.issue_id] = []
                results[patch.issue_id] += patch_applies

        self.set_overall_status_for_results(results)
        LOG.info("List of Patch applies: %s", str(results))
        return results

    @classmethod
    def set_overall_status_for_results(cls, results):
        for issue_id, patch_applies in results.items():
            statuses = set(map(lambda pa: pa.result, patch_applies))
            if len(statuses) == 1 and next(iter(statuses)) == JiraPatchStatus.PATCH_ALREADY_COMMITTED:
                cls._set_overall_status_for_patches(issue_id, patch_applies, PatchOverallStatus("ALL COMMITTED"))
                continue

            statuses = []
            for patch_apply in patch_applies:
                status = cls._translate_patch_apply_status_to_str(patch_apply)
                statuses.append(status)

            cls._set_overall_status_for_patches(issue_id, patch_applies, PatchOverallStatus(", ".join(statuses)))

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

    def print_results_table(self, results):
        data, headers = self.convert_data_for_result_printer(results)
        BasicResultPrinter.print_table(data, headers)

    def update_gsheet(self, results: Dict[str, List[PatchApply]]):
        update_date_str = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        status_per_jira = self._get_status_for_jira_ids(results)
        cell_updates = [
            GenericCellUpdate(jira_id, {"status": status, "update_date": update_date_str})
            for jira_id, status in status_per_jira.items()
        ]
        self.gsheet_wrapper.update_issues_with_results(cell_updates)

    @staticmethod
    def _get_status_for_jira_ids(results: Dict[str, List[PatchApply]]) -> Dict[str, str]:
        status_per_jira: Dict[str, str] = {}
        for issue_id, patch_applies in results.items():
            if len(patch_applies) > 0:
                patch = patch_applies[0].patch
                if patch:
                    overall_status = patch.overall_status
                else:
                    # We only have the PatchApply object here, not the Patch
                    overall_status = PatchOverallStatus(patch_applies[0].result)
                status_per_jira[issue_id] = overall_status.status
        return status_per_jira

    @staticmethod
    def convert_data_for_result_printer(results):
        data = []
        headers = [
            "Row",
            "Issue",
            "Patch apply",
            "Owner",
            "Patch file",
            "Branch",
            "Explicit",
            "Result",
            "Number of conflicted files",
            "Overall result",
        ]
        row = 0
        for issue_id, patch_applies in results.items():
            for idx, patch_apply in enumerate(patch_applies):
                row += 1
                patch = patch_apply.patch
                explicit = "Yes" if patch_apply.explicit else "No"
                conflicts = "N/A" if patch_apply.conflicts == 0 else str(patch_apply.conflicts)
                if patch:
                    owner = patch.owner_display_name
                    filename = patch.filename
                    status = patch.overall_status.status
                else:
                    owner = "N/A"
                    filename = "N/A"
                    status = "N/A"
                data.append(
                    [
                        row,
                        issue_id,
                        idx + 1,
                        owner,
                        filename,
                        patch_apply.branch,
                        explicit,
                        patch_apply.result,
                        conflicts,
                        status,
                    ]
                )

        return data, headers

    def get_remote_branches_committed_for_issue(self, issue_id):
        commit_hashes = self.upstream_repo.get_commit_hashes(issue_id)
        remote_branches = self.upstream_repo.get_remote_branches_for_commits(commit_hashes)
        return set(remote_branches)
