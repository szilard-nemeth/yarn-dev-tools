import logging
from typing import Dict

from googleapiwrapper.google_sheet import GSheetWrapper, GSheetOptions, GenericCellUpdate
from pythoncommons.file_utils import FileUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils

from yarndevtools.commands.reviewsheetbackportupdater.common import ReviewSheetBackportUpdaterData
from yarndevtools.commands.reviewsheetbackportupdater.representation import ReviewSheetBackportUpdaterOutputManager
from yarndevtools.commands_common import BackportedJira
from yarndevtools.common.shared_command_utils import SharedCommandUtils
from yarndevtools.constants import ANY_JIRA_ID_PATTERN

LOG = logging.getLogger(__name__)


class ReviewSheetBackportUpdaterConfig:
    def __init__(self, parser, args, output_dir: str):
        self._validate_args(parser, args)
        self.output_dir = output_dir
        self.session_dir = ProjectUtils.get_session_dir_under_child_dir(FileUtils.basename(output_dir))
        self.full_cmd: str = OsUtils.determine_full_command_filtered(filter_password=True)
        self.downstream_branches = args.branches if hasattr(args, "branches") else []

    @staticmethod
    def _get_attribute(args, attr_name, default=None):
        val = getattr(args, attr_name)
        if not val:
            return default
        return val

    def _validate_args(self, parser, args):
        self.worksheet = args.gsheet_worksheet
        if args.gsheet_client_secret is None or args.gsheet_spreadsheet is None or self.worksheet is None:
            parser.error(
                "Gsheet requires the following arguments: "
                "--gsheet-client-secret, --gsheet-spreadsheet and --gsheet-worksheet."
            )

        self.gsheet_options = GSheetOptions(
            args.gsheet_client_secret,
            args.gsheet_spreadsheet,
            worksheet=self.worksheet,
            jira_column=args.gsheet_jira_column,
            update_date_column=args.gsheet_update_date_column,
            status_column=args.gsheet_status_info_column,
        )

    def __str__(self):
        return (
            f"Full command was: {self.full_cmd}\n"
            f"Output dir: {self.output_dir}\n"
            f"Session dir: {self.session_dir}\n"
        )


class ReviewSheetBackportUpdater:
    def __init__(self, args, parser, output_dir: str, downstream_repo):
        self.config = ReviewSheetBackportUpdaterConfig(parser, args, output_dir)
        self.gsheet_wrapper: GSheetWrapper or None = GSheetWrapper(self.config.gsheet_options)
        self.downstream_repo = downstream_repo
        self.downstream_repo.fetch(all=True)
        self.data: ReviewSheetBackportUpdaterData = ReviewSheetBackportUpdaterData()

    def run(self):
        LOG.info(f"Starting Review sheet backport updater. Config: \n{str(self.config)}")
        jira_ids = self._load_data_from_sheet()
        self.data.jira_ids = self._sanitize_jira_ids(jira_ids)
        self.data.backported_jiras = SharedCommandUtils.find_commits_on_branches(
            self.config.downstream_branches, self.intermediate_results_file, self.downstream_repo, jira_ids
        )
        self._process_results()

    @staticmethod
    def _sanitize_jira_ids(jira_ids):
        sanitized_jira_ids = []
        for jira_id in jira_ids:
            if " " in jira_id or "\n" in jira_id:
                LOG.warning("Replacing space and newline in Jira ID '%s'", jira_id)
                jira_id = jira_id.replace(" ", "").replace("\n", "")
            sanitized_jira_ids.append(jira_id)
        return sanitized_jira_ids

    def _load_data_from_sheet(self):
        jira_data_from_sheet = self.gsheet_wrapper.fetch_jira_data()
        LOG.info(f"Successfully loaded data from worksheet: {self.config.worksheet}")

        # header: List[str] = raw_data_from_gsheet[0]
        # expected_header = [
        #     "JIRA",
        #     "Description",
        #     "Prio",
        #     "Depends on",
        #     "Status",
        #     "Assignee",
        #     "Patch Owner",
        #     "First line Reviewer",
        #     "Committer Reviewer",
        #     "Target",
        #     "Motivation",
        #     "Component",
        #     "Notes",
        #     "Last Updated",
        #     "Reviewsync",
        #     "Trunk",
        #     "branch-3.2",
        #     "branch-3.1",
        #     "Backported",
        # ]
        # TODO FIX
        # if header != expected_header:
        #     raise ValueError(
        #         "Detected suspicious worksheet table header. "
        #         f"Expected header: {expected_header}, "
        #         f"Current header: {header}"
        #     )

        jira_ids = []
        for jira_id in jira_data_from_sheet:
            matches = ANY_JIRA_ID_PATTERN.findall(jira_id)
            if matches:
                jira_ids.append(jira_id)
        return jira_ids

    @property
    def intermediate_results_file(self):
        return self.get_file_path_from_basedir("intermediate-results.txt")

    def get_file_path_from_basedir(self, file_name):
        return FileUtils.join_path(self.config.output_dir, file_name)

    def _process_results(self):
        # update_date_str = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        status_per_jira = self._get_status_for_jira_ids()

        output_manager = ReviewSheetBackportUpdaterOutputManager(self.config)
        output_manager.print_summary(self.data)

        cell_updates = [GenericCellUpdate(jira_id, {"status": status}) for jira_id, status in status_per_jira.items()]
        self.gsheet_wrapper.update_issues_with_results(cell_updates)

    def _get_status_for_jira_ids(self):
        # TODO Handle revert commits
        result = {}
        for jira_id, backported_jira in self.data.backported_jiras.items():
            if not backported_jira.commits:
                result[jira_id] = "NOT BACKPORTED TO ANY BRANCHES"
            else:
                branches = set()
                commits = set()
                for c in backported_jira.commits:
                    commits.add(c.commit_obj)
                    for br in c.branches:
                        branches.add(br)
                self.data.backported_to_branches[jira_id] = branches
                self.data.commits_of_jira[jira_id] = commits
                if branches:
                    result[jira_id] = "BACKPORTED TO: {}".format(branches)
                else:
                    result[jira_id] = "NOT BACKPORTED TO ANY BRANCHES"
        return result
