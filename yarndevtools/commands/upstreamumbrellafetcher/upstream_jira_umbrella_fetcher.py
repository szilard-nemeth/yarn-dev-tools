import logging
import os
import sys
from dataclasses import dataclass
from typing import List, Any, Collection, Set, Dict

from pythoncommons.file_utils import FileUtils
from pythoncommons.git_wrapper import GitWrapper, GitLogLineFormat
from pythoncommons.jira_utils import JiraUtils
from pythoncommons.jira_wrapper import JiraWrapper, JiraStatus
from pythoncommons.object_utils import ListUtils, PickleUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.process import CommandRunner
from pythoncommons.project_utils import ProjectUtils
from pythoncommons.string_utils import StringUtils

from pythoncommons.git_constants import (
    HEAD,
    COMMIT_FIELD_SEPARATOR,
    SHORT_SHA_LENGTH,
    ORIGIN,
)

from yarndevtools.commands.upstreamumbrellafetcher.common import (
    JiraUmbrellaData,
    ExecutionMode,
    UpstreamCommitsPerBranch,
)
from yarndevtools.commands.upstreamumbrellafetcher.representation import (
    UmbrellaFetcherOutputManager,
    UmbrellaFetcherRenderedSummary,
    UmbrellaFetcherSummaryData,
)
from yarndevtools.commands_common import CommitData, BackportedJira, BackportedCommit, CommandAbs
from yarndevtools.common.shared_command_utils import SharedCommandUtils, CommandType
from yarndevtools.constants import (
    ORIGIN_TRUNK,
    SummaryFile,
    UPSTREAM_JIRA_BASE_URL,
)
from yarndevtools.yarn_dev_tools_config import YarnDevToolsConfig, DEFAULT_BASE_BRANCH

LOG = logging.getLogger(__name__)
PICKLED_DATA_FILENAME = "pickled_umbrella_data.obj"
COMMON_UPSTREAM_BRANCHES = ["trunk", "branch-3.3", "branch-3.2", "branch-3.1"]
DEFAULT_BRANCH = "trunk"


@dataclass
class JiraData:
    subjira_statuses: Dict[str, JiraStatus]
    resolved_jiras: Set[str]
    not_committed_jiras: Set[str]


class UpstreamJiraUmbrellaFetcherConfig:
    def __init__(
        self, output_dir: str, args, upstream_base_branch: str, upstream_repo: GitWrapper, downstream_repo: GitWrapper
    ):
        # TODO This is overwritten below
        self.output_dir = ProjectUtils.get_session_dir_under_child_dir(FileUtils.basename(output_dir))
        self.execution_mode = (
            ExecutionMode.MANUAL_BRANCH_MODE
            if hasattr(args, "branches") and args.branches
            else ExecutionMode.AUTO_BRANCH_MODE
        )
        self.downstream_branches = args.branches if hasattr(args, "branches") else []
        self.common_upstream_branches: List[str] = (
            list(COMMON_UPSTREAM_BRANCHES)
            if hasattr(args, "add_common_upstream_branches") and args.add_common_upstream_branches
            else []
        )
        self.upstream_repo_path = upstream_repo.repo_path
        self.downstream_repo_path = downstream_repo.repo_path
        self.jira_id = args.jira_id
        self.output_dir = output_dir
        self.upstream_base_branch = upstream_base_branch
        self.force_mode = args.force_mode if hasattr(args, "force_mode") else False
        self.ignore_changes = args.ignore_changes if hasattr(args, "ignore_changes") else False
        self.full_cmd: str or None = None
        self._validate(downstream_repo)
        self.umbrella_result_basedir = FileUtils.join_path(self.output_dir, self.jira_id)
        self.extended_backport_table = False
        self.all_branches_to_consider = list(self.downstream_branches) + list(self.common_upstream_branches)

    def _validate(self, downstream_repo: GitWrapper):
        if self.execution_mode == ExecutionMode.MANUAL_BRANCH_MODE:
            if not self.downstream_branches:
                raise ValueError("Execution mode is 'manual-branch' but no branch was provided. Exiting...")

            LOG.info("Manual branch execution mode, validating provided branches..")
            for branch in self.downstream_branches:
                if not downstream_repo.is_branch_exist(branch):
                    raise ValueError(
                        "Cannot find branch called '{}' in downstream repository {}. "
                        "Please verify the provided branch names!".format(branch, self.downstream_repo_path)
                    )

    def __str__(self):
        downstream_branches_to_check = "N/A"
        if self.downstream_branches:
            downstream_branches_to_check = ", ".join(self.downstream_branches)
        return (
            f"Full command was: {self.full_cmd} \n"
            f"Upstream jira: {self.jira_id}\n"
            f"Upstream repo: {self.upstream_repo_path}\n"
            f"Downstream repo: {self.downstream_repo_path}\n"
            f"Execution mode: {self.execution_mode.name} \n"
            f"Output dir: {self.output_dir} \n"
            f"Umbrella result basedir: {self.umbrella_result_basedir} \n"
            f"Downstream branches to check: {downstream_branches_to_check} \n"
            f"Ignore changes: {self.ignore_changes} \n"
        )


# TODO Add documentation
class UpstreamJiraUmbrellaFetcher(CommandAbs):
    def __init__(
        self, args, upstream_repo: GitWrapper, downstream_repo: GitWrapper, output_dir: str, upstream_base_branch: str
    ):
        self.upstream_repo = upstream_repo
        self.downstream_repo = downstream_repo
        self.config = UpstreamJiraUmbrellaFetcherConfig(
            output_dir, args, upstream_base_branch, upstream_repo, downstream_repo
        )

        # These fields will be assigned when data is fetched
        self.data: JiraUmbrellaData
        self.output_manager = UmbrellaFetcherOutputManager(self.config)

    @staticmethod
    def create_parser(subparsers):
        parser = subparsers.add_parser(
            CommandType.JIRA_UMBRELLA_DATA_FETCHER.name,
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
        parser.set_defaults(func=UpstreamJiraUmbrellaFetcher.execute)

    @staticmethod
    def execute(args, parser=None):
        output_dir = ProjectUtils.get_output_child_dir(CommandType.JIRA_UMBRELLA_DATA_FETCHER.output_dir_name)
        jira_umbrella_fetcher = UpstreamJiraUmbrellaFetcher(
            args, YarnDevToolsConfig.UPSTREAM_REPO, YarnDevToolsConfig.DOWNSTREAM_REPO, output_dir, DEFAULT_BASE_BRANCH
        )
        FileUtils.create_symlink_path_dir(
            CommandType.JIRA_UMBRELLA_DATA_FETCHER.session_link_name,
            jira_umbrella_fetcher.config.umbrella_result_basedir,
            YarnDevToolsConfig.PROJECT_OUT_ROOT,
        )
        jira_umbrella_fetcher.run()

    def run(self):
        self.config.full_cmd = OsUtils.determine_full_command()
        LOG.info(f"Starting umbrella jira fetcher... \n{str(self.config)}")
        self.log_current_upstream_repo_branch()
        self.upstream_repo.fetch(all=True)
        self.downstream_repo.fetch(all=True)
        if self.config.force_mode:
            LOG.info("FORCE MODE is on")
            self.do_fetch()
        else:
            loaded = self.load_pickled_umbrella_data()
            if not loaded:
                self.do_fetch()
            else:
                LOG.info("Loaded pickled data from: %s", self.pickled_data_file)
                self.print_summary()

    def get_file_path_from_basedir(self, file_name):
        return FileUtils.join_path(self.config.umbrella_result_basedir, file_name)

    # TODO Move these outputfile properties to OutputManager
    @property
    def jira_html_file(self):
        return self.get_file_path_from_basedir("jira.html")

    @property
    def jira_list_file(self):
        return self.get_file_path_from_basedir("jira-list.txt")

    def commits_file(self, upstream_branch):
        if os.sep in upstream_branch:
            upstream_branch = upstream_branch.replace(os.sep, "_")
        if "." in upstream_branch:
            upstream_branch = upstream_branch.replace(".", "_")
        return self.get_file_path_from_basedir(f"commit-hashes_{upstream_branch}.txt")

    @property
    def changed_files_file(self):
        return self.get_file_path_from_basedir("changed-files.txt")

    @property
    def summary_file(self):
        return self.get_file_path_from_basedir(SummaryFile.TXT.value)

    @property
    def intermediate_results_file(self):
        return self.get_file_path_from_basedir("intermediate-results.txt")

    @property
    def pickled_data_file(self):
        return self.get_file_path_from_basedir(PICKLED_DATA_FILENAME)

    @property
    def patches_basedir(self):
        return self.get_file_path_from_basedir("patches")

    def do_fetch(self):
        LOG.info("Fetching jira umbrella data...")
        self.data = JiraUmbrellaData()
        self.fetch_jira_ids()
        self.find_upstream_commits_and_save_to_file()
        if self.config.execution_mode == ExecutionMode.AUTO_BRANCH_MODE:
            self.find_downstream_commits_auto_mode()
        elif self.config.execution_mode == ExecutionMode.MANUAL_BRANCH_MODE:
            self.data.backported_jiras = self.find_downstream_commits_manual_mode()
        self.data.execution_mode = self.config.execution_mode

        if self.config.ignore_changes:
            self.data.list_of_changed_files = []
        else:
            self.save_changed_files_to_file()

        self.data.jira_data = self.cross_check_subjira_statuses_with_commits()
        # TODO Write self.subjira_statuses to file
        # TODO Write self.subjira_statuses to table
        self.write_all_changes_files()
        self.pickle_umbrella_data()
        self.print_summary()

    def load_pickled_umbrella_data(self):
        LOG.info("Trying to load pickled data from file: %s", self.pickled_data_file)
        if FileUtils.does_file_exist(self.pickled_data_file):
            self.data = PickleUtils.load(self.pickled_data_file)
            return True
        else:
            LOG.info("Pickled umbrella data file not found under path: %s", self.pickled_data_file)
            return False

    def log_current_upstream_repo_branch(self, force_switch_branch=True):
        curr_branch = self.upstream_repo.get_current_branch_name()
        LOG.info("Current branch: %s", curr_branch)

        if curr_branch != self.config.upstream_base_branch:
            if force_switch_branch:
                self.upstream_repo.checkout_branch(self.config.upstream_base_branch)
                return
            raise ValueError(f"Current branch is not {self.config.upstream_base_branch}. Exiting!")

    def fetch_jira_ids(self):
        LOG.info("Fetching HTML of jira: %s", self.config.jira_id)
        self.data.jira_html = JiraUtils.download_jira_html(
            "https://issues.apache.org/jira/browse/", self.config.jira_id, self.jira_html_file
        )
        self.data.jira_ids_and_titles = JiraUtils.parse_subjiras_and_jira_titles_from_umbrella_html(
            self.data.jira_html, self.jira_list_file, filter_ids=[self.config.jira_id], find_all_links=False
        )
        self.data.subjira_ids = list(self.data.jira_ids_and_titles.keys())
        if not self.data.subjira_ids:
            raise ValueError(f"Cannot find subjiras for jira with id: {self.config.jira_id}")
        LOG.info("Found %d subjiras: %s", len(self.data.subjira_ids), self.data.subjira_ids)
        self.data.piped_jira_ids = "|".join(self.data.subjira_ids)

    def find_upstream_commits_and_save_to_file(self):
        # It's quite complex to grep for multiple jira IDs with gitpython, so let's rather call an external command
        upsream_branches = self._get_branches()
        for upstream_branch in upsream_branches:
            git_log_result = self.upstream_repo.log(
                SharedCommandUtils.ensure_remote_specified(upstream_branch), oneline_with_date=True
            )
            cmd, output = SharedCommandUtils._run_egrep(
                git_log_result, self.intermediate_results_file, self.data.piped_jira_ids
            )
            if not output:
                LOG.warning(
                    f"Cannot find any commits for jira: {self.config.jira_id} on upstream branch: {upstream_branch}"
                )
                self.data.upstream_commits_by_branch[upstream_branch] = UpstreamCommitsPerBranch(upstream_branch, [])
                continue

            normal_commit_lines = output.split("\n")
            modified_log_lines = self._find_missing_upstream_commits_by_message(git_log_result, normal_commit_lines)
            matched_upstream_commit_list = normal_commit_lines + modified_log_lines
            if not matched_upstream_commit_list:
                LOG.warning(
                    f"Cannot find any commits for jira: {self.config.jira_id} on upstream branch: {upstream_branch}"
                )
                self.data.upstream_commits_by_branch[upstream_branch] = UpstreamCommitsPerBranch(upstream_branch, [])
                continue

            # Commits in reverse order (the oldest first)
            matched_upstream_commit_list.reverse()
            upstream_commits_by_branch = UpstreamCommitsPerBranch(upstream_branch, matched_upstream_commit_list)

            LOG.info(
                "Number of matched commits: %s on upstream branch: %s",
                upstream_commits_by_branch.no_of_matched_commits,
                upstream_branch,
            )
            LOG.debug(
                "Matched commits on upstream branch: %s: \n%s",
                upstream_branch,
                StringUtils.list_to_multiline_string(upstream_commits_by_branch.matched_upstream_commit_list),
            )
            FileUtils.save_to_file(
                self.commits_file(upstream_branch),
                StringUtils.list_to_multiline_string(upstream_commits_by_branch.matched_upstream_commit_hashes),
            )
            self.data.upstream_commits_by_branch[upstream_branch] = upstream_commits_by_branch

    def _get_branches(self):
        if self.config.common_upstream_branches:
            upsream_branches: List[str] = self.config.common_upstream_branches
        else:
            upsream_branches: List[str] = [ORIGIN_TRUNK]
        return upsream_branches

    def _find_missing_upstream_commits_by_message(self, git_log_result, normal_commit_lines):
        # Example line:
        # 'bad6038a4879be7b93eb52cfb54ddfd4ce7111cd YARN-10622. Fix preemption policy to exclude childless ParentQueues.
        # Contributed by Andras Gyori 2021-02-15T14:48:42+01:00'
        found_jira_ids = set(map(lambda x: x.split(COMMIT_FIELD_SEPARATOR)[1][:-1], normal_commit_lines))
        not_found_jira_ids = set(self.data.subjira_ids).difference(found_jira_ids)
        not_found_jira_titles = [
            jira_title for jira_id, jira_title in self.data.jira_ids_and_titles.items() if jira_id in not_found_jira_ids
        ]
        LOG.debug("Found jira ids in git log: %s", found_jira_ids)
        LOG.debug("Not found jira ids in git log: %s", not_found_jira_ids)
        LOG.debug("Trying to find commits by jira titles from git log: %s", not_found_jira_titles)

        # If the not_found_jira_titles are all unresolved jiras,
        # egrep would fail, so we don't want to fail the whole script here,
        # so disabling fail_on_error / fail_on_empty_output
        cmd, output = SharedCommandUtils._run_egrep(
            git_log_result, self.intermediate_results_file, "|".join(not_found_jira_titles)
        )
        if not output:
            return []
        output_lines2 = output.split("\n")
        # For these special commits, prepend Jira ID to commit message if it was there
        # Create reverse-dict
        temp_dict = {v: k for k, v in self.data.jira_ids_and_titles.items()}
        modified_log_lines = []
        for log_line in output_lines2:
            # Just a 'smart' heuristic :)
            # Reconstruct commit message by using a merged form of all words until "Contributed".
            commit_msg = ""
            split_line = log_line.split(COMMIT_FIELD_SEPARATOR)
            commit_hash = split_line[0]
            words = split_line[1:]
            for w in words:
                if "Contributed" in w:
                    break
                commit_msg += " " + w
            commit_msg = commit_msg.lstrip()
            if commit_msg not in temp_dict:
                LOG.error("Cannot find Jira ID for commit by its commit message. Git log line: %s", log_line)
            else:
                jira_id = temp_dict[commit_msg]
                words.insert(0, jira_id + ".")
                modified_log_line = commit_hash + " " + COMMIT_FIELD_SEPARATOR.join(words)
                LOG.debug("Adding modified log line. Original: %s, Modified: %s", log_line, modified_log_line)
                modified_log_lines.append(modified_log_line)
        return modified_log_lines

    def find_downstream_commits_auto_mode(self):
        jira_ids = self.get_jira_ids_from_all_upstream_branches()
        for idx, jira_id in enumerate(jira_ids):
            progress = f"[{idx + 1} / {len(jira_ids)}] "
            LOG.info("%s Checking if %s is backported to downstream repo", progress, jira_id)
            downstream_commits_for_jira = self.downstream_repo.log(HEAD, oneline_with_date=True, all=True, grep=jira_id)
            LOG.info("%s Downstream git log result for %s: %s", progress, jira_id, downstream_commits_for_jira)

            if downstream_commits_for_jira:
                backported_commits = [
                    BackportedCommit(
                        CommitData.from_git_log_str(commit_str, format=GitLogLineFormat.ONELINE_WITH_DATE), []
                    )
                    for commit_str in downstream_commits_for_jira
                ]
                LOG.info(
                    "Identified %d backported commits for %s:\n%s",
                    len(backported_commits),
                    jira_id,
                    "\n".join([f"{bc.commit_obj.hash} {bc.commit_obj.message}" for bc in backported_commits]),
                )

                backported_jira: BackportedJira = BackportedJira(jira_id, backported_commits)

                for backported_commit in backported_jira.commits:
                    commit_hash = backported_commit.commit_obj.hash
                    LOG.info(
                        "%s Looking for remote branches of backported commit: %s (hash: %s)",
                        progress,
                        jira_id,
                        commit_hash,
                    )
                    backported_commit.branches = self.downstream_repo.branch(None, recursive=True, contains=commit_hash)
                self.data.backported_jiras[jira_id] = backported_jira
                LOG.info("%s Finished checking downstream backport for jira: %s", progress, jira_id)

    def find_downstream_commits_manual_mode(self):
        branches = self.config.downstream_branches
        grep_intermediate_results_file = self.intermediate_results_file
        downstream_repo = self.downstream_repo
        jira_ids = self.get_jira_ids_from_all_upstream_branches()
        return SharedCommandUtils.find_commits_on_branches(
            branches, grep_intermediate_results_file, downstream_repo, jira_ids
        )

    # TODO Migrate this to OutputManager
    def save_changed_files_to_file(self):
        list_of_changed_files = []
        for c_hash in self.get_commit_hashes_from_all_upstream_branches():
            changed_files = self.upstream_repo.diff_tree(c_hash, no_commit_id=True, name_only=True, recursive=True)
            list_of_changed_files.append(changed_files)
            LOG.debug("List of changed files for commit hash '%s': %s", c_hash, changed_files)
        # Filter dupes, flatten list of lists
        list_of_changed_files = [y for x in list_of_changed_files for y in x]
        self.data.list_of_changed_files = list(set(list_of_changed_files))
        LOG.info("Got %d unique changed files", len(self.data.list_of_changed_files))
        FileUtils.save_to_file(
            self.changed_files_file, StringUtils.list_to_multiline_string(self.data.list_of_changed_files)
        )

    # TODO Migrate this to OutputManager
    def write_all_changes_files(self):
        """
        Iterate over changed files, print all matching changes to the particular file
        Create changes file for each touched file
        :return:
        """
        LOG.info("Recording changes of individual files...")
        for idx, changed_file in enumerate(self.data.list_of_changed_files):
            target_file = FileUtils.join_path(
                self.config.umbrella_result_basedir, "changes", os.path.basename(changed_file)
            )
            FileUtils.ensure_file_exists(target_file, create=True)

            # NOTE: It seems impossible to call the following command with gitpython:
            # git log --follow --oneline -- <file>
            # Use a simple CLI command instead

            # TODO check if change file exists - It can happen that it was deleted
            cli_command = (
                f"cd {self.upstream_repo.repo_path} && "
                f"git log {ORIGIN_TRUNK} --follow --oneline -- {changed_file} | "
                f'egrep "{self.data.piped_jira_ids}"'
            )
            LOG.info("[%d / %d] CLI command: %s", idx + 1, len(self.data.list_of_changed_files), cli_command)
            cmd, output = CommandRunner.run_cli_command(
                cli_command, fail_on_empty_output=False, print_command=False, fail_on_error=False
            )

            if output:
                LOG.info("Saving changes result to file: %s", target_file)
                FileUtils.save_to_file(target_file, output)
            else:
                LOG.error(
                    f"Failed to detect changes of file: {changed_file}. CLI command was: {cli_command}. "
                    f"This seems to be a programming error. Exiting..."
                )
                FileUtils.save_to_file(target_file, "")
                sys.exit(1)

    # TODO Migrate this to OutputManager
    def print_summary(self):
        table_data = self.prepare_table_data()
        summary_data: UmbrellaFetcherSummaryData = UmbrellaFetcherSummaryData(self.config, self.data)
        self.rendered_summary = UmbrellaFetcherRenderedSummary(summary_data, table_data, self.config)
        self.output_manager.print_and_save_summary(self.rendered_summary)

    def pickle_umbrella_data(self):
        LOG.debug("Final umbrella data object: %s", self.data)
        LOG.info("Dumping %s object to file %s", JiraUmbrellaData.__name__, self.pickled_data_file)
        PickleUtils.dump(self.data, self.pickled_data_file)

    # TODO Migrate this to class that is responsible for creating data for table
    def prepare_table_data(self, backport_remote_filter=ORIGIN):
        all_commits_backport_data: List[Any] = []
        # TODO Make extended_backport_table mode non-mutually exclusive of auto/manual branch mode, let user combine these
        # TODO Make sure to add branch presence info dynamically in all cases!!!
        if self.config.extended_backport_table:
            all_commits_backport_data = []
            for backported_jira in self.data.backported_jiras.values():
                for commit in backported_jira.commits:
                    all_commits_backport_data.append(
                        [
                            backported_jira.jira_id,
                            commit.commit_obj.hash[:SHORT_SHA_LENGTH],
                            commit.commit_obj.message,
                            self.filter_branches(backport_remote_filter, commit.branches),
                            commit.commit_obj.date,
                        ]
                    )
        else:
            if self.config.execution_mode == ExecutionMode.AUTO_BRANCH_MODE:
                for jira_id in self.get_jira_ids_from_all_upstream_branches():
                    if jira_id in self.data.backported_jiras:
                        for backported_jira in self.data.backported_jiras.values():
                            all_branches: Collection[str] = self._get_all_branches_for_auto_mode(
                                backport_remote_filter, backported_jira
                            )
                            row = [backported_jira.jira_id, list(set(all_branches))]
                            all_commits_backport_data.append(row)
                    else:
                        row = [jira_id, []]
                        all_commits_backport_data.append(row)
            elif self.config.execution_mode == ExecutionMode.MANUAL_BRANCH_MODE:
                # TODO Handle revert commits
                for jira_id in self.get_jira_ids_from_all_upstream_branches():
                    all_upstream_branches_for_jira = set(self.get_upstream_branches_for_jira(jira_id))
                    if jira_id in self.data.backported_jiras:
                        backported_jira = self.data.backported_jiras[jira_id]
                        all_backport_branches_for_jira = set([br for c in backported_jira.commits for br in c.branches])
                        all_branches = all_backport_branches_for_jira | all_upstream_branches_for_jira
                    else:
                        all_branches = all_upstream_branches_for_jira
                    branch_presence_list = [br in all_branches for br in self.config.all_branches_to_consider]

                    jira_status: JiraStatus = self.data.jira_data.subjira_statuses[jira_id]
                    row = [jira_id, jira_status.resolution, jira_status.status_category] + branch_presence_list
                    all_commits_backport_data.append(row)

                for jira_id in self.data.jira_data.not_committed_jiras:
                    jira_status = self.data.jira_data.subjira_statuses[jira_id]
                    row = [jira_id, jira_status.resolution, jira_status.status_category] + len(
                        self.config.all_branches_to_consider
                    ) * [False]
                    all_commits_backport_data.append(row)
        return all_commits_backport_data

    def _get_all_branches_for_auto_mode(self, backport_remote_filter, backported_jira):
        all_branches = []
        for commit in backported_jira.commits:
            if commit.commit_obj.reverted:
                continue
            branches = self.filter_branches(backport_remote_filter, commit.branches)
            if branches:
                all_branches.extend(branches)
            if self.config.common_upstream_branches:
                all_branches.extend(self.config.common_upstream_branches)
        return all_branches

    @staticmethod
    def filter_branches(backport_remote_filter, branches):
        if backport_remote_filter and any(backport_remote_filter in br for br in branches):
            res_branches = list(filter(lambda br: backport_remote_filter in br, branches))
        else:
            res_branches = branches
        return res_branches

    def get_jira_ids_from_all_upstream_branches(self):
        branches = self.data.upstream_commits_by_branch.keys()
        return self._get_jira_ids_from_branches(branches)

    def _get_jira_ids_from_branches(self, branches):
        all_jira_ids = set()
        for branch in branches:
            commits_per_branch = self.data.upstream_commits_by_branch[branch]
            new_jira_ids = [commit_obj.jira_id for commit_obj in commits_per_branch.matched_upstream_commitdata_list]

            duplicate_jira_ids: Set[str] = ListUtils.get_duplicates(new_jira_ids)
            if duplicate_jira_ids:
                LOG.error(
                    "Detected duplicate Jira IDs: %s\n" "Ideally, this should not happen for Jiras under an umbrella!",
                    duplicate_jira_ids,
                )
            all_jira_ids.update(new_jira_ids)
        return all_jira_ids

    def get_commit_hashes_from_all_upstream_branches(self):
        all_hashes = set()
        for commits_per_branch in self.data.upstream_commits_by_branch.values():
            all_hashes.update([commit_obj.hash for commit_obj in commits_per_branch.matched_upstream_commitdata_list])
        return all_hashes

    def get_upstream_branches_for_jira(self, jira_id):
        # TODO Optimize this lookup
        branches = set()
        for branch, commits_per_branch in self.data.upstream_commits_by_branch.items():
            for commit_data in commits_per_branch.matched_upstream_commitdata_list:
                if commit_data.jira_id == jira_id:
                    branches.add(branch)
                    break
        return list(branches)

    def cross_check_subjira_statuses_with_commits(self):
        jira_wrapper = JiraWrapper(UPSTREAM_JIRA_BASE_URL, DEFAULT_BRANCH, self.patches_basedir)
        subjira_statuses: Dict[str, JiraStatus] = jira_wrapper.get_subjira_statuses_of_umbrella(self.config.jira_id)

        # diff_jira_ids1 = set(self.data.subjira_ids).difference(subjira_statuses.keys())
        # diff_jira_ids2 = set(subjira_statuses.keys()).difference(self.data.subjira_ids)
        jira_ids_of_commits = set(self._get_jira_ids_from_branches([ORIGIN_TRUNK]))
        jiras_resolved = {
            jira_id: jira_status for jira_id, jira_status in subjira_statuses.items() if jira_status == "Done"
        }
        jiras_ids_resolved = set(jiras_resolved.keys())

        not_committed_jiras = set(jiras_ids_resolved).difference(jira_ids_of_commits)
        return JiraData(subjira_statuses, jiras_ids_resolved, not_committed_jiras)
