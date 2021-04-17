import logging
from datetime import datetime
from typing import Dict, List, Any, Set

from pythoncommons.string_utils import StringUtils, auto_str

from yarndevtools.commands.branchcomparator.common import BranchData, BranchType, CommonCommitsBase
from yarndevtools.commands.branchcomparator.common_representation import SummaryDataAbs
from yarndevtools.commands_common import CommitData

LOG = logging.getLogger(__name__)


# TODO move these to utils class
def get_commits_without_jira_id(branch_data: Dict[BranchType, BranchData], br_type: BranchType):
    return branch_data[br_type].commits_with_missing_jira_id_filtered


def get_commits(branch_data: Dict[BranchType, BranchData], br_type: BranchType):
    return branch_data[br_type].commits_after_merge_base_filtered


def get_commit_hashes(branch_data: Dict[BranchType, BranchData], br_type: BranchType):
    return set([hash for hash in branch_data[br_type].get_commit_hashes()])


def filter_commits_by_hashes(
    branch_data: Dict[BranchType, BranchData], br_type: BranchType, commit_hashes: Set[str]
) -> List[CommitData]:
    commits: List[CommitData] = get_commits(branch_data, br_type)
    return list(filter(lambda c: c.hash in commit_hashes, commits))


@auto_str
class CommitGroup:
    # TODO Should have a property whether it matched by Jira ID or commit message or both
    def __init__(self, br_type: BranchType or None, commits: Set[CommitData]):
        # Put commits into ascending order by date
        self.br_type = br_type
        self.commits: List[CommitData] = sorted(
            commits, key=lambda cd: datetime.strptime(cd.date, "%Y-%m-%dT%H:%M:%S%z")
        )
        self.jira_ids = set([jid for c in self.commits for jid in c.jira_id_data.all_matched_jira_ids])
        # TODO handle revert status: IDEA --> track reverts for unique pairs of all jira ids of a commit

    @property
    def oldest_commit_date(self):
        return self.commits[0].date

    @property
    def size(self):
        return len(self.commits)

    @property
    def commit_hashes(self):
        return [c.hash for c in self.commits]

    @property
    def as_string(self):
        str = ""
        for commit in self.commits:
            str += commit.as_oneline_string(incl_date=True, incl_author=False, incl_committer=True)
            str += "\n"
        return str


class CommonCommits(CommonCommitsBase):
    def __init__(self):
        super().__init__()
        # TODO implement
        # self.after_merge_base: List[Tuple[CommitData, CommitData]] = []

        # Commits matched by message with missing Jira ID
        # self.matched_only_by_message: List[Tuple[CommitData, CommitData]] = []

        # Commits matched by Jira ID but not by message
        # self.matched_only_by_jira_id: List[Tuple[CommitData, CommitData]] = []

        # Commits matched by Jira ID and by message as well
        # self.matched_both: List[Tuple[CommitData, CommitData]] = []


class GroupedCommitMatcherSummaryData(SummaryDataAbs):
    def __init__(self, config, branches):
        super().__init__(config, branches)
        # TODO implement

    def common_commits_after_merge_base(self):
        # TODO implement
        # return [c[0] for c in self.common_commits.after_merge_base]
        pass

    def add_stats_common_commit_details(self, res):
        # res += "\n\n=====Stats: COMMON COMMITS ACROSS BRANCHES=====\n"
        # res += (
        #     f"Number of common commits with missing Jira ID, matched by commit message: "
        #     f"{len(self.common_commits.matched_only_by_message)}\n"
        # )
        # res += (
        #     f"Number of common commits with matching Jira ID but different commit message: "
        #     f"{len(self.common_commits.matched_only_by_jira_id)}\n"
        # )
        # res += (
        #     f"Number of common commits with matching Jira ID and commit message: "
        #     f"{len(self.common_commits.matched_both)}\n"
        # )
        # return res
        # TODO implement
        pass

    def add_stats_common_commits_on_branches(self, res):
        # res += "\n\n=====Stats: COMMON=====\n"
        # res += f"Merge-base commit: {self.branches.merge_base.as_oneline_string(incl_date=True)}\n"
        # res += f"Number of common commits before merge-base: {len(self.common_commits.before_merge_base)}\n"
        # res += f"Number of common commits after merge-base: {len(self.common_commits.after_merge_base)}\n"
        # return res
        # TODO implement
        pass


class GroupedCommitMatcher:
    def __init__(self, branch_data: Dict[BranchType, BranchData]):
        self.branch_data = branch_data

    # def create_common_commits_obj(self) -> CommonCommits:
    #     self.common_commits = CommonCommits()
    #     return self.common_commits
    #
    # def create_summary_data(self, config, branches) -> SummaryDataAbs:
    #     return GroupedCommitMatcherSummaryData(config, branches)

    def match_commits(self) -> Any:
        self.jira_id_to_commits: JiraIdToCommitMappings = JiraIdToCommitMappings(self.branch_data)
        self.commit_grouper: CommitGrouper = CommitGrouper(self.branch_data, self.jira_id_to_commits)
        # TODO print groups that has 2 or more jira IDS (also print to file)


class JiraIdToCommitMappings:
    def __init__(self, branch_data: Dict[BranchType, BranchData]):
        self.branch_data = branch_data
        # Inner-dict key: Jira ID
        self._dict: Dict[BranchType, Dict[str, List[CommitData]]] = self._map_jira_ids_to_commits()

    def _map_jira_ids_to_commits(self):
        # This is better than BranchData.jira_id_to_commits as it maps all jira IDs (including downstream) to commits.
        # At the same time, it does not break the behaviour of SimpleCommitMatcher.

        # Inner-dict: Mapping of jira ids to commit list
        # Outer dict: Branch type
        result_dict: Dict[BranchType, Dict[str, List[CommitData]]] = {}
        for br_data in self.branch_data.values():
            br_type = br_data.type
            if br_type not in result_dict:
                result_dict[br_type] = {}

            dic_to_update = result_dict[br_type]
            for commit in get_commits(self.branch_data, br_data.type):
                jira_ids = commit.jira_id_data.all_matched_jira_ids
                for jid in jira_ids:
                    if jid not in dic_to_update:
                        dic_to_update[jid] = []
                    dic_to_update[jid].append(commit)
        return result_dict

    def get_by_branch_type(self, br_type: BranchType):
        return self._dict[br_type]


class CommitGrouper:
    def __init__(self, branch_data: Dict[BranchType, BranchData], jira_id_to_commits: JiraIdToCommitMappings):
        # Example commit messages from git log:
        # "COMPX-5506 YARN-10500" --> commit 1
        # "YARN-10500" --> commit 1
        # "COMPX-5506" --> commit 1

        # Algorithm: Go over dict, define groups:
        # Key: Jira ID, value: List of commits
        # 1. The list itself is a group
        # 2. Check all commits in the list, check all of their other jira ids
        # 3. For all other jira IDs, fetch commits from the dict
        # 4. Pay attention to ordering
        # 5. Pay attention to revert commits
        self.branch_data = branch_data
        self.jira_id_to_commits = jira_id_to_commits
        self._groups: Dict[BranchType, List[CommitGroup]] = self._create_groups()
        groups_by_msg = self._create_groups_by_message()
        self._groups[BranchType.MASTER].extend(groups_by_msg[BranchType.MASTER])
        self._groups[BranchType.FEATURE].extend(groups_by_msg[BranchType.FEATURE])
        # TODO Start a second-pass that tries to group jira-id based groups with commit message groups?
        # It can happen that a commit message group has the same commit message like already existing commits in groups, with jira ids
        self.sanity_check()
        self.print_group_stats()

    def _create_groups(self):
        groups: Dict[BranchType, List[CommitGroup]] = {}
        for br_type in self.branch_data.keys():
            groups[br_type] = []
            jira_ids_commits_for_branch = self.jira_id_to_commits.get_by_branch_type(br_type)
            visited_commit_hashes: Set[str] = set()

            # In the following scenario, grouped_commits could hold ee50a12d60ca19941f13fd123b9e8a8ea5d41f42 twice.
            # for key: COMPX-3136:
            # 0 = {CommitData} CommitData(hash=ee50a12d60ca19941f13fd123b9e8a8ea5d41f42, jira_id=YARN-10295,
            #     message=COMPX-3136: YARN-10295. CapacityScheduler NPE can cause apps to get stuck without resources.
            #     Contributed by Benjamin Teke, date=2020-06-15T00:48:23-08:00
            # 1 = {CommitData} CommitData(hash=7d51df190fc182f747940c8b90d3bc0e1703de81, jira_id=YARN-10296,
            #     message=COMPX-3136: YARN-10296. Make ContainerPBImpl#getId/setId synchronized.
            #     Contributed by Benjamin Teke, date=2020-06-15T07:57:54-07:00

            # for key: YARN-10295
            # [CommitData(hash=ee50a12d60ca19941f13fd123b9e8a8ea5d41f42, jira_id=YARN-10295,
            #     message=COMPX-3136: YARN-10295. CapacityScheduler NPE can cause apps to get stuck without resources.
            #     Contributed by Benjamin Teke
            # --> SOLUTION: Use set collection type for 'grouped_commits'.
            # --> PROBLEM: This drops away the ordering info of commits.

            # TODO Add all commits with missing Jira ID to dedicated group

            for jira_id, commits in jira_ids_commits_for_branch.items():
                grouped_commits: Set[CommitData] = set()
                for commit in commits:
                    # Optimization: Commit is only visited if all of its jira IDs / other related commits
                    # are checked and added to groups
                    if commit.hash in visited_commit_hashes:
                        continue
                    grouped_commits.add(commit)
                    jira_ids = commit.jira_id_data.all_matched_jira_ids
                    for jid in jira_ids:
                        # skip current jira id, we are interested in the other ones
                        if jid == jira_id:
                            continue
                        other_commits: List[CommitData] = jira_ids_commits_for_branch[jid]
                        grouped_commits.update(other_commits)
                if len(grouped_commits) > 0:
                    groups[br_type].append(CommitGroup(br_type, grouped_commits))
                    visited_commit_hashes.update([c.hash for c in grouped_commits])
        return groups

    def _create_groups_by_message(self) -> Dict[BranchType, List[CommitGroup]]:
        groups: Dict[BranchType, List[CommitGroup]] = {}
        for br_type in self.branch_data.keys():
            groups[br_type] = []
            branch_data = self.branch_data[br_type]
            for msg, commits_with_msg_only in branch_data.filtered_commits_by_message.items():
                groups[br_type].append(CommitGroup(br_type, set(commits_with_msg_only)))
        return groups

    def groups_by_branch_type(self, br_type: BranchType) -> List[CommitGroup]:
        return self._groups[br_type]

    def sum_len_of_groups(self, br_type: BranchType):
        return sum([g.size for g in self._groups[br_type]])

    def all_commit_hashes_in_groups(self, br_type: BranchType):
        return [ch for g in self._groups[br_type] for ch in g.commit_hashes]

    def print_group_stats(self):
        for br_type in self.branch_data.keys():
            groups = self._groups[br_type]
            preds = [lambda x: x.size == 1, lambda x: x.size == 2, lambda x: x.size > 2]
            partitioned_groups: List[List[CommitGroup]] = self._partition_multi(preds, groups)

            print_helper_dict = {
                "1 commit": partitioned_groups[0],
                "2 commits": partitioned_groups[1],
                "3 or more commits": partitioned_groups[2],
            }
            for group_type, partition_group in print_helper_dict.items():
                groups_str_list = [self._group_to_str(g, idx) for idx, g in enumerate(partition_group)]
                LOG.debug(
                    f"Listing commit groups with {group_type} on branch {br_type} ({len(partition_group)}): "
                    f"{StringUtils.list_to_multiline_string(groups_str_list)}"
                )

    @staticmethod
    def _group_to_str(group: CommitGroup, idx):
        return f"Group {idx + 1}: \n{group.as_string}"

    # TODO partition methods should be moved to pythoncommons
    @staticmethod
    def _partition(pred, iterable):
        trues = []
        falses = []
        for item in iterable:
            if pred(item):
                trues.append(item)
            else:
                falses.append(item)
        return trues, falses

    @staticmethod
    def _partition_multi(predicates, iterable) -> List[List[Any]]:
        lists = []
        for i in range(len(predicates)):
            lists.append([])

        for item in iterable:
            for idx, pred in enumerate(predicates):
                if pred(item):
                    lists[idx].append(item)
        return lists

    def sanity_check(self):
        for br_type in self.branch_data.keys():
            # This will get commits_after_merge_base_filtered from BranchData
            num_commits_on_branch = len(get_commits(self.branch_data, br_type))

            # Get all number of commits from all groups
            sum_len_groups = self.sum_len_of_groups(br_type)

            # Diff all commits on branch vs. all number of commits in groups
            # If they are the same it means all commits are added to exactly one group
            if num_commits_on_branch == sum_len_groups:
                LOG.info("Sanity check was successful")
                return

            hashes_on_branch = get_commit_hashes(self.branch_data, br_type)
            hashes_of_groups = self.all_commit_hashes_in_groups(br_type)
            message = (
                f"Number of all commits on branch vs. number of all commits in all groups "
                f"for the branch is different!\n"
                f"Number of commits on branch is: {num_commits_on_branch}\n"
                f"Number of all items in all groups: {sum_len_groups}"
            )
            LOG.error(message)

            if len(hashes_on_branch) < len(hashes_of_groups):
                # TODO think about this what could be a useful exception message here
                raise NotImplementedError(
                    "len(Commits of all groups) > len(commits on branch) sanity check is not yet implemented"
                )

            diffed_hashes = set(hashes_on_branch).difference(set(hashes_of_groups))
            commits_by_hashes = self.branch_data[br_type].get_commits_by_hashes(diffed_hashes)
            LOG.error(f"Commits that are not found amoung groups: {commits_by_hashes}")

            # Well, two big numbers like 414 vs. 410 commits doesn't give much of clarity, so let's print the
            # commit details
            LOG.debug(f"Querying commits on branch {br_type} against {len(diffed_hashes)} commit hashes..")
            filtered_commits: List[CommitData] = filter_commits_by_hashes(self.branch_data, br_type, diffed_hashes)
            commit_strs = StringUtils.list_to_multiline_string([(c.hash, c.message) for c in filtered_commits])
            LOG.error(message)
            raise ValueError(message + f"\nCommits missing from groups: \n{commit_strs})")


class UnifiedCommitGroups:
    pass
