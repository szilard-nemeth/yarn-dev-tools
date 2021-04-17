import logging
from datetime import datetime
from typing import Dict, List, Any, Set, Tuple, FrozenSet

from pythoncommons.collection_utils import CollectionUtils
from pythoncommons.string_utils import StringUtils, auto_str

from yarndevtools.commands.branchcomparator.common import (
    BranchData,
    BranchType,
    MatchingResultBase,
    CommitMatchType,
    CommitMatcherBase,
)
from yarndevtools.commands.branchcomparator.common_representation import convert_commit_to_str
from yarndevtools.commands_common import CommitData

LOG = logging.getLogger(__name__)


class GroupedCommitMatcherUtils:
    @staticmethod
    def get_commits_without_jira_id(branch_data: Dict[BranchType, BranchData], br_type: BranchType):
        return branch_data[br_type].commits_with_missing_jira_id_filtered

    @staticmethod
    def get_commits(branch_data: Dict[BranchType, BranchData], br_type: BranchType):
        return branch_data[br_type].commits_after_merge_base_filtered

    @staticmethod
    def get_commit_hashes(branch_data: Dict[BranchType, BranchData], br_type: BranchType):
        return set([hash for hash in branch_data[br_type].get_commit_hashes()])

    @staticmethod
    def filter_commits_by_hashes(
        branch_data: Dict[BranchType, BranchData], br_type: BranchType, commit_hashes: Set[str]
    ) -> List[CommitData]:
        commits: List[CommitData] = GroupedCommitMatcherUtils.get_commits(branch_data, br_type)
        return list(filter(lambda c: c.hash in commit_hashes, commits))


@auto_str
class CommitGroup:
    def __init__(self, br_type: BranchType or None, commits: Set[CommitData], match_type: CommitMatchType):
        # Put commits into ascending order by date
        self.br_type = br_type
        self.commits: List[CommitData] = sorted(
            commits, key=lambda cd: datetime.strptime(cd.date, "%Y-%m-%dT%H:%M:%S%z")
        )
        self.match_type: CommitMatchType = match_type
        self.all_jira_ids = frozenset([jid for c in self.commits for jid in c.jira_id_data.all_matched_jira_ids])
        self.commits_by_jira_id: Dict[str, List[CommitData]] = self._populate_commits_by_jira_id()
        self.commit_revert_info: Dict[str, bool] = {}
        self._set_reverted_status()

    def _populate_commits_by_jira_id(self):
        result: Dict[str, List[CommitData]] = {}
        for c in self.commits:
            if c.jira_id not in result:
                result[c.jira_id] = []
            result[c.jira_id].append(c)
        return result

    def _set_reverted_status(self):
        if len(self.commits) == 0 or len(self.commits_by_jira_id) == 0:
            raise ValueError("It seems commits are not yet set to self.commits")
        for jira_id, commits in self.commits_by_jira_id.items():
            # Commits are ordered in ascending by date
            reverted = False
            for c in commits:
                if c.reverted_at_least_once:
                    LOG.debug(f"Found reverted commit: {convert_commit_to_str(c)}")
                reverted = c.reverted_at_least_once
            self.commit_revert_info[jira_id] = reverted

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

    def as_indexed_str(self, idx):
        return f"Group {idx + 1}: \n{self.as_string}"


class GroupedMatchingResult(MatchingResultBase):
    # TODO implement
    # self.after_merge_base: List[Tuple[CommitData, CommitData]] = []

    # Commits matched by message with missing Jira ID
    # self.matched_only_by_message: List[Tuple[CommitData, CommitData]] = []

    # Commits matched by Jira ID but not by message
    # self.matched_only_by_jira_id: List[Tuple[CommitData, CommitData]] = []

    # Commits matched by Jira ID and by message as well
    # self.matched_both: List[Tuple[CommitData, CommitData]] = []
    def __init__(self):
        super().__init__()
        self.matched_groups: List[Tuple[CommitGroup, CommitGroup]] = []
        self.unmatched_groups: Dict[BranchType, List[CommitGroup]] = {BranchType.MASTER: [], BranchType.FEATURE: []}
        self._matched_groups: Set[FrozenSet] = set()

    def matched_group_candidate(
        self, jira_ids_set: FrozenSet[str], master_group: CommitGroup, feature_group: CommitGroup
    ):
        # TODO add more advanced matching: Right now, 2 commit groups are matched if all the jira IDs are the same for them
        #   Here we can add commit message matching
        #   Here we can add advanced matching like considering reverts, order of commits should be same in 2 matched groups, etc.
        self.matched_groups.append((master_group, feature_group))
        self._matched_groups.add(jira_ids_set)

    def finalize(self, all_groups: Dict[BranchType, Dict[FrozenSet, CommitGroup]]):
        for br_type in all_groups.keys():
            all_groups_for_branch = all_groups[br_type]
            unmatched_keys = frozenset(all_groups_for_branch.keys()).difference(frozenset(self._matched_groups))
            self.unmatched_groups[br_type] = [all_groups_for_branch[k] for k in unmatched_keys]


class GroupedCommitMatcherSummaryData:
    def __init__(self, config, branches):
        super().__init__(config, branches)
        # TODO implement

    def common_commits_after_merge_base(self):
        # TODO implement
        # return [c[0] for c in self.common_commits.after_merge_base]
        pass

    def add_stats_matched_commit_details(self, res):
        # res += "\n\n=====Stats: COMMON COMMITS ACROSS BRANCHES=====\n"
        # res += (
        #     f"Number of common commits with missing Jira ID, matched by commit message: "
        #     f"{len(self.matching_result.matched_only_by_message)}\n"
        # )
        # res += (
        #     f"Number of common commits with matching Jira ID but different commit message: "
        #     f"{len(self.matching_result.matched_only_by_jira_id)}\n"
        # )
        # res += (
        #     f"Number of common commits with matching Jira ID and commit message: "
        #     f"{len(self.matching_result.matched_both)}\n"
        # )
        # return res
        # TODO implement
        pass

    def add_stats_matched_commits_on_branches(self, res):
        # res += "\n\n=====Stats: COMMON=====\n"
        # res += f"Merge-base commit: {self.branches.merge_base.as_oneline_string(incl_date=True)}\n"
        # res += f"Number of common commits before merge-base: {len(self.common_commits.before_merge_base)}\n"
        # res += f"Number of common commits after merge-base: {len(self.common_commits.after_merge_base)}\n"
        # return res
        # TODO implement
        pass


class GroupedCommitMatcher(CommitMatcherBase):
    # TODO Add algorithm documentation
    def __init__(self, branch_data: Dict[BranchType, BranchData]):
        self.branch_data = branch_data
        self.matching_result: GroupedMatchingResult or None = None

    def create_matching_result(self) -> GroupedMatchingResult:
        self.matching_result = GroupedMatchingResult()
        return self.matching_result

    def match_commits(self) -> Any:
        self.jira_id_to_commits: JiraIdToCommitMappings = JiraIdToCommitMappings(self.branch_data)
        self.commit_grouper: CommitGrouper = CommitGrouper(self.branch_data, self.jira_id_to_commits)
        self.matching_result: GroupedMatchingResult = self.match_based_on_groups()

    def match_based_on_groups(self) -> GroupedMatchingResult:
        # TODO make commit group objects ordered by date if they are not yet ordered
        # TODO print groups that has 2 or more jira IDS (also print to file)
        # TODO Start a second-pass that tries to group jira-id based groups with commit message groups?
        # It can happen that a commit message group has the same commit message like already existing commits in groups, with jira ids
        master_groups: Dict[FrozenSet, CommitGroup] = self.commit_grouper.groups_by_jira_id_dict()[BranchType.MASTER]
        feature_groups: Dict[FrozenSet, CommitGroup] = self.commit_grouper.groups_by_jira_id_dict()[BranchType.FEATURE]
        LOG.info(
            f"Matching commit groups. "
            f"Found master groups: {len(master_groups)}"
            f"Found feature groups: {len(feature_groups)}"
        )

        result: GroupedMatchingResult = GroupedMatchingResult()
        for jira_ids_set, m_group in master_groups.items():
            if jira_ids_set in feature_groups:
                f_group = feature_groups[jira_ids_set]
                result.matched_group_candidate(jira_ids_set, m_group, f_group)
        result.finalize(self.commit_grouper.groups_by_jira_id_dict())
        return result


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
            for commit in GroupedCommitMatcherUtils.get_commits(self.branch_data, br_data.type):
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
        self._groups_by_jira_id: Dict[BranchType, List[CommitGroup]] = self._create_groups()
        self._groups_by_msg: Dict[BranchType, List[CommitGroup]] = self._create_groups_by_message()
        self.sanity_check()
        self.print_group_stats()

    def groups_by_jira_id_dict(self) -> Dict[BranchType, Dict[FrozenSet, CommitGroup]]:
        result: Dict[BranchType, Dict[FrozenSet, CommitGroup]] = {}
        for br_type, br_data in self.branch_data.items():
            result[br_type] = {}
            curr_res: Dict[FrozenSet, CommitGroup] = result[br_type]
            for group in self._groups_by_jira_id[br_type]:
                if group.all_jira_ids in curr_res:
                    raise ValueError(
                        "Found groups with same set of jira IDs on the same branch, this should never happen."
                        f"Branch: {br_type}"
                        f"Group 1: {group}"
                        f"Group 2: {curr_res[group.all_jira_ids]}"
                    )
                curr_res[group.all_jira_ids] = group

            # Sanity check
            if len(self._groups_by_jira_id[br_type]) != len(result[br_type]):
                raise ValueError(
                    "Length of original groups and resulted group dict is not the same! "
                    f"Length of original groups: {len(self._groups_by_jira_id[br_type])} "
                    f"Length of new grouping: {len(result[br_type])}"
                )
        return result

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
                    groups[br_type].append(CommitGroup(br_type, grouped_commits, CommitMatchType.MATCHED_BY_ID))
                    visited_commit_hashes.update([c.hash for c in grouped_commits])
        return groups

    def _create_groups_by_message(self) -> Dict[BranchType, List[CommitGroup]]:
        groups: Dict[BranchType, List[CommitGroup]] = {}
        for br_type in self.branch_data.keys():
            groups[br_type] = []
            branch_data = self.branch_data[br_type]
            for msg, commits_with_msg_only in branch_data.filtered_commits_by_message.items():
                groups[br_type].append(
                    CommitGroup(br_type, set(commits_with_msg_only), CommitMatchType.MATCHED_BY_MESSAGE)
                )
        return groups

    def groups_by_branch_type(self, br_type: BranchType) -> List[CommitGroup]:
        return self._groups_by_jira_id[br_type]

    def sum_len_of_groups(self, br_type: BranchType):
        return sum([g.size for g in self._groups_by_jira_id[br_type]]) + sum(
            [g.size for g in self._groups_by_msg[br_type]]
        )

    def all_commit_hashes_in_groups(self, br_type: BranchType):
        hashes_1 = [ch for g in self._groups_by_jira_id[br_type] for ch in g.commit_hashes]
        hashes_2 = [ch for g in self._groups_by_msg[br_type] for ch in g.commit_hashes]
        return hashes_1 + hashes_2

    def print_group_stats(self):
        for br_type in self.branch_data.keys():
            self._print_group_stats_internal(br_type, self._groups_by_jira_id[br_type], "jira id based")
            self._print_group_stats_internal(br_type, self._groups_by_msg[br_type], "commit message based")

    def _print_group_stats_internal(self, br_type: BranchType, groups: List[CommitGroup], type_of_group: str):
        # TODO consider printing this as a grid / html table
        predicates = [lambda x: x.size == 1, lambda x: x.size == 2, lambda x: x.size > 2]
        partitioned_groups: List[List[CommitGroup]] = CollectionUtils.partition_multi(predicates, groups)
        print_helper_dict = {
            "1 commit": partitioned_groups[0],
            "2 commits": partitioned_groups[1],
            "3 or more commits": partitioned_groups[2],
        }
        for cardinality, partition_group in print_helper_dict.items():
            groups_str_list = [g.as_indexed_str(idx) for idx, g in enumerate(partition_group)]
            LOG.debug(
                f"Listing {type_of_group} commit groups with {cardinality} "
                f"on branch {br_type} (# of groups: {len(partition_group)}): \n"
                f"{StringUtils.list_to_multiline_string(groups_str_list)}"
            )

    def sanity_check(self):
        for br_type in self.branch_data.keys():
            # This will get commits_after_merge_base_filtered from BranchData
            num_commits_on_branch = len(GroupedCommitMatcherUtils.get_commits(self.branch_data, br_type))

            # Get all number of commits from all groups
            sum_len_groups = self.sum_len_of_groups(br_type)

            # Diff all commits on branch vs. all number of commits in groups
            # If they are the same it means all commits are added to exactly one group
            if num_commits_on_branch == sum_len_groups:
                LOG.info("Sanity check was successful")
                return

            hashes_on_branch = GroupedCommitMatcherUtils.get_commit_hashes(self.branch_data, br_type)
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
            filtered_commits: List[CommitData] = GroupedCommitMatcherUtils.filter_commits_by_hashes(
                self.branch_data, br_type, diffed_hashes
            )
            commit_strs = StringUtils.list_to_multiline_string([(c.hash, c.message) for c in filtered_commits])
            LOG.error(message)
            raise ValueError(message + f"\nCommits missing from groups: \n{commit_strs})")
