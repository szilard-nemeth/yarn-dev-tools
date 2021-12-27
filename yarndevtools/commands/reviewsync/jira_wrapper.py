from typing import Dict, List

import re
import logging

from pythoncommons.jira_wrapper import JiraWrapper, PatchApplicability, AdvancedJiraPatch

LOG = logging.getLogger(__name__)

DEFAULT_PATCH_EXTENSION = "patch"
TRUNK_PATCH_FILENAME_PATTERN_TEMPLATE = r"^(\w+-\d+)$SEPCHAR(\d+)\." + DEFAULT_PATCH_EXTENSION + "$"


class HadoopJiraWrapper(JiraWrapper):
    def __init__(self, jira_url, default_branch, patches_root, git_wrapper):
        super().__init__(jira_url, default_branch, patches_root)
        self.git_wrapper = git_wrapper

    def get_patches_per_branch(self, issue_id, additional_branches, committed_on_branches):
        issue = self.get_jira_issue(issue_id)
        if not issue:
            LOG.error("No Jira issue found for Jira ID: %s", issue)
            return []
        owner = self.determine_patch_owner(issue)
        patches = self._get_patch_objects(issue, issue_id, owner, committed_on_branches)
        # After this call, we have on 1-1 mappings between patch and branch
        branches_to_patches = self._map_patches_to_branches(issue_id, patches)
        branch_to_patch_dict = self._get_latest_patches_per_branch(branches_to_patches)
        LOG.info("[%s] Found patches (only latest by filename): %s", issue_id, branches_to_patches)

        # Sanity check: self.default_branch patch is present for the Jira issue
        if self.default_branch not in branch_to_patch_dict:
            LOG.error(
                "[%s] Patch targeted to default branch (name: '%s') should be present for each issue, "
                "however trunk patch is not present for this issue!",
                issue_id,
                self.default_branch,
            )
            return []

        self._map_patches_for_additional_branches(
            additional_branches, branch_to_patch_dict, committed_on_branches, issue_id
        )
        LOG.debug("Found patches from all issues, only latest and after overrides applied: %s", branches_to_patches)
        patches = self._deduplicate_patches(branch_to_patch_dict)
        return patches

    def _map_patches_for_additional_branches(
        self, additional_branches, branch_to_patch_dict, committed_on_branches, issue_id
    ):
        for branch in additional_branches:
            # If we don't have patch for this branch, use the same patch targeted to the default branch.
            # Otherwise, keeping the one that explicitly targets the branch is in precedence.
            # In other words, we should make an override: A Patch explicitly targeted to
            # a branch should have precedence over a patch originally targeted to the default branch.
            # Example:
            # Branch: branch-3.2
            # Trunk patch: 002.patch
            # Result: branches_to_patches = { 'trunk': '002.patch', 'branch-3.2': '002.patch' }

            # Example2:
            # Patches: 002.patch, branch-3.2.001.patch
            # Branches: trunk, branch-3.2
            # Result: 002.patch --> trunk, branch-3.2.001.patch --> branch-3.2 [[002.patch does not target branch-3.2]]
            branch_required = branch not in committed_on_branches
            if not branch_required:
                LOG.info(
                    "[%s] Patch should be targeted to additional branch %s, but it is already committed on that branch!",
                    issue_id,
                    branch,
                )
            if branch_required and branch not in branch_to_patch_dict:
                patch = branch_to_patch_dict[self.default_branch]
                patch.add_additional_branch(branch, PatchApplicability(True, explicit=False))
                branch_to_patch_dict[branch] = patch

    @staticmethod
    def _map_patches_to_branches(issue_id, patches) -> Dict[str, List[str]]:
        # key: branch name, value: list of JiraPatch objects
        branches_to_patches: Dict[str, List[str]] = {}
        for patch in patches:
            # Sanity check
            if not len(patch.target_branches) == 1:
                raise ValueError(
                    "Patch should be targeted to "
                    "only one branch at this point. Patch: {}, Branches: {}".format(
                        patch.filename, patch.target_branches
                    )
                )
            branch = patch.target_branches[0]
            if branch not in branches_to_patches:
                branches_to_patches[branch] = []
            branches_to_patches[branch].append(patch)
        LOG.debug("[%s] Found patches (grouped by branch): %s", issue_id, branches_to_patches)
        return branches_to_patches

    def _get_patch_objects(self, issue, issue_id, owner, committed_on_branches):
        attachments = issue.fields.attachment
        patches = [self.create_jira_patch_obj(issue_id, a.filename, owner, committed_on_branches) for a in attachments]
        patches = [p for p in patches if p is not None]
        LOG.debug("[%s] Found patches (all): %s", issue_id, patches)
        return patches

    @staticmethod
    def _deduplicate_patches(branch_to_patch_dict):
        # We could also have duplicates at this point,
        # the combination of sets and __eq__ method of JiraPatch will sort out duplicates
        dedup_patches = set()
        for branch, patch in branch_to_patch_dict.items():
            dedup_patches.add(patch)
        dedup_patches = list(dedup_patches)
        LOG.info("Found patches from all issues, after all filters applied: %s", dedup_patches)
        return dedup_patches

    def create_jira_patch_obj(self, issue_id, filename, owner, committed_on_branches):
        sep_char = self._get_separator_char_from_patch_filename(filename)
        if not sep_char:
            LOG.error(
                "[%s] Filename %s does not seem to have separator character after Jira issue ID!", issue_id, filename
            )
            return None

        pattern = TRUNK_PATCH_FILENAME_PATTERN_TEMPLATE.replace("$SEPCHAR", re.escape(sep_char))
        trunk_search_obj = re.search(pattern, filename)

        # First, let's suppose that we have a patch file targeted to trunk
        # Example filename: YARN-9213.003.patch
        if trunk_search_obj:
            return self._create_patch_object_from_trunk_filename(
                trunk_search_obj, issue_id, filename, owner, committed_on_branches
            )
        else:
            # Trunk filename pattern did not match.
            # Try to match against pattern that has other branch than trunk.
            # Examples:
            # YARN-9213.branch-3.2.004.patch
            # YARN-9139.branch-3.1.001.patch
            # YARN-9213.branch3.2.001.patch
            # YARN-9573.001.branch-3.1.patch
            return self._create_patch_object_from_other_branch_filename(
                sep_char, issue_id, filename, owner, committed_on_branches
            )

    @staticmethod
    def _get_separator_char_from_patch_filename(filename):
        search_obj = re.search(r"\w+-\d+(.)", filename)
        if search_obj and len(search_obj.groups()) == 1:
            return search_obj.group(1)
        return None

    @staticmethod
    def _get_latest_patches_per_branch(patches_dict):
        # Sort patches in descending order, i.e. [004, 003, 002, 001]
        for branch_name in patches_dict:
            patches_dict[branch_name].sort(key=lambda patch: patch.version, reverse=True)

        patches_per_branch = {}
        for branch, patches in patches_dict.items():
            # We know that this is ordered by patch version, DESC
            if len(patches) == 0:
                raise ValueError("Expected at least one target branch for patch: " + str(patches))
            patches_per_branch[branch] = patches[0]

        return patches_per_branch

    def _create_patch_object_from_trunk_filename(
        self, trunk_search_obj, issue_id, filename, owner, committed_on_branches
    ):
        if len(trunk_search_obj.groups()) == 2:
            parsed_issue_id = trunk_search_obj.group(1)
            parsed_version = trunk_search_obj.group(2)
            LOG.debug(
                "Parsed jira details for issue %s: filename: %s, issue id: %s, version: %s",
                issue_id,
                filename,
                parsed_issue_id,
                parsed_version,
            )

            if parsed_issue_id != issue_id:
                raise ValueError(
                    "Parsed issue id {} does not match original issue id {}!".format(parsed_issue_id, issue_id)
                )

            if self.default_branch not in committed_on_branches:
                applicability = PatchApplicability(True)
            else:
                applicability = PatchApplicability(False, "Patch already committed on {}".format(self.default_branch))
            return AdvancedJiraPatch(
                parsed_issue_id, owner, parsed_version, self.default_branch, filename, applicability
            )
        else:
            raise ValueError("Filename {} does not match for trunk branch pattern, ".format(filename))

    def _create_patch_object_from_other_branch_filename(
        self, sep_char, issue_id, filename, owner, committed_on_branches
    ):
        search_obj = re.search(
            r"(\w+-\d+)"
            + re.escape(sep_char)
            + r"([a-zA-Z\-0-9.]+)"
            + re.escape(sep_char)
            + r"(\d+)\."
            + DEFAULT_PATCH_EXTENSION
            + "$",
            filename,
        )
        if search_obj and len(search_obj.groups()) == 3:
            parsed_issue_id = search_obj.group(1)
            parsed_branch = search_obj.group(2)
            parsed_version = search_obj.group(3)

            LOG.debug(
                "Parsed jira details for issue %s: filename:%s, issue id: %s, branch: %s, version: %s",
                issue_id,
                filename,
                parsed_issue_id,
                parsed_branch,
                parsed_version,
            )

            branch_exist = self.git_wrapper.is_branch_exist(parsed_branch)
            if not branch_exist:
                LOG.error(
                    "Branch does not exist: %s. Please validate if attachment filename is correct, filename: %s",
                    parsed_branch,
                    filename,
                )
                return None
            if parsed_branch not in committed_on_branches:
                applicability = PatchApplicability(True)
            else:
                applicability = PatchApplicability(False, "Patch already committed on {}".format(parsed_branch))
            return AdvancedJiraPatch(parsed_issue_id, owner, parsed_version, parsed_branch, filename, applicability)
        else:
            LOG.error("[%s] Filename %s does not match for any patch file name regex pattern!", issue_id, filename)
            return None
