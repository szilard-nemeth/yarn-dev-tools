import logging
from git import Repo, RemoteProgress, GitCommandError
import os

from pythoncommons.git_utils import GitUtils

from yarndevtools.commands.reviewsync.jira_patch import HadoopJiraPatch
from yarndevtools.commands.reviewsync.patch_apply import PatchStatus, PatchApply

HADOOP_UPSTREAM_REPO_URL = "https://github.com/apache/hadoop.git"
BRANCH_PREFIX = "reviewsync"
LOG = logging.getLogger(__name__)


class GitWrapper:
    def __init__(self, base_path):
        self.base_path = base_path
        self.hadoop_repo_path = os.path.join(self.base_path, "hadoop")
        self._ensure_base_path_exists()

    def _ensure_base_path_exists(self):
        if not os.path.exists(self.base_path):
            os.mkdir(self.base_path)

    def sync_hadoop(self, fetch=True):
        if not os.path.exists(self.hadoop_repo_path):
            # Do initial clone
            LOG.info("Cloning Hadoop for the first time, into directory: %s", self.hadoop_repo_path)
            self.repo = Repo.clone_from(
                HADOOP_UPSTREAM_REPO_URL, self.hadoop_repo_path, progress=ProgressPrinter("clone")
            )
        else:
            self.repo = Repo(self.hadoop_repo_path)
            origin = self.repo.remote("origin")
            assert origin

            if fetch:
                LOG.info(
                    "Fetching changes from Hadoop repository (%s) into directory %s",
                    HADOOP_UPSTREAM_REPO_URL,
                    self.hadoop_repo_path,
                )
                for fetch_info in origin.fetch(progress=ProgressPrinter("fetch")):
                    LOG.debug("Updated %s to %s", fetch_info.ref, fetch_info.commit)

    def is_branch_exist(self, branch: str, exc_info=True):
        try:
            self.repo.git.rev_parse("--verify", branch)
            return True
        except GitCommandError:
            LOG.exception("Branch does not exist", exc_info=exc_info)
            return False

    def validate_branches(self, branches):
        if not self.repo:
            raise ValueError("Repository is not yet synced! Please invoke sync_hadoop method before this method!")
        for branch in branches:
            Repo.rev_parse(self.repo, "origin/" + branch)

    def apply_patch(self, patch):
        if not isinstance(patch, HadoopJiraPatch):
            raise ValueError("patch must be an instance of JiraPatch!")
        if not self.repo:
            raise ValueError("Repository is not yet synced! Please invoke sync_hadoop method before this method!")

        LOG.info("Applying patch %s on branches: %s", patch.filename, patch.target_branches)
        LOG.debug("Applying patch %s", patch)

        results = []
        for branch in patch.target_branches:
            patch_branch_name = "{prefix}-{branch}-{filename}".format(
                prefix=BRANCH_PREFIX, branch=branch, filename=patch.filename
            )
            target_branch = "origin/" + branch

            if not patch.is_applicable_for_branch(branch):
                LOG.warning(
                    "Patch %s is not applicable on branch %s! Reason: %s!",
                    patch,
                    branch,
                    patch.get_reason_for_non_applicability(branch),
                )
                results.append(PatchApply(patch, target_branch, PatchStatus.PATCH_ALREADY_COMMITTED))
                continue

            # If branch already exists, move it to target_branch
            if patch_branch_name in self.repo.heads:
                LOG.info(
                    "Patch branch already exists with name %s, moving branch pointer to %s",
                    patch_branch_name,
                    target_branch,
                )
                patch_branch = self.repo.heads[patch_branch_name]
                patch_branch.set_commit(target_branch)
            else:
                patch_branch = self.repo.create_head(patch_branch_name, target_branch)

            self.repo.head.reference = patch_branch
            self.cleanup()
            try:
                LOG.debug("[%s] Applying patch %s to branch: %s...", patch.issue_id, patch.filename, target_branch)
                status, stdout, stderr = self.repo.git.execute(
                    ["git", "apply", patch.file_path], with_extended_output=True
                )
                self.log_git_exec(status, stderr, stdout)
                if status == 0:
                    LOG.info(
                        "[%s] Successfully applied patch %s to branch: %s.",
                        patch.issue_id,
                        patch.filename,
                        target_branch,
                    )
                    results.append(PatchApply(patch, target_branch, PatchStatus.APPLIES_CLEANLY))
                else:
                    LOG.error("Something bad happened")
                    self.log_git_exec(status, stderr, stdout, level=logging.INFO)
            except GitCommandError as gce:
                if "patch does not apply" in gce.stderr:
                    LOG.info("[%s] Patch %s does not apply to %s!" % (patch.issue_id, patch.filename, target_branch))
                    self.log_git_exec(gce.status, gce.stderr, gce.stdout)

                    conflicts = GitUtils.get_number_of_conflicts_from_str(gce.stderr)
                    results.append(
                        PatchApply(
                            patch, target_branch, PatchStatus.CONFLICT, conflicts=conflicts, conflict_details=gce.stderr
                        )
                    )
                else:
                    results.append(PatchApply(patch, target_branch, PatchStatus.UNKNOWN_ERROR))

        return results

    def cleanup(self):
        self.repo.head.reset(index=True, working_tree=True)
        self.repo.git.clean("-xdfq")

    def log_git_exec(self, status, stderr, stdout, level=logging.DEBUG):
        if level == logging.DEBUG:
            LOG.debug("Status of git command: %s", status)
            LOG.debug("stdout of git command: %s", stdout)
            LOG.debug("stderr of git command: %s", stderr)
        else:
            LOG.info("Status of git command: %s", status)
            LOG.info("stdout of git command: %s", stdout)
            LOG.info("stderr of git command: %s", stderr)

    def get_remote_branches_committed_for_issue(self, issue_id):
        commit_hashes = self._get_commit_hashes(issue_id)
        remote_branches = self._get_remote_branches_for_commits(commit_hashes)
        return set(remote_branches)

    def _get_commit_hashes(self, issue_id):
        status, stdout, stderr = self.repo.git.execute(
            ["git", "log", "--oneline", "--all", "--grep", issue_id], with_extended_output=True
        )
        self.log_git_exec(status, stderr, stdout)
        if status != 0:
            raise ValueError("[%s] Failed to run git log command that finds a Jira issue!")
        if stdout:
            commit_hashes = []
            for line in stdout.splitlines():
                line_parts = line.split(" ")
                if len(line_parts) > 0:
                    commit_hashes.append(line_parts[0])
            return commit_hashes

        return []

    def _get_remote_branches_for_commits(self, commits, strip_remote=True):
        if commits is None:
            raise ValueError("List of commits should not be None!")

        remote_branches = []
        for commit in commits:
            status, stdout, stderr = self.repo.git.execute(
                ["git", "branch", "-r", "--contains", commit], with_extended_output=True
            )
            self.log_git_exec(status, stderr, stdout)
            if status != 0:
                raise ValueError("[%s] Failed to run git branch command that finds remote branches for commit!")
            if stdout:
                for r_branch in stdout.splitlines():
                    if len(r_branch) > 0:
                        stripped_rbranch = r_branch.lstrip()

                        if strip_remote:
                            local_branch = GitUtils.convert_remote_branch_name_to_local(r_branch)
                            remote_branches.append(local_branch)
                        else:
                            remote_branches.append(stripped_rbranch)
            else:
                return []
        return remote_branches


class ProgressPrinter(RemoteProgress):
    def __init__(self, operation):
        super(ProgressPrinter, self).__init__()
        self.operation = operation

    def update(self, op_code, cur_count, max_count=None, message=""):
        percentage = cur_count / (max_count or 100.0) * 100
        LOG.debug("Progress of git %s: %s%% (speed: %s)", self.operation, percentage, message or "-")
