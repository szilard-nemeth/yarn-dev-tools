import logging

from pythoncommons.jira_wrapper import JiraPatch, PatchOverallStatus

LOG = logging.getLogger(__name__)


class HadoopJiraPatch(JiraPatch):
    def __init__(self, issue_id, owner, version, target_branch, patch_file, applicability):
        super(HadoopJiraPatch, self).__init__(issue_id, owner, patch_file)
        self.issue_id = issue_id
        # TODO owner and owner_short are currently not queried anywhere except __str__
        self.version = version
        self.target_branches = [target_branch]
        self.applicability = {target_branch: applicability}
        self.overall_status = PatchOverallStatus("N/A")

    def get_applicability(self, branch):
        return self.applicability[branch]

    def add_additional_branch(self, branch, applicability):
        self.target_branches.append(branch)
        self.applicability[branch] = applicability

    def is_applicable_for_branch(self, branch):
        if branch in self.applicability:
            return self.applicability[branch].applicable
        return False

    def get_reason_for_non_applicability(self, branch):
        if branch in self.applicability:
            return self.applicability[branch].reason
        return "Unknown"

    def is_applicable(self):
        applicabilities = set([True if a.applicable else False for a in self.applicability.values()])
        LOG.debug("Patch applicabilities: %s for patch %s", applicabilities, self)
        return True in applicabilities

    # TODO verify these
    def __repr__(self):
        return super(HadoopJiraPatch, self).__repr__() + repr((self.version, self.target_branches))

    # TODO verify these
    def __str__(self):
        return super().__str__() + "version: " + str(self.version) + ", target_branch: " + str(self.target_branches)

    def __hash__(self):
        return hash((self.issue_id, self.owner, self.filename, tuple(self.target_branches)))

    def __eq__(self, other):
        if isinstance(other, HadoopJiraPatch):
            return super().__eq__(self, other) and self.target_branches == other.target_branches
        return False
