from enum import Enum

from pythoncommons.git_constants import (
    COMMIT_FIELD_SEPARATOR,
    REVERT,
)
from pythoncommons.string_utils import auto_str
from yarndevtools.constants import (
    YARN_JIRA_ID_PATTERN,
)


class GitLogLineFormat(Enum):
    ONELINE_WITH_DATE = 0
    ONELINE_WITH_DATE_AND_AUTHOR = 1


@auto_str
class CommitData:
    def __init__(self, c_hash, jira_id, message, date, branches=None, reverted=False, author=None):
        self.hash = c_hash
        self.jira_id = jira_id
        self.message = message
        self.date = date
        self.branches = branches
        self.reverted = reverted
        self.author = author

    @staticmethod
    def from_git_log_str(
        git_log_str,
        format: GitLogLineFormat = None,
        pattern=YARN_JIRA_ID_PATTERN,
        allow_unmatched_jira_id=False,
        author=None,
    ):
        """
        1. Commit hash: It is in the first column.
        2. Jira ID: Expecting the Jira ID to be the first segment of commit message, so this is the second column.
        3. Commit message: From first to (last - 1) th index
        4. Authored date (commit date): The very last segment is the commit date.
        :param git_log_str:
        :return:
        """
        if not format:
            format = GitLogLineFormat.ONELINE_WITH_DATE
        comps = git_log_str.split(COMMIT_FIELD_SEPARATOR)
        match = pattern.search(git_log_str)

        jira_id = None
        if not match:
            if not allow_unmatched_jira_id:
                raise ValueError(
                    f"Cannot find YARN jira id in git log string: {git_log_str}. "
                    f"Pattern was: {CommitData.JIRA_ID_PATTERN.pattern}"
                )
        else:
            jira_id = match.group(1)

        revert_count = git_log_str.upper().count(REVERT.upper())
        reverted = False
        if revert_count % 2 == 1:
            reverted = True

        # Alternatively, commit date and author may be gathered with git show,
        # but this requires more CLI calls, so it's not the preferred way.
        # commit_date = self.upstream_repo.show(commit_hash, no_patch=True, no_notes=True, pretty='%cI')
        # commit_author = self.upstream_repo.show(commit_hash, suppress_diff=True, format="%ae"))

        c_hash = comps[0]
        if format == GitLogLineFormat.ONELINE_WITH_DATE:
            # Example: 'ceab00b0db84455da145e0545fe9be63b270b315
            #  COMPX-3264. Fix QueueMetrics#containerAskToCount map synchronization issues 2021-03-22T02:18:52-07:00'
            message = COMMIT_FIELD_SEPARATOR.join(comps[1:-1])
            date = comps[-1]
        elif format == GitLogLineFormat.ONELINE_WITH_DATE_AND_AUTHOR:
            # Example: 'ceab00b0db84455da145e0545fe9be63b270b315
            #  COMPX-3264. Fix QueueMetrics#containerAskToCount map synchronization issues 2021-03-22T02:18:52-07:00
            #    snemeth@cloudera.com'
            message = COMMIT_FIELD_SEPARATOR.join(comps[1:-2])
            date = comps[-2]
            author = comps[-1]
        else:
            raise ValueError(f"Unrecognized format value: {format}")
        return CommitData(c_hash=c_hash, jira_id=jira_id, message=message, date=date, reverted=reverted, author=author)

    def as_oneline_string(self) -> str:
        return f"{self.hash} {self.message}"
