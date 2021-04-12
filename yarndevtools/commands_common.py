from enum import Enum
from re import Pattern

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


class GitLogParseConfig:
    def __init__(
        self,
        log_format: GitLogLineFormat = GitLogLineFormat.ONELINE_WITH_DATE,
        pattern: Pattern = YARN_JIRA_ID_PATTERN,
        allow_unmatched_jira_id: bool = False,
        author: str = None,
        print_unique_jira_projects: bool = False,
        commit_field_separator: str = COMMIT_FIELD_SEPARATOR,
    ):
        self.log_format = log_format
        self.pattern = pattern
        self.print_unique_jira_projects = print_unique_jira_projects
        self.allow_unmatched_jira_id = allow_unmatched_jira_id
        self.author = author
        self.commit_field_separator = commit_field_separator


class GitLogParser:
    def __init__(self, config: GitLogParseConfig, keep_state: bool = False):
        self.config = config
        self.keep_state = keep_state

    def parse_line(self, git_log_line: str):
        comps = git_log_line.split(self.config.commit_field_separator)
        jira_id = GitLogParser._determine_jira_id(git_log_line, self.config)
        reverted = self._determine_if_reverted(git_log_line)

        # Alternatively, commit date and author may be gathered with git show,
        # but this requires more CLI calls, so it's not the preferred way.
        # commit_date = self.upstream_repo.show(commit_hash, no_patch=True, no_notes=True, pretty='%cI')
        # commit_author = self.upstream_repo.show(commit_hash, suppress_diff=True, format="%ae"))

        # Hash is always at first place
        commit_hash = comps[0]
        author = self.config.author
        if self.config.log_format == GitLogLineFormat.ONELINE_WITH_DATE:
            # Example: 'ceab00b0db84455da145e0545fe9be63b270b315
            #  COMPX-3264. Fix QueueMetrics#containerAskToCount map synchronization issues 2021-03-22T02:18:52-07:00'
            message = COMMIT_FIELD_SEPARATOR.join(comps[1:-1])
            date = comps[-1]  # date is the last item
        elif self.config.log_format == GitLogLineFormat.ONELINE_WITH_DATE_AND_AUTHOR:
            # Example: 'ceab00b0db84455da145e0545fe9be63b270b315
            #  COMPX-3264. Fix QueueMetrics#containerAskToCount map synchronization issues 2021-03-22T02:18:52-07:00
            #    snemeth@cloudera.com'
            message = COMMIT_FIELD_SEPARATOR.join(comps[1:-2])
            date = comps[-2]  # date is the 2nd to last
            author = comps[-1]  # author is the last item
        else:
            raise ValueError(f"Unrecognized format value: {self.config.log_format}")
        return CommitData(commit_hash, jira_id, message, date, reverted=reverted, author=author)

    @staticmethod
    def _determine_jira_id(git_log_str, parse_config: GitLogParseConfig):
        match = parse_config.pattern.search(git_log_str)
        jira_id = None
        if not match:
            if not parse_config.allow_unmatched_jira_id:
                raise ValueError(
                    f"Cannot find YARN jira id in git log string: {git_log_str}. "
                    f"Pattern was: {CommitData.JIRA_ID_PATTERN.pattern}"
                )
        else:
            jira_id = match.group(1)
        return jira_id

    @staticmethod
    def _determine_if_reverted(git_log_line):
        revert_count = git_log_line.upper().count(REVERT.upper())
        reverted = False
        if revert_count % 2 == 1:
            reverted = True
        return reverted


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

    # TODO make another method that can work with full git log results, not just a line of it
    @staticmethod
    def from_git_log_str(
        git_log_str: str,
        format: GitLogLineFormat = GitLogLineFormat.ONELINE_WITH_DATE,
        pattern: Pattern = YARN_JIRA_ID_PATTERN,
        allow_unmatched_jira_id: bool = False,
        author: str = None,
    ):
        """
        1. Commit hash: It is in the first column.
        2. Jira ID: Expecting the Jira ID to be the first segment of commit message, so this is the second column.
        3. Commit message: From first to (last - 1) th index
        4. Authored date (commit date): The very last segment is the commit date.
        :param git_log_str:
        :return:
        """

        # TODO Signature can be modified later if all usages migrated to use GitLogParseConfig object as input
        parse_config = GitLogParseConfig(
            format, pattern, allow_unmatched_jira_id, author, print_unique_jira_projects=True
        )
        parser = GitLogParser(parse_config, keep_state=True)
        return parser.parse_line(git_log_str)

    def as_oneline_string(self) -> str:
        return f"{self.hash} {self.message}"
