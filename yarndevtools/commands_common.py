from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from re import Pattern
from typing import List, Any, Set, Tuple

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


class JiraIdParseStrategy(ABC):
    @abstractmethod
    def parse(self, git_log_line: str, config) -> str or None:
        pass


class MatchFirstJiraIdParseStrategy(JiraIdParseStrategy):
    def parse(self, git_log_line: str, config) -> Tuple[str, List[str]] or None:
        match = config.pattern.search(git_log_line)
        if match:
            return [match.group(1)]
        return []


class GitLogParseConfig:
    def __init__(
        self,
        log_format: GitLogLineFormat = GitLogLineFormat.ONELINE_WITH_DATE,
        pattern: Pattern = YARN_JIRA_ID_PATTERN,
        allow_unmatched_jira_id: bool = False,
        author: str = None,
        print_unique_jira_projects: bool = False,
        commit_field_separator: str = COMMIT_FIELD_SEPARATOR,
        jira_id_parse_strategy: JiraIdParseStrategy = None,
        keep_parser_state: bool = False,
    ):
        self.jira_id_parse_strategy = jira_id_parse_strategy
        if not self.jira_id_parse_strategy:
            self.jira_id_parse_strategy = MatchFirstJiraIdParseStrategy()
        self.log_format = log_format
        self.pattern = pattern
        self.print_unique_jira_projects = print_unique_jira_projects
        self.allow_unmatched_jira_id = allow_unmatched_jira_id
        self.author = author
        self.commit_field_separator = commit_field_separator
        # TODO implement using this property and serialize parser state to json for future reference + add to zip
        self.keep_parser_state = keep_parser_state


@dataclass
class CommitParserState:
    git_log_line_raw: str
    all_fields: List[str]
    found_jira_ids: List[str]
    commit_data_obj: Any


@dataclass
class GitLogParserState:
    unique_jira_projects: Set[str] = field(default_factory=set)
    commit_states: List[CommitParserState] = field(default_factory=list)


class GitLogParser:
    def __init__(self, config: GitLogParseConfig):
        self.config = config
        self.keep_state = config.keep_parser_state
        self.state = None
        if self.keep_state:
            self.state = GitLogParserState()

    def parse_line(self, git_log_line: str):
        fields = git_log_line.split(self.config.commit_field_separator)
        jira_id, jira_ids = GitLogParser._determine_jira_ids(git_log_line, self.config)
        reverted, reverted_at_least_once = self._determine_if_reverted(git_log_line)

        # Alternatively, commit date and author may be gathered with git show,
        # but this requires more CLI calls, so it's not the preferred way.
        # commit_date = self.upstream_repo.show(commit_hash, no_patch=True, no_notes=True, pretty='%cI')
        # commit_author = self.upstream_repo.show(commit_hash, suppress_diff=True, format="%ae"))

        # Hash is always at first place
        commit_hash = fields[0]
        author = self.config.author
        if self.config.log_format == GitLogLineFormat.ONELINE_WITH_DATE:
            # Example: 'ceab00b0db84455da145e0545fe9be63b270b315
            #  COMPX-3264. Fix QueueMetrics#containerAskToCount map synchronization issues 2021-03-22T02:18:52-07:00'
            message = COMMIT_FIELD_SEPARATOR.join(fields[1:-1])
            date = fields[-1]  # date is the last item
        elif self.config.log_format == GitLogLineFormat.ONELINE_WITH_DATE_AND_AUTHOR:
            # Example: 'ceab00b0db84455da145e0545fe9be63b270b315
            #  COMPX-3264. Fix QueueMetrics#containerAskToCount map synchronization issues 2021-03-22T02:18:52-07:00
            #    snemeth@cloudera.com'
            message = COMMIT_FIELD_SEPARATOR.join(fields[1:-2])
            date = fields[-2]  # date is the 2nd to last
            author = fields[-1]  # author is the last item
        else:
            raise ValueError(f"Unrecognized format value: {self.config.log_format}")

        commit_data = CommitData(
            commit_hash,
            jira_id,
            message,
            date,
            reverted=reverted,
            reverted_at_least_once=reverted_at_least_once,
            author=author,
        )

        if self.keep_state:
            commit_state = CommitParserState(git_log_line, fields, jira_ids, commit_data)
            self.state.commit_states.append(commit_state)
            self.state.unique_jira_projects.update(
                [GitLogParser._get_jira_project_from_jira_id(jid) for jid in jira_ids]
            )

        return commit_data

    @staticmethod
    def _determine_jira_ids(git_log_str, parse_config: GitLogParseConfig) -> Tuple[str or None, List[str]]:
        jira_ids = parse_config.jira_id_parse_strategy.parse(git_log_str, config=parse_config)
        if not jira_ids and not parse_config.allow_unmatched_jira_id:
            raise ValueError(
                f"Cannot find YARN jira id in git log string: {git_log_str}. "
                f"Pattern was: {CommitData.JIRA_ID_PATTERN.pattern}"
            )
        if not jira_ids:
            return None, []
        else:
            return jira_ids[0], jira_ids

    @staticmethod
    def _determine_if_reverted(git_log_line):
        revert_count = git_log_line.upper().count(REVERT.upper())
        final_reverted = False
        if revert_count % 2 == 1:
            final_reverted = True
        return final_reverted, revert_count > 0

    @staticmethod
    def _get_jira_project_from_jira_id(jira_id):
        if "-" not in jira_id:
            raise ValueError(f"Unexpected jira id: {jira_id}")
        return jira_id.split("-")[0]


@auto_str
class CommitData:
    def __init__(
        self, c_hash, jira_id, message, date, branches=None, reverted=False, reverted_at_least_once=False, author=None
    ):
        self.hash = c_hash
        self.jira_id = jira_id
        self.message = message
        self.date = date
        self.branches = branches
        self.reverted = reverted
        self.author = author
        self.reverted_at_least_once = reverted_at_least_once

    @staticmethod
    def from_git_log_output(git_log_output: List[str], parse_config: GitLogParseConfig) -> List[Any]:
        parser = GitLogParser(parse_config)
        result: List[CommitData] = []
        for commit_str in git_log_output:
            result.append(parser.parse_line(commit_str))
        return result

    # TODO make another method that can work with full git log results, not just a line of it
    @staticmethod
    def from_git_log_str(
        git_log_str: str,
        format: GitLogLineFormat = GitLogLineFormat.ONELINE_WITH_DATE,
        pattern: Pattern = YARN_JIRA_ID_PATTERN,
        allow_unmatched_jira_id: bool = False,
        author: str = None,
        jira_id_parse_strategy: JiraIdParseStrategy = None,
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
            log_format=format,
            pattern=pattern,
            allow_unmatched_jira_id=allow_unmatched_jira_id,
            author=author,
            print_unique_jira_projects=True,
            jira_id_parse_strategy=jira_id_parse_strategy,
            keep_parser_state=True,
        )
        parser = GitLogParser(parse_config)
        return parser.parse_line(git_log_str)

    def as_oneline_string(self) -> str:
        return f"{self.hash} {self.message}"
