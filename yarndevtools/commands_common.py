import argparse
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from re import Pattern
from typing import List, Any, Set, Dict

from pythoncommons.git_constants import (
    COMMIT_FIELD_SEPARATOR,
    REVERT,
)
from pythoncommons.git_wrapper import GitLogLineFormat
from pythoncommons.string_utils import auto_str

from yarndevtools.commands.unittestresultaggregator.common import MATCH_EXPRESSION_PATTERN
from yarndevtools.constants import (
    YARN_JIRA_ID_PATTERN,
)

LOG = logging.getLogger(__name__)


class JiraIdTypePreference(Enum):
    UPSTREAM = "upstream"
    DOWNSTREAM = "downstream"


class JiraIdChoosePreference(Enum):
    FIRST = "first"
    LAST = "last"


class CommandAbs(ABC):
    def __init__(self):
        pass

    @staticmethod
    @abstractmethod
    def create_parser(subparsers):
        pass

    @staticmethod
    @abstractmethod
    def execute(args, parser=None):
        pass


class GSheetArguments:
    @staticmethod
    def add_gsheet_arguments(parser):
        # Arguments for Google sheet integration
        gsheet_group = parser.add_argument_group("google-sheet", "Arguments for Google sheet integration")

        gsheet_group.add_argument(
            "--gsheet-client-secret",
            dest="gsheet_client_secret",
            required=False,
            help="Client credentials for accessing Google Sheet API",
        )

        gsheet_group.add_argument(
            "--gsheet-spreadsheet",
            dest="gsheet_spreadsheet",
            required=False,
            help="Name of the Google Sheet spreadsheet",
        )

        gsheet_group.add_argument(
            "--gsheet-worksheet",
            dest="gsheet_worksheet",
            required=False,
            help="Name of the worksheet in the Google Sheet spreadsheet",
        )
        return gsheet_group


class EmailArguments:
    @staticmethod
    def add_email_arguments(parser, add_subject=True, add_attachment_filename=True):
        # TODO Use dash notation instead of underscore
        parser.add_argument("--smtp_server", required=True, type=str, help="SMPT server")
        parser.add_argument("--smtp_port", required=True, type=str, help="SMTP port")
        parser.add_argument("--account_user", required=True, type=str, help="Email account's user")
        parser.add_argument("--account_password", required=True, type=str, help="Email account's password")
        if add_subject:
            parser.add_argument("--subject", required=True, type=str, help="Subject of the email")
        parser.add_argument("--sender", required=True, type=str, help="Sender of the email [From]")
        parser.add_argument("--recipients", required=True, type=str, nargs="+", help="List of email recipients [To]")
        if add_attachment_filename:
            parser.add_argument("--attachment-filename", required=False, type=str, help="Override attachment filename")


class ArgumentParserUtils:
    @staticmethod
    def matches_match_expression_pattern(value):
        if not re.match(MATCH_EXPRESSION_PATTERN, value):
            raise argparse.ArgumentTypeError(
                f"Must conform to this format: <alias>::<pattern>. Provided value: {value}"
            )
        return value


@dataclass
class JiraIdData:
    chosen: str or None
    _all_matched: Dict[str, Any]

    DOWNSTREAM_KEY = "downstream"
    UPSTREAM_KEY = "upstream"
    POS_KEY = "positions"

    @staticmethod
    def create_jira_id_dict(truth_list: List[bool], matches: List[str]):
        if len(truth_list) != len(matches):
            raise ValueError("Truth list and matches should have the same length!" f"Printing args: {locals()}")
        jira_id_dict = {JiraIdData.UPSTREAM_KEY: [], JiraIdData.DOWNSTREAM_KEY: [], JiraIdData.POS_KEY: {}}

        for idx, t in enumerate(truth_list):
            key = JiraIdData.DOWNSTREAM_KEY
            if t:
                # True value means upstream jira id
                key = JiraIdData.UPSTREAM_KEY
            match = matches[idx]
            jira_id_dict[key].append(match)
            jira_id_dict[JiraIdData.POS_KEY][match] = idx
        return jira_id_dict

    @property
    def all_matched_jira_ids(self) -> List[Any]:
        if self.DOWNSTREAM_KEY not in self._all_matched and self.UPSTREAM_KEY not in self._all_matched:
            return []
        return self._all_matched[self.DOWNSTREAM_KEY] + self._all_matched[self.UPSTREAM_KEY]

    @property
    def has_matched_jira_id(self):
        return len(self.all_matched_jira_ids) > 0


class JiraIdParseStrategy(ABC):
    @abstractmethod
    def parse(self, git_log_line: str, config, parser) -> JiraIdData:
        pass

    @abstractmethod
    def allow_unknown_jira_id(self) -> bool:
        pass


class MatchFirstJiraIdParseStrategy(JiraIdParseStrategy):
    def parse(self, git_log_line: str, config, parser) -> JiraIdData:
        match = config.pattern.search(git_log_line)
        if match:
            match_value = match.group(1)
            jira_id_dict = JiraIdData.create_jira_id_dict([True], [match_value])
            return JiraIdData(match_value, jira_id_dict)
        return JiraIdData(None, {})

    def allow_unknown_jira_id(self) -> bool:
        return False


class MatchJiraIdFromBeginningParseStrategy(JiraIdParseStrategy):
    def parse(self, git_log_line: str, config, parser) -> JiraIdData:
        # Do a little trick here
        # Example log line:
        # 843f66f4dc012dff402bfebc183d46673cd47419 Clean up checkstyle warnings from YARN-11024/10907/10929. Contributed by Benjamin Teke 2021-12-14T22:00:46+01:00
        # Cut the commit hash and check if position of match is starting from 0th index of the string
        mod_git_log_line = " ".join(git_log_line.split()[1:])
        match = config.pattern.search(mod_git_log_line)
        if match:
            if not match.span()[0] == 0:
                LOG.warning(
                    "Cannot find Jira ID at the beginning of the commit message. "
                    "Git log line: %s\n match span: %s, match details: %s",
                    git_log_line,
                    match.span(),
                    match,
                )
                return JiraIdData(None, {})
            match_value = match.group(1)
            jira_id_dict = JiraIdData.create_jira_id_dict([True], [match_value])
            return JiraIdData(match_value, jira_id_dict)
        return JiraIdData(None, {})

    def allow_unknown_jira_id(self) -> bool:
        return True


class MatchAllJiraIdStrategy(JiraIdParseStrategy):
    UPSTREAM_JIRA_PROJECTS = ["HADOOP", "HBASE", "HDFS", "MAPREDUCE", "YARN"]
    UPSTREAM_JIRA_PROJECTS_TUP = tuple(UPSTREAM_JIRA_PROJECTS)
    # TODO make lower/upper comparison

    def __init__(
        self,
        type_preference: JiraIdTypePreference,
        choose_preference: JiraIdChoosePreference,
        fallback_type: JiraIdTypePreference = None,
    ):
        self.type_preference = type_preference
        self.choose_preference = choose_preference
        self.fallback_type = fallback_type
        if self.fallback_type and self.fallback_type == self.type_preference:
            raise ValueError(
                f"Fallback type '{self.fallback_type}' "
                f"should not be the same as type preference '{self.type_preference}'"
            )

    def parse(self, git_log_line: str, config, parser) -> JiraIdData:
        matches = config.pattern.findall(git_log_line)
        if not matches:
            parser.add_violation(git_log_line, "No jira ID match")
            return JiraIdData(None, {})
        truth_list = self._get_upstream_jira_id_truth_list(matches)
        true_count = sum(truth_list)
        jira_id_dict = JiraIdData.create_jira_id_dict(truth_list, matches)

        if self.type_preference == JiraIdTypePreference.UPSTREAM:
            if not any(truth_list):
                if self.fallback_type:
                    parser.add_violation(git_log_line, "No upstream jira ID found but fallback type is set")
                    idx = self.index_of_first(truth_list, lambda x: not x)
                    return JiraIdData(matches[idx], jira_id_dict)
                else:
                    parser.add_violation(git_log_line, "No upstream jira ID found")
                    return JiraIdData(None, jira_id_dict)
            if self.choose_preference == JiraIdChoosePreference.LAST and true_count == 1:
                LOG.warning(
                    f"Choose preference is {self.choose_preference.value} "
                    f"but only one jira ID match found for log line: {git_log_line}"
                )
            if self.choose_preference == JiraIdChoosePreference.FIRST:
                idx = self.index_of_first(truth_list, lambda x: x)
                return JiraIdData(matches[idx], jira_id_dict)
            elif self.choose_preference == JiraIdChoosePreference.LAST:
                idx = self.index_of_first(reversed(truth_list), lambda x: x)
                return JiraIdData(matches[idx], jira_id_dict)

        if self.type_preference == JiraIdTypePreference.DOWNSTREAM:
            pass
            # TODO Implement downstream

    def allow_unknown_jira_id(self) -> bool:
        return False

    def _get_upstream_jira_id_truth_list(self, jira_ids):
        return [jid.startswith(self.UPSTREAM_JIRA_PROJECTS_TUP) for jid in jira_ids]

    @staticmethod
    def index_of_first(lst, pred):
        for i, v in enumerate(lst):
            if pred(v):
                return i
        return None


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
        verbose_mode: bool = False,
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
        self.verbose_mode = verbose_mode


@dataclass
class CommitParserState:
    git_log_line_raw: str
    all_fields: List[str]
    jira_id_data: JiraIdData
    commit_data_obj: Any


@dataclass
class GitLogParserState:
    unique_jira_projects: Set[str] = field(default_factory=set)
    commit_states: List[CommitParserState] = field(default_factory=list)


class GitLogParser:
    def __init__(self, config: GitLogParseConfig):
        self.config = config
        self.violations: Dict[str, List[str]] = {}
        self.keep_state = config.keep_parser_state
        self.state = None
        if self.keep_state:
            self.state = GitLogParserState()

    def add_violation(self, git_log_line: str, description: str):
        if description not in self.violations:
            self.violations[description] = []
        self.violations[description].append(git_log_line)

    def parse_line(self, git_log_line: str):
        fields = git_log_line.split(self.config.commit_field_separator)
        jira_id_data = self._determine_jira_ids(git_log_line, self.config)
        reverted, reverted_at_least_once = self._determine_if_reverted(git_log_line)

        # Alternatively, commit date and author may be gathered with git show,
        # but this requires more CLI calls, so it's not the preferred way.
        # commit_date = self.upstream_repo.show(commit_hash, no_patch=True, no_notes=True, pretty='%cI')
        # commit_author = self.upstream_repo.show(commit_hash, suppress_diff=True, format="%ae"))

        # Hash is always at first place
        commit_hash = fields[0]
        author = self.config.author
        committer = None
        if self.config.log_format == GitLogLineFormat.ONELINE_WITH_DATE:
            # Fields: <hash> <commit message> <date>
            # Example: 'ceab00b0db84455da145e0545fe9be63b270b315
            #  COMPX-3264. Fix QueueMetrics#containerAskToCount map synchronization issues 2021-03-22T02:18:52-07:00'
            message = COMMIT_FIELD_SEPARATOR.join(fields[1:-1])
            date = fields[-1]  # date is the last item
        elif self.config.log_format == GitLogLineFormat.ONELINE_WITH_DATE_AND_AUTHOR:
            # Fields: <hash> <commit message> <date> <author>
            # Example: 'ceab00b0db84455da145e0545fe9be63b270b315
            #  COMPX-3264. Fix QueueMetrics#containerAskToCount map synchronization issues 2021-03-22T02:18:52-07:00
            #    snemeth@cloudera.com'
            message = COMMIT_FIELD_SEPARATOR.join(fields[1:-2])
            date = fields[-2]  # date is the 2nd to last
            author = fields[-1]  # author is the last item

        elif self.config.log_format == GitLogLineFormat.ONELINE_WITH_DATE_AUTHOR_COMMITTER:
            # Fields: <hash> <commit message> <date> <author> <committer>
            # Example: 'ceab00b0db84455da145e0545fe9be63b270b315
            #  COMPX-3264. Fix QueueMetrics#containerAskToCount map synchronization issues 2021-03-22T02:18:52-07:00
            #    snemeth@cloudera.com' 'pbacsko@cloudera.com'
            message = COMMIT_FIELD_SEPARATOR.join(fields[1:-3])
            date = fields[-3]
            author = fields[-2]
            committer = fields[-1]
        else:
            raise ValueError(f"Unrecognized format value: {self.config.log_format}")

        commit_data = CommitData(
            commit_hash,
            jira_id_data.chosen,
            message,
            date,
            reverted=reverted,
            reverted_at_least_once=reverted_at_least_once,
            author=author,
            committer=committer,
            jira_id_data=jira_id_data,
        )

        if self.keep_state:
            commit_state = CommitParserState(git_log_line, fields, jira_id_data, commit_data)
            self.state.commit_states.append(commit_state)
            self.state.unique_jira_projects.update(
                [GitLogParser._get_jira_project_from_jira_id(jid) for jid in jira_id_data.all_matched_jira_ids]
            )

        return commit_data

    def _determine_jira_ids(self, git_log_str, parse_config: GitLogParseConfig) -> JiraIdData:
        allow_errors = parse_config.jira_id_parse_strategy.allow_unknown_jira_id()
        jira_id_data = parse_config.jira_id_parse_strategy.parse(git_log_str, parse_config, self)
        if not jira_id_data.has_matched_jira_id and not parse_config.allow_unmatched_jira_id and not allow_errors:
            raise ValueError(
                f"Cannot find Jira ID in git log string: {git_log_str}. " f"Pattern was: {parse_config.pattern}"
            )
        return jira_id_data

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

    def log_violations(self):
        sum_violations = sum([len(self.violations[k]) for k in self.violations.keys()])
        LOG.error("Found all violations: %d", sum_violations)
        LOG.error(f"Found {len(self.violations)} kinds of parser violations: {set(self.violations.keys())}")
        for type, commits in self.violations.items():
            LOG.error(f"Found {len(self.violations[type])} violations for: {type}")
            if self.config.verbose_mode:
                LOG.debug(f"Listing violated commits: {commits}")


@auto_str
class CommitData:
    def __init__(
        self,
        c_hash,
        jira_id,
        message,
        date,
        branches=None,
        reverted=False,
        reverted_at_least_once=False,
        author=None,
        committer=None,
        jira_id_data: JiraIdData = None,
    ):
        self.hash = c_hash
        self.jira_id: str = jira_id
        self.message: str = message
        self.date: str = date
        self.branches = branches
        self.reverted: bool = reverted
        self.author: str = author
        self.committer: str = committer
        self.reverted_at_least_once: bool = reverted_at_least_once
        self.jira_id_data: JiraIdData = jira_id_data

    @staticmethod
    def from_git_log_output(git_log_output: List[str], parse_config: GitLogParseConfig) -> List[Any]:
        parser = GitLogParser(parse_config)
        result: List[CommitData] = []
        for commit_str in git_log_output:
            result.append(parser.parse_line(commit_str))

        parser.log_violations()
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

        # TODO Make this method smart to check the known log formats and parse all the values accordingly
        #  so that format parameter is not needed anymore
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

    def as_oneline_string(self, incl_date=False, incl_author=False, incl_committer=False) -> str:
        result_str = ""
        if incl_date:
            result_str += f"{self.date} "
        if incl_author:
            result_str += f"{self.author} "
        if incl_committer:
            result_str += f"{self.committer} "
        result_str += f"{self.hash} {self.message}"
        return result_str

    def __eq__(self, other: object) -> bool:
        if isinstance(other, CommitData):
            return self.hash == other.hash and self.jira_id == other.jira_id
        return False

    def __hash__(self) -> int:
        return hash(self.hash)


@auto_str
class BackportedJira:
    def __init__(self, jira_id, commits):
        self.jira_id: str = jira_id
        self.commits: List[BackportedCommit] = commits
        self.commitdata_by_hash: Dict[str, BackportedCommit] = self._commitdata_by_hashes(commits)

    @staticmethod
    def _commitdata_by_hashes(backported_commits):
        res = {}
        for backported_commit in backported_commits:
            commit = backported_commit.commit_obj
            if commit.hash not in res:
                res[commit.hash] = backported_commit
        return res

    def add_backported_commit(self, backported_commit):
        self.commits.append(backported_commit)
        self.commitdata_by_hash[backported_commit.commit_obj.hash] = backported_commit

    def extend_branches_by_hash(self, hash, backported_commit):
        self.commitdata_by_hash[hash].branches.extend(backported_commit.branches)


@dataclass
class BackportedCommit:
    def __init__(self, commit_obj, branches):
        self.commit_obj = commit_obj
        self.branches = branches

    def __eq__(self, other):
        if isinstance(other, BackportedCommit):
            return self.commit_obj == other.commit_obj
        return False

    def __hash__(self) -> int:
        return hash(hash(self.commit_obj) + hash(self.branches))
