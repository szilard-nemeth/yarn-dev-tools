# TODO Think about how to get rid of this module?
import datetime
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import List
from pythoncommons.string_utils import RegexUtils

MATCH_EXPRESSION_SEPARATOR = "::"
MATCH_EXPRESSION_PATTERN = "^([a-zA-Z]+)%s(.*)$" % MATCH_EXPRESSION_SEPARATOR
AGGREGATED_WS_POSTFIX = "aggregated"
LOG = logging.getLogger(__name__)


class SummaryMode(Enum):
    HTML = "html"
    TEXT = "text"
    ALL = "all"
    NONE = "none"


class OperationMode(Enum):
    GSHEET = "GSHEET"
    PRINT = "PRINT"


@dataclass(eq=True, frozen=True)
class MatchExpression:
    alias: str
    original_expression: str
    pattern: str


REGEX_EVERYTHING = ".*"
MATCH_ALL_LINES_EXPRESSION: MatchExpression = MatchExpression("Failed testcases", REGEX_EVERYTHING, REGEX_EVERYTHING)
MATCHTYPE_ALL_POSTFIX = "ALL"


@dataclass(eq=True, frozen=True)
class KnownTestFailureInJira:
    tc_name: str
    jira: str
    resolution_date: datetime.datetime


@dataclass
class EmailMetaData:
    message_id: str
    thread_id: str
    subject: str
    date: datetime.datetime


@dataclass
class FailedTestCase:
    full_name: str
    email_meta: EmailMetaData
    simple_name: str = None
    parameterized: bool = False
    parameter: str = None

    def __post_init__(self):
        self.simple_name = self.full_name
        match = RegexUtils.ensure_matches_pattern(self.full_name, r"(.*)\[(.*)\]$")
        if match:
            self.parameterized = True
            self.simple_name = match.group(1)
            self.parameter: str = match.group(2)
            LOG.info(
                f"Found parameterized testcase failure: {self.full_name}. "
                f"Simple testcase name: {self.simple_name}, "
                f"Parameter: {self.parameter}"
            )


@dataclass
class BuildComparisonResult:
    fixed: List[FailedTestCase]
    still_failing: List[FailedTestCase]
    new_failures: List[FailedTestCase]


@dataclass
class FailedTestCaseAggregated:
    full_name: str
    simple_name: str
    parameterized: bool
    parameter: str = None
    latest_failure: datetime.datetime or None = None
    failure_freq: int or None = None
    failure_dates: List[datetime.datetime] = field(default_factory=list)
    known_failure: bool or None = None
    reoccurred: bool or None = None  # reoccurred_failure_after_jira_resolution


@dataclass(eq=True, frozen=True)
class AggregateFilter:
    val: str or None


@dataclass(eq=True, frozen=True)
class TestCaseFilter:
    match_expr: MatchExpression
    aggr_filter: AggregateFilter or None
    aggregate: bool = False

    def short_str(self):
        return f"{self.match_expr.alias} / {self._safe_get_aggr_filter()} (aggregate: {self.aggregate})"

    def _safe_get_aggr_filter(self):
        if not self.aggr_filter:
            return "*"
        return self.aggr_filter.val


# TODO consider converting this a hashable object and drop str
def get_key_by_testcase_filter(tcf: TestCaseFilter):
    key: str = tcf.match_expr.alias.lower()
    if tcf.aggr_filter:
        key += f"_{tcf.aggr_filter.val.lower()}"
    elif tcf.aggregate:
        key += f"_{AGGREGATED_WS_POSTFIX}"
    else:
        key += f"_{MATCHTYPE_ALL_POSTFIX.lower()}"
    return key
