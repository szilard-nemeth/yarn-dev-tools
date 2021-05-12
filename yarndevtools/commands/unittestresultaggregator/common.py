from dataclasses import dataclass, field

# TODO Think about how to get rid of this module?
import datetime
from enum import Enum
from typing import List

MATCH_EXPRESSION_SEPARATOR = "::"
MATCH_EXPRESSION_PATTERN = "^([a-zA-Z]+)%s(.*)$" % MATCH_EXPRESSION_SEPARATOR


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


@dataclass
class MatchedLinesFromMessage:
    message_id: str
    thread_id: str
    subject: str
    date: datetime.datetime
    lines: List[str] = field(default_factory=list)


@dataclass(eq=True, frozen=True)
class AggregateFilter:
    val: str


@dataclass(eq=True, frozen=True)
class TestCaseFilter:
    match_expr: MatchExpression
    aggr_filter: AggregateFilter or None


@dataclass
class TestCaseFilters:
    match_expressions: List[MatchExpression]
    aggregate_filters: List[AggregateFilter]

    def __post_init__(self):
        if not all([isinstance(af, str) or isinstance(af, AggregateFilter) for af in self.aggregate_filters]):
            raise ValueError(f"Mixed instances in self.aggregate_filters: {self.aggregate_filters}")

        tmp_list: List[AggregateFilter] = []
        for aggr_filter in self.aggregate_filters:
            if isinstance(aggr_filter, str):
                tmp_list.append(AggregateFilter(aggr_filter))

        if tmp_list:
            self.aggregate_filters = tmp_list

    @property
    def extended_match_expressions(self) -> List[MatchExpression]:
        return self.match_expressions + [MATCH_ALL_LINES_EXPRESSION]

    def get_testcase_filter_objs(
        self, extended_expressions=False, match_expr_separately_always=False, match_expr_if_no_aggr_filter=False
    ) -> List[TestCaseFilter]:
        result: List[TestCaseFilter] = []
        match_expressions_list = self.extended_match_expressions if extended_expressions else self.match_expressions
        for match_expr in match_expressions_list:
            if match_expr_separately_always:
                result.append(TestCaseFilter(match_expr, None))
            elif match_expr_if_no_aggr_filter and not self.aggregate_filters:
                result.append(TestCaseFilter(match_expr, None))

            # We don't need aggregate for all lines
            if match_expr != MATCH_ALL_LINES_EXPRESSION:
                for aggr_filter in self.aggregate_filters:
                    result.append(TestCaseFilter(match_expr, aggr_filter))
        return result

    def match_all_lines(self) -> bool:
        return len(self.match_expressions) == 1 and self.match_expressions[0] == MATCH_ALL_LINES_EXPRESSION

    @staticmethod
    def convert_raw_match_expressions_to_objs(raw_match_exprs: List[str]) -> List[MatchExpression]:
        if not raw_match_exprs:
            return [MATCH_ALL_LINES_EXPRESSION]

        match_expressions: List[MatchExpression] = []
        for raw_match_expr in raw_match_exprs:
            segments = raw_match_expr.split(MATCH_EXPRESSION_SEPARATOR)
            alias = segments[0]
            if alias == MATCHTYPE_ALL_POSTFIX:
                raise ValueError(
                    f"Alias for match expression '{MATCHTYPE_ALL_POSTFIX}' is reserved. Please use another alias."
                )
            match_expr = segments[1]
            pattern = REGEX_EVERYTHING + match_expr.replace(".", "\\.") + REGEX_EVERYTHING
            match_expressions.append(MatchExpression(alias, raw_match_expr, pattern))
        return match_expressions


# TODO consider converting this a hashable object and drop str
def get_key_by_testcase_filter(tcf: TestCaseFilter):
    key: str = tcf.match_expr.alias.lower()
    if tcf.aggr_filter:
        key += f"_{tcf.aggr_filter.val.lower()}"
    else:
        key += f"_{MATCHTYPE_ALL_POSTFIX.lower()}"
    return key
