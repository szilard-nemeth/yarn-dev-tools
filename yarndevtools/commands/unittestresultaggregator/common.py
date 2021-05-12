from dataclasses import dataclass, field

# TODO Think about how to get rid of this module?
import datetime
from enum import Enum
from typing import List


class SummaryMode(Enum):
    HTML = "html"
    TEXT = "text"
    ALL = "all"
    NONE = "none"


class OperationMode(Enum):
    GSHEET = "GSHEET"
    PRINT = "PRINT"


@dataclass
class MatchExpression:
    alias: str
    original_expression: str
    pattern: str


REGEX_EVERYTHING = ".*"
MATCH_ALL_LINES_EXPRESSION = MatchExpression("Failed testcases", REGEX_EVERYTHING, REGEX_EVERYTHING)
MATCHTYPE_ALL_POSTFIX = "ALL"


@dataclass
class MatchedLinesFromMessage:
    message_id: str
    thread_id: str
    subject: str
    date: datetime.datetime
    lines: List[str] = field(default_factory=list)


# TODO consider converting this a hashable object and drop str
def get_key_by_match_expr_and_aggr_filter(match_expr, aggr_filter=None):
    key = match_expr.alias.lower()
    if aggr_filter:
        key += f"_{aggr_filter.lower()}"
    else:
        key += f"_{MATCHTYPE_ALL_POSTFIX.lower()}"
    return key
