import logging
from enum import Enum

from yarndevtools.commands.unittestresultaggregator.common_tmp.model import (
    MatchExpression,
    TestCaseFilter,
)

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


VALID_OPERATION_MODES = [OperationMode.PRINT, OperationMode.GSHEET]

REGEX_EVERYTHING = ".*"
MATCH_ALL_LINES_EXPRESSION: MatchExpression = MatchExpression("Failed testcases", REGEX_EVERYTHING, REGEX_EVERYTHING)
MATCHTYPE_ALL_POSTFIX = "ALL"


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
