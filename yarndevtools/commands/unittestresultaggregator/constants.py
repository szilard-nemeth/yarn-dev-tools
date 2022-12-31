from dataclasses import dataclass
from enum import Enum


class OperationMode(Enum):
    GSHEET = "GSHEET"
    PRINT = "PRINT"


class SummaryMode(Enum):
    HTML = "html"
    TEXT = "text"
    ALL = "all"
    NONE = "none"


class ExecutionMode(Enum):
    DB_ONLY = "db_only"
    EMAIL_ONLY = "email_only"
    DB_AND_EMAIL = "db_and_email"
    DB_AND_EMAIL_FORCED = "db_and_email_forced"


MATCH_EXPRESSION_SEPARATOR = "::"
MATCH_EXPRESSION_PATTERN = "^([a-zA-Z]+)%s(.*)$" % MATCH_EXPRESSION_SEPARATOR
AGGREGATED_WS_POSTFIX = "aggregated"
VALID_OPERATION_MODES = [OperationMode.PRINT, OperationMode.GSHEET]
REGEX_EVERYTHING = ".*"


@dataclass(eq=True, frozen=True)
class MatchExpression:
    alias: str
    original_expression: str
    pattern: str


MATCH_ALL_LINES_EXPRESSION: MatchExpression = MatchExpression("Failed testcases", REGEX_EVERYTHING, REGEX_EVERYTHING)
MATCHTYPE_ALL_POSTFIX = "ALL"
