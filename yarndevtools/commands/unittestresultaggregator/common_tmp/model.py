import datetime
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List

from pythoncommons.string_utils import auto_str, RegexUtils

from yarndevtools.commands.unittestresultaggregator.common import (
    LOG,
    MATCH_ALL_LINES_EXPRESSION,
    MATCH_EXPRESSION_SEPARATOR,
    MATCHTYPE_ALL_POSTFIX,
    REGEX_EVERYTHING,
)
from yarndevtools.commands.unittestresultaggregator.email.common import FailedTestCaseFromEmail


class FailedTestCaseAbs(ABC):
    @abstractmethod
    def date(self) -> datetime.datetime:
        pass

    @abstractmethod
    def full_name(self):
        pass

    @abstractmethod
    def simple_name(self):
        pass

    @abstractmethod
    def subject(self):
        # TODO yarndevtoolsv2: Email-specific abstractmethod
        pass

    @abstractmethod
    def parameter(self) -> str:
        pass

    @abstractmethod
    def parameterized(self) -> bool:
        pass


@dataclass(eq=True, frozen=True)
class MatchExpression:
    alias: str
    original_expression: str
    pattern: str


@dataclass(eq=True, frozen=True)
class KnownTestFailureInJira:
    tc_name: str
    jira: str
    resolution_date: datetime.datetime


@dataclass
class BuildComparisonResult:
    fixed: List[FailedTestCaseAbs]
    still_failing: List[FailedTestCaseAbs]
    new_failures: List[FailedTestCaseAbs]

    @staticmethod
    def create_empty():
        return BuildComparisonResult([], [], [])


@dataclass
class FailedTestCaseAggregated:
    # TODO yarndevtoolsv2: this is very similar to FailedTestCase, should use composition
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


@dataclass(eq=True, frozen=True)
class TestCaseKey:
    tc_filter: TestCaseFilter
    full_name: str
    # TODO yarndevtoolsv2: Email-specific property
    email_subject: str or None = None

    @staticmethod
    def create_from(
        tcf: TestCaseFilter,
        ftc: FailedTestCaseAbs,
        use_full_name=True,
        use_simple_name=False,
        # TODO yarndevtoolsv2: Email-specific property
        include_email_subject=True,
    ):
        if all([use_full_name, use_simple_name]) or not any([use_full_name, use_simple_name]):
            raise ValueError("Either 'use_simple_name' or 'use_full_name' should be set to True, but not both!")
        tc_name = ftc.full_name() if use_full_name else None
        tc_name = ftc.simple_name() if use_simple_name else tc_name
        # TODO yarndevtoolsv2 email specific stuff
        subject = ftc.subject if include_email_subject else None
        return TestCaseKey(tcf, tc_name, subject)


@dataclass
class TestCaseFilters:
    # TODO yarndevtoolsv2: Revisit any email specific logic in this class?
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

        # EXAMPLE SCENARIO / CONFIG:
        #   match_expression #1 = 'YARN::org.apache.hadoop.yarn', pattern='.*org\\.apache\\.hadoop\\.yarn.*')
        #   match_expression #2 = 'MR::org.apache.hadoop.mapreduce', pattern='.*org\\.apache\\.hadoop\\.mapreduce.*')
        #   Aggregation filter #1 = CDPD-7.x
        #   Aggregation filter #2 = CDPD-7.1.x

        # 3 filters: Global ALL, YARN ALL, MR ALL
        self._SIMPLE_MATCHED_LINE_FILTERS = self._get_testcase_filter_objs(
            extended_expressions=True, match_expr_separately_always=True, without_aggregates=True
        )

        # 4 filters:
        # YARN CDPD-7.1.x aggregated, YARN CDPD-7.x aggregated,
        # MR CDPD-7.1.x aggregated, MR CDPD-7.x aggregated
        self._AGGREGATION_FILTERS: List[TestCaseFilter] = self._get_testcase_filter_objs(
            extended_expressions=False, match_expr_if_no_aggr_filter=True
        )
        # 2 filters: YARN ALL aggregated, MR ALL aggregated
        self._aggregated_match_expr_filters = self._get_testcase_filter_objs(
            extended_expressions=False,
            match_expr_separately_always=True,
            aggregated_match_expressions=True,
            without_aggregates=True,
        )
        self._AGGREGATION_FILTERS += self._aggregated_match_expr_filters

        self.ALL_VALID_FILTERS = self._AGGREGATION_FILTERS + self._SIMPLE_MATCHED_LINE_FILTERS

        self.LATEST_FAILURE_FILTERS = self._get_testcase_filter_objs(
            match_expr_separately_always=False, match_expr_if_no_aggr_filter=False, without_aggregates=False
        )
        self.TESTCASES_TO_JIRAS_FILTERS = self._AGGREGATION_FILTERS
        self._print_filters()

    def _print_filters(self):
        fields = self.__dict__
        values = {f: [x for x in self.__getattribute__(f)] for f in fields if "FILTERS" in f}
        values_short = {f: [x.short_str() for x in self.__getattribute__(f)] for f in fields if "FILTERS" in f}
        LOG.info(f"Printing filters: {values}")
        LOG.info(f"Printing filters (short): {values_short}")

    @property
    def extended_match_expressions(self) -> List[MatchExpression]:
        return self.match_expressions + [MATCH_ALL_LINES_EXPRESSION]

    def _get_testcase_filter_objs(
        self,
        extended_expressions=False,
        match_expr_separately_always=False,
        match_expr_if_no_aggr_filter=False,
        without_aggregates=False,
        aggregated_match_expressions=False,
    ) -> List[TestCaseFilter]:
        match_expressions_list = self.extended_match_expressions if extended_expressions else self.match_expressions

        result: List[TestCaseFilter] = []
        for match_expr in match_expressions_list:
            if match_expr_separately_always or (match_expr_if_no_aggr_filter and not self.aggregate_filters):
                self._append_tc_filter_with_match_expr(aggregated_match_expressions, match_expr, result)

            if without_aggregates:
                continue

            # We don't need aggregate for all lines
            if match_expr != MATCH_ALL_LINES_EXPRESSION:
                for aggr_filter in self.aggregate_filters:
                    result.append(TestCaseFilter(match_expr, aggr_filter, aggregate=True))
        return result

    @staticmethod
    def _append_tc_filter_with_match_expr(aggregated_match_expressions, match_expr, result):
        aggregated = True if aggregated_match_expressions else False
        result.append(TestCaseFilter(match_expr, None, aggregate=aggregated))

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

    def get_non_aggregate_filters(self):
        return self._SIMPLE_MATCHED_LINE_FILTERS

    def get_aggregate_filters(self):
        return self._AGGREGATION_FILTERS

    def get_match_expression_aggregate_filters(self):
        return self._aggregated_match_expr_filters


@auto_str
class FailedTestCase(FailedTestCaseAbs):
    def __init__(self, full_name, simple_name=None, parameterized=False, parameter=None):
        self._full_name = full_name
        self._simple_name = simple_name
        self._parameterized = parameterized
        self._parameter = parameter
        self.__post_init__()

    def __post_init__(self):
        self._simple_name = self._full_name
        match = RegexUtils.ensure_matches_pattern(self._full_name, r"(.*)\[(.*)\]$")
        if match:
            self._parameterized = True
            self._simple_name = match.group(1)
            self._parameter: str = match.group(2)
            LOG.info(
                f"Found parameterized testcase failure: {self._full_name}. "
                f"Simple testcase name: {self._simple_name}, "
                f"Parameter: {self._parameter}"
            )

    def date(self) -> datetime.datetime:
        # TODO implement
        pass

    def full_name(self):
        return self._full_name

    def simple_name(self):
        return self._simple_name

    def subject(self):
        raise AttributeError("No subject for this testcase type!")

    def parameter(self) -> str:
        return self._parameter

    def parameterized(self) -> bool:
        return self._parameterized


class FailedTestCaseFactory:
    @staticmethod
    def create_from_email(matched_line, email_meta):
        return FailedTestCaseFromEmail(matched_line, email_meta)

    # TODO Implement create_from_xxx
