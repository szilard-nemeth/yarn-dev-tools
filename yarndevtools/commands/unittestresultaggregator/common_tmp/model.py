import datetime
from abc import ABC, abstractmethod
from collections import UserDict
from dataclasses import dataclass, field
from typing import List, Dict

from pythoncommons.string_utils import auto_str, RegexUtils

from yarndevtools.commands.unittestresultaggregator.common import (
    MATCH_EXPRESSION_SEPARATOR,
    AGGREGATED_WS_POSTFIX,
    REGEX_EVERYTHING,
    MATCH_ALL_LINES_EXPRESSION,
    MATCHTYPE_ALL_POSTFIX,
    MatchExpression,
)
import logging

LOG = logging.getLogger(__name__)


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


@dataclass
class BuildComparisonResult:
    fixed: List[FailedTestCaseAbs]
    still_failing: List[FailedTestCaseAbs]
    new: List[FailedTestCaseAbs]

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

    def __post_init__(self):
        super().__setattr__("_key", self.generate_key())

    def key(self):
        return super().__getattribute__("_key")

    def short_str(self):
        return f"{self.match_expr.alias} / {self._safe_get_aggr_filter()} (aggregate: {self.aggregate})"

    def _safe_get_aggr_filter(self):
        if not self.aggr_filter:
            return "*"
        return self.aggr_filter.val

    def generate_key(self):
        # TODO consider converting this a hashable object and drop str
        key: str = self.match_expr.alias.lower()
        if self.aggr_filter:
            key += f"_{self.aggr_filter.val.lower()}"
        elif self.aggregate:
            key += f"_{AGGREGATED_WS_POSTFIX}"
        else:
            key += f"_{MATCHTYPE_ALL_POSTFIX.lower()}"
        return key


@dataclass(eq=True, frozen=True)
class TestCaseKey:
    tc_filter: TestCaseFilter
    full_name: str
    # TODO yarndevtoolsv2: Email-specific properties throughout class
    email_subject: str or None = None

    @staticmethod
    def create_from(
        tcf: TestCaseFilter,
        ftc: FailedTestCaseAbs,
        use_full_name=True,
        use_simple_name=False,
        include_email_subject=True,
    ):
        if all([use_full_name, use_simple_name]) or not any([use_full_name, use_simple_name]):
            raise ValueError("Either 'use_simple_name' or 'use_full_name' should be set to True, but not both!")
        tc_name = ftc.full_name() if use_full_name else None
        tc_name = ftc.simple_name() if use_simple_name else tc_name
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


class TestFailuresByFilters(UserDict):
    def __init__(self, all_filters):
        super().__init__()
        self.data: Dict[TestCaseFilter, List[FailedTestCaseAbs]] = {}
        self._testcase_cache: Dict[TestCaseKey, FailedTestCaseAbs] = {}

        for tcf in all_filters:
            if tcf not in self.data:
                self.data[tcf] = []

    def __getitem__(self, tcf):
        return self.data[tcf]

    def get_filters(self):
        return self.data.keys()

    def add(self, tcf, failed_testcase):
        tc_key = TestCaseKey.create_from(
            tcf,
            failed_testcase,
            use_full_name=True,
            use_simple_name=False,
            include_email_subject=True,
        )
        if tc_key in self._testcase_cache:
            stored_testcase = self._testcase_cache[tc_key]
            # TODO printout seems to be wrong
            LOG.debug(
                f"Found already existing testcase key: {tc_key}. "
                f"Value: {stored_testcase}, "
                f"Email data (stored): {stored_testcase.subject()} "
                f"Email data (new): {stored_testcase.subject()}"
            )
            return
        else:
            self._testcase_cache[tc_key] = failed_testcase

        self.data[tcf].append(failed_testcase)


class FinalAggregationResults:
    # TODO yarndevtoolsv2: Revisit any email specific logic in this class
    def __init__(self, all_filters: List[TestCaseFilter]):
        self.test_failures = TestFailuresByFilters(all_filters)
        # TODO yarndevtoolsv2: specify types? (caused cyclic import issues previously)
        self._aggregated = None
        self._comparison = None
        self._latest_failures = None
        self._known_failure_checker = None

    def add_failure(self, tcf: TestCaseFilter, failed_testcase: FailedTestCaseAbs):
        self.test_failures.add(tcf, failed_testcase)

    def get_failure(self, tcf) -> List[FailedTestCaseAbs]:
        return self.test_failures[tcf]

    def get_latest_testcases(self, tcf) -> List[FailedTestCaseAbs]:
        return self._latest_failures[tcf]

    def get_build_comparison_results(self, tcf) -> BuildComparisonResult:
        return self._comparison[tcf]

    def get_aggregated_testcases(self, tcf) -> List[FailedTestCaseAggregated]:
        return self._aggregated[tcf]

    def print_keys(self):
        LOG.debug(f"Keys of _failed_testcases_by_filter: {self.test_failures.get_filters()}")


@dataclass
class EmailMetaData:
    message_id: str
    thread_id: str
    subject: str
    date: datetime.datetime


@auto_str
class FailedTestCaseFromEmail(FailedTestCase):
    def __init__(self, full_name, email_meta: EmailMetaData):
        super().__init__(full_name)
        self.email_meta: EmailMetaData = email_meta

    def date(self) -> datetime.datetime:
        return self.email_meta.date

    def subject(self):
        return self.email_meta.subject
