import datetime
import logging
from abc import ABC, abstractmethod
from collections import UserDict
from copy import copy
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict

from pythoncommons.string_utils import auto_str, RegexUtils

from yarndevtools.commands.unittestresultaggregator.constants import (
    MATCH_EXPRESSION_SEPARATOR,
    AGGREGATED_WS_POSTFIX,
    REGEX_EVERYTHING,
    MATCH_ALL_LINES_EXPRESSION,
    MATCHTYPE_ALL_POSTFIX,
    MatchExpression,
)
from yarndevtools.commands.unittestresultaggregator.db.model import EmailContent
from yarndevtools.common.common_model import JobBuildData

LOG = logging.getLogger(__name__)


@auto_str
class FailedBuildAbs(ABC):
    def __init__(self, failed_testcases: List[str]):
        stripped_failed_testcases = list(map(lambda line: line.strip(), failed_testcases))
        self._failed_testcases = stripped_failed_testcases

    @classmethod
    def create_from_email(cls, email_content: EmailContent):
        return FailedBuildFromEmail(email_content)

    @classmethod
    def create_from_job_build_data(cls, job_build_data: JobBuildData):
        return FailedBuildFromDbJobBuildData(job_build_data)

    def failed_testcases(self) -> List[str]:
        return self._failed_testcases

    def filter_testcases(self, skip_lines_starting_with: List[str]):
        pass

    @abstractmethod
    def build_url(self) -> str:
        pass

    @abstractmethod
    def job_name(self) -> str:
        pass

    @abstractmethod
    def build_number(self) -> str:
        pass

    @abstractmethod
    def origin(self):
        pass

    @abstractmethod
    def date(self) -> datetime.datetime:
        pass

    def short_str(self):
        return f"Job: {self.job_name()}\n" f"build number: {self.build_number()}"


class FailedBuildFromDbJobBuildData(FailedBuildAbs):
    def __init__(self, job_build_data: JobBuildData):
        super().__init__(job_build_data.failed_testcases)
        self._job_build_data: JobBuildData = job_build_data

    def build_url(self) -> str:
        return self._job_build_data.build_url

    def job_name(self) -> str:
        return self._job_build_data.job_name

    def build_number(self) -> str:
        return self._job_build_data.build_number

    def origin(self):
        return "failed jenkins build"

    def date(self) -> datetime.datetime:
        return self._job_build_data.build_datetime


class FailedBuildFromEmail(FailedBuildAbs):
    def __init__(self, email_content: EmailContent):
        super().__init__(email_content.lines)
        self._email_content: EmailContent = email_content

    def filter_testcases(self, skip_lines_starting_with: List[str]):
        filtered_lines = []
        for testcase in self._failed_testcases:
            if self._check_if_line_is_valid(testcase, skip_lines_starting_with):
                filtered_lines.append(testcase)
            else:
                LOG.trace(f"Skipping invalid line: {testcase} [Mail subject: {self.origin()}]")
        self._failed_testcases = filtered_lines

    @staticmethod
    def _check_if_line_is_valid(line, skip_lines_starting_with):
        for skip_str in skip_lines_starting_with:
            if line.startswith(skip_str):
                return False
        return True

    def build_url(self) -> str:
        return self._email_content.build_url

    def job_name(self) -> str:
        return self._email_content.job_name

    def build_number(self) -> str:
        return self._email_content.build_number

    def origin(self):
        return self._email_content.subject

    def date(self) -> datetime.datetime:
        return self._email_content.date


class AggregatedFailurePropertyFilter(Enum):
    UNKNOWN = ("unknown", "known_failure", True)
    REOCCURRED = ("reoccurred", "reoccurred", False)

    def __init__(self, real_name: str, property_name: str, inverted: bool):
        self.real_name = real_name
        self.property_name = property_name
        self.inverted = inverted


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
    def origin(self):
        pass

    @abstractmethod
    def parameter(self) -> str:
        pass

    @abstractmethod
    def parameterized(self) -> bool:
        pass

    @abstractmethod
    def build_url(self) -> str:
        pass

    @abstractmethod
    def job_name(self) -> str:
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


@dataclass(eq=True, unsafe_hash=True)
class TestCaseFilter:
    match_expr: MatchExpression
    aggr_filter: AggregateFilter or None
    aggregate: bool = False

    def __post_init__(self):
        super().__setattr__("_key", self._generate_key())
        if not self.aggregate and self.aggr_filter:
            LOG.warning(
                "Testcase filter is not set to aggregate but filter has an aggregate filter. Setting it to aggregate=True. Printing original filter: %s",
                self,
            )
            self.aggregate = True

    def key(self):
        return super().__getattribute__("_key")

    def short_str(self):
        return f"{self.match_expr.alias} / {self._safe_get_aggr_filter()} (aggregate: {self.aggregate})"

    def _safe_get_aggr_filter(self):
        if not self.aggr_filter:
            return "*"
        return self.aggr_filter.val

    def _generate_key(self):
        if self.match_expr == MATCH_ALL_LINES_EXPRESSION:
            return MATCHTYPE_ALL_POSTFIX + f"_{AGGREGATED_WS_POSTFIX}" if self.aggregate else MATCHTYPE_ALL_POSTFIX

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
    origin: str or None = None

    @staticmethod
    def create_from(
        tcf: TestCaseFilter,
        ftc: FailedTestCaseAbs,
        use_full_name=True,
        use_simple_name=False,
        include_origin=True,
    ):
        if all([use_full_name, use_simple_name]) or not any([use_full_name, use_simple_name]):
            raise ValueError("Either 'use_simple_name' or 'use_full_name' should be set to True, but not both!")
        tc_name = ftc.full_name() if use_full_name else None
        tc_name = ftc.simple_name() if use_simple_name else tc_name
        origin = ftc.origin() if include_origin else None
        return TestCaseKey(tcf, tc_name, origin)


class TestCaseFilters:
    def __init__(self, filters):
        self._filters: List[TestCaseFilter] = filters
        self._index = 0

    @staticmethod
    def create_empty():
        return TestCaseFilters([])

    def add(self, f):
        self._filters.append(f)

    def __add__(self, other):
        res = TestCaseFilters(copy(self._filters))
        res._filters.extend(other._filters)
        return res

    def __len__(self):
        return len(self._filters)

    def __iter__(self):
        self._index = 0
        return self

    def __next__(self):
        if self._index == len(self._filters):
            raise StopIteration
        result = self._filters[self._index]
        self._index += 1
        return result


@dataclass
class TestCaseFilterDefinitions:
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
        self._SIMPLE_MATCHED_LINE_FILTERS: TestCaseFilters = self._get_testcase_filter_objs(
            extended_expressions=True, match_expr_separately_always=True, without_aggregates=True
        )

        # 4 filters:
        # YARN CDPD-7.1.x aggregated, YARN CDPD-7.x aggregated,
        # MR CDPD-7.1.x aggregated, MR CDPD-7.x aggregated
        self._AGGREGATION_FILTERS: TestCaseFilters = self._get_testcase_filter_objs(
            extended_expressions=False, match_expr_if_no_aggr_filter=True
        )
        # 2 filters: YARN ALL aggregated, MR ALL aggregated
        self._aggregated_match_expr_filters: TestCaseFilters = self._get_testcase_filter_objs(
            extended_expressions=False,
            match_expr_separately_always=True,
            aggregated_match_expressions=True,
            without_aggregates=True,
        )
        self._AGGREGATION_FILTERS += self._aggregated_match_expr_filters

        self.ALL_VALID_FILTERS: TestCaseFilters = self._AGGREGATION_FILTERS + self._SIMPLE_MATCHED_LINE_FILTERS

        self.LATEST_FAILURE_FILTERS: TestCaseFilters = self._get_testcase_filter_objs(
            match_expr_separately_always=False, match_expr_if_no_aggr_filter=False, without_aggregates=False
        )
        self.TESTCASES_TO_JIRAS_FILTERS: TestCaseFilters = self._AGGREGATION_FILTERS
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
    ) -> TestCaseFilters:
        match_expressions_list = self.extended_match_expressions if extended_expressions else self.match_expressions

        filters = TestCaseFilters.create_empty()
        for match_expr in match_expressions_list:
            if match_expr_separately_always or (match_expr_if_no_aggr_filter and not self.aggregate_filters):
                filters.add(
                    TestCaseFilter(match_expr, None, aggregate=(True if aggregated_match_expressions else False))
                )

            if without_aggregates:
                continue

            # We don't need aggregate for all lines
            if match_expr != MATCH_ALL_LINES_EXPRESSION:
                for aggr_filter in self.aggregate_filters:
                    filters.add(TestCaseFilter(match_expr, aggr_filter, aggregate=True))
        return filters

    def match_all_lines(self) -> bool:
        return len(self.match_expressions) == 1 and self.match_expressions[0] == MATCH_ALL_LINES_EXPRESSION

    @staticmethod
    def convert_raw_match_expressions_to_objs(raw_expressions: List[str]) -> List[MatchExpression]:
        if not raw_expressions:
            return [MATCH_ALL_LINES_EXPRESSION]

        match_expressions: List[MatchExpression] = []
        for expression in raw_expressions:
            segments = expression.split(MATCH_EXPRESSION_SEPARATOR)
            alias = segments[0]
            if alias == MATCHTYPE_ALL_POSTFIX:
                raise ValueError(
                    f"Alias for match expression '{MATCHTYPE_ALL_POSTFIX}' is reserved. Please use another alias."
                )
            match_expr = segments[1]
            pattern = REGEX_EVERYTHING + match_expr.replace(".", "\\.") + REGEX_EVERYTHING
            match_expressions.append(MatchExpression(alias, expression, pattern))
        return match_expressions

    def get_non_aggregate_filters(self):
        return self._SIMPLE_MATCHED_LINE_FILTERS

    def get_aggregate_filters(self):
        return self._AGGREGATION_FILTERS

    def get_match_expression_aggregate_filters(self):
        return self._aggregated_match_expr_filters


@auto_str
class FailedTestCase(FailedTestCaseAbs):
    def __init__(self, full_name, failed_build: FailedBuildAbs, simple_name=None, parameterized=False, parameter=None):
        self._full_name = full_name
        self._failed_build: FailedBuildAbs = failed_build
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
            LOG.debug(
                f"Found parameterized testcase failure: {self._full_name}. "
                f"Simple testcase name: {self._simple_name}, "
                f"Parameter: {self._parameter}"
            )

    def full_name(self):
        return self._full_name

    def simple_name(self):
        return self._simple_name

    def parameter(self) -> str:
        return self._parameter

    def parameterized(self) -> bool:
        return self._parameterized

    def date(self) -> datetime.datetime:
        return self._failed_build.date()

    def origin(self):
        return self._failed_build.origin()

    def build_url(self) -> str:
        return self._failed_build.build_url()

    def job_name(self) -> str:
        return self._failed_build.job_name()


class TestFailuresByFilters(UserDict):
    def __init__(self, all_filters: TestCaseFilters):
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
            include_origin=True,
        )
        if tc_key in self._testcase_cache:
            stored_testcase = self._testcase_cache[tc_key]
            # TODO logging: printout seems to be wrong
            LOG.trace(
                f"Found already existing testcase key: {tc_key}. "
                f"Value: {stored_testcase}, "
                f"Email data (stored): {stored_testcase.origin()} "
                f"Email data (new): {stored_testcase.origin()}"
            )
            return
        else:
            self._testcase_cache[tc_key] = failed_testcase

        self.data[tcf].append(failed_testcase)


class EmailContentProcessor(ABC):
    @abstractmethod
    def process(self, email_content: EmailContent):
        pass
