import datetime
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Set, Tuple

from pythoncommons.date_utils import DateUtils

from yarndevtools.commands.unittestresultaggregator.common_tmp.model import (
    MatchExpression,
    KnownTestFailureInJira,
    BuildComparisonResult,
    FailedTestCaseAggregated,
    TestCaseFilter,
    TestCaseKey,
    FailedTestCaseAbs,
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


class TestFailureComparison:
    def __init__(
        self,
        filters: List[TestCaseFilter],
        test_failures_by_tcf: Dict[TestCaseFilter, List[FailedTestCaseAbs]],
        compare_with_last: bool = True,
    ):
        self._testcase_filters = filters
        self._test_failures_by_tcf = test_failures_by_tcf
        self._compare_with_last = compare_with_last
        self._results: Dict[TestCaseFilter, BuildComparisonResult] = self._compare()

    def get(self, tcf: TestCaseFilter):
        return self._results[tcf]

    def _compare(self, compare_with_n_days_old=None):
        if (self._compare_with_last and compare_with_n_days_old) or not any(
            [self._compare_with_last, compare_with_n_days_old]
        ):
            raise ValueError(
                "Either use 'compare_with_last' or 'compare_with_n_days_old' " "but not both at the same time."
            )
        last_n_days = 1 if self._compare_with_last else compare_with_n_days_old

        result = {}
        for tcf in self._testcase_filters:
            LOG.debug("Creating failure comparison for testcase filter: %s", tcf)
            failed_testcases = self._test_failures_by_tcf[tcf]
            sorted_testcases = sorted(failed_testcases, key=lambda ftc: ftc.date(), reverse=True)
            if not sorted_testcases:
                LOG.warning("No failed testcases found for testcase filter: %s", tcf)
                return

            latest_tcs, old_build_tcs = self._get_comparable_testcase_lists(sorted_testcases, last_n_days)
            latest_tc_keys: Set[str] = set(latest_tcs.keys())
            older_tc_keys: Set[str] = set(old_build_tcs.keys())

            fixed: Set[str] = older_tc_keys.difference(latest_tc_keys)
            still_failing: Set[str] = latest_tc_keys.intersection(older_tc_keys)
            new_failures: Set[str] = latest_tc_keys.difference(older_tc_keys)
            result[tcf] = BuildComparisonResult(
                fixed=[old_build_tcs[k] for k in fixed],
                still_failing=[latest_tcs[k] for k in still_failing],
                new=[latest_tcs[k] for k in new_failures],
            )
        return result

    @staticmethod
    def _get_comparable_testcase_lists(
        sorted_testcases, last_n_days
    ) -> Tuple[Dict[str, FailedTestCaseAbs], Dict[str, FailedTestCaseAbs]]:
        # Result lists
        latest_testcases: Dict[str, FailedTestCaseAbs] = {}
        to_compare_testcases: Dict[str, FailedTestCaseAbs] = {}

        reference_date: datetime.datetime = sorted_testcases[0].date()
        # Find all testcases for latest build
        start_idx = 0
        while True:
            tc = sorted_testcases[start_idx]
            if tc.date() == reference_date:
                latest_testcases[tc.simple_name()] = tc
                start_idx += 1
            else:
                # We found a new date, will be processed with the next loop
                break

        # Find all testcases for build to compare:
        # Either build before last build or build with specified "distance" from the latest build
        stored_delta: int or None = None
        for i in range(len(sorted_testcases) - 1, start_idx, -1):
            tc = sorted_testcases[i]
            delta_days = (reference_date - tc.date()).days
            if stored_delta and delta_days != stored_delta:
                break

            if delta_days <= last_n_days:
                if not stored_delta:
                    stored_delta = delta_days
                to_compare_testcases[tc.simple_name()] = tc

        # If we haven't found any other testcase, it means delta_days haven't reached the given number of days.
        # Relax criteria
        if not to_compare_testcases:
            next_date = sorted_testcases[start_idx].date()
            for i in range(start_idx, len(sorted_testcases)):
                tc = sorted_testcases[i]
                if not tc.date() == next_date:
                    break
                to_compare_testcases[tc.simple_name()] = tc

        return latest_testcases, to_compare_testcases


class KnownTestFailures:
    def __init__(self, gsheet_wrapper=None, gsheet_jira_table=None):
        self._testcases_to_jiras: List[KnownTestFailureInJira] = []
        self.gsheet_wrapper = gsheet_wrapper
        if gsheet_jira_table:
            self._testcases_to_jiras: List[KnownTestFailureInJira] = self._load_and_convert_known_test_failures_in_jira(
                gsheet_jira_table
            )
        self._index = 0
        self._num_testcases = len(self._testcases_to_jiras)

    def __len__(self):
        return self._num_testcases

    def __iter__(self):
        return self

    def __next__(self):
        if self._index == self._num_testcases:
            raise StopIteration
        result = self._testcases_to_jiras[self._index]
        self._index += 1
        return result

    def _load_and_convert_known_test_failures_in_jira(self, gsheet_jira_table) -> List[KnownTestFailureInJira]:
        # TODO yarndevtoolsv2: Data should be written to mongoDB once
        raw_data_from_gsheet = self.gsheet_wrapper.read_data(gsheet_jira_table, "A1:E150")
        LOG.info(f"Successfully loaded data from worksheet: {gsheet_jira_table}")

        header: List[str] = raw_data_from_gsheet[0]
        expected_header = ["Testcase", "Jira", "Resolution date"]
        if header != expected_header:
            raise ValueError(
                "Detected suspicious known test failures table header. "
                f"Expected header: {expected_header}, "
                f"Current header: {header}"
            )

        raw_data_from_gsheet = raw_data_from_gsheet[1:]
        known_tc_failures = []
        for row in raw_data_from_gsheet:
            self._preprocess_row(row)
            t_name = row[0]
            jira_link = row[1]
            date_time = DateUtils.convert_to_datetime(row[2], "%m/%d/%Y") if row[2] else None
            known_tc_failures.append(KnownTestFailureInJira(t_name, jira_link, date_time))

        return known_tc_failures

    @staticmethod
    def _preprocess_row(row):
        row_len = len(row)
        if row_len < 2:
            raise ValueError(
                "Both 'Testcase' and 'Jira' are mandatory items but row does not contain them. "
                f"Problematic row: {row}"
            )
        # In case of 'Resolution date' is missing, append an empty-string so that all rows will have
        # an equal number of cells. This eases further processing.
        if row_len == 2:
            row.append("")


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


class FinalAggregationResults:
    # TODO yarndevtoolsv2: Revisit any email specific logic in this class
    # TODO yarndevtoolsv2: Extract build comparison + jira logic to new class
    def __init__(self, all_filters: List[TestCaseFilter]):
        self._test_failures_by_tcf: Dict[TestCaseFilter, List[FailedTestCaseAbs]] = {}
        self._aggregated_test_failures: Dict[TestCaseFilter, List[FailedTestCaseAggregated]] = {}
        self.comparison: TestFailureComparison = None
        self._tc_keys: Dict[TestCaseKey, FailedTestCaseAbs] = {}
        self._latest_testcases: Dict[TestCaseFilter, List[FailedTestCaseAbs]] = defaultdict(list)
        self._init_with_testcase_filters(all_filters)

    def _init_with_testcase_filters(self, filters: List[TestCaseFilter]):
        for tcf in filters:
            if tcf not in self._test_failures_by_tcf:
                self._test_failures_by_tcf[tcf] = []

    def _add_known_failed_testcase(self, tc_key: TestCaseKey, ftc: FailedTestCaseAbs):
        self._tc_keys[tc_key] = ftc

    def add_failure(self, tcf: TestCaseFilter, failed_testcase: FailedTestCaseAbs):
        tc_key = TestCaseKey.create_from(tcf, failed_testcase)
        if tc_key in self._tc_keys:
            stored_testcase = self._tc_keys[tc_key]
            LOG.debug(
                f"Found already existing testcase key: {tc_key}. "
                f"Value: {stored_testcase}, "
                f"Email data (stored): {stored_testcase.subject()} "
                f"Email data (new): {stored_testcase.subject()}"
            )
            return
        else:
            self._add_known_failed_testcase(tc_key, failed_testcase)

        self._test_failures_by_tcf[tcf].append(failed_testcase)

    def get(self, tcf) -> List[FailedTestCaseAbs]:
        return self._test_failures_by_tcf[tcf]

    def get_latest_testcases(self, tcf) -> List[FailedTestCaseAbs]:
        return self._latest_testcases[tcf]

    def get_build_comparison_results(self, tcf) -> BuildComparisonResult:
        return self.comparison.get(tcf)

    def get_aggregated_testcases(self, tcf) -> List[FailedTestCaseAggregated]:
        return self._aggregated_test_failures[tcf]

    def print_keys(self):
        LOG.debug(f"Keys of _failed_testcases_by_filter: {self._test_failures_by_tcf.keys()}")

    def aggregate(self, testcase_filters: List[TestCaseFilter]):
        for tcf in testcase_filters:
            failure_freqs: Dict[TestCaseKey, int] = {}
            latest_failures: Dict[TestCaseKey, datetime.datetime] = {}
            tc_key_to_testcases: Dict[TestCaseKey, List[FailedTestCaseAbs]] = defaultdict(list)
            aggregated_test_failures: List[FailedTestCaseAggregated] = []
            for testcase in self._test_failures_by_tcf[tcf]:
                tc_key = TestCaseKey.create_from(
                    tcf, testcase, use_simple_name=True, use_full_name=False, include_email_subject=False
                )
                tc_key_to_testcases[tc_key].append(testcase)
                if tc_key not in failure_freqs:
                    failure_freqs[tc_key] = 1
                    latest_failures[tc_key] = testcase.date()
                else:
                    LOG.debug(
                        "Found TC key in failure_freqs dict. "
                        f"Current TC: {testcase}, "
                        f"Previously stored TC: {failure_freqs[tc_key]}, "
                    )
                    failure_freqs[tc_key] = failure_freqs[tc_key] + 1
                    if testcase.date() > latest_failures[tc_key]:
                        latest_failures[tc_key] = testcase.date()

            for tc_key, testcases in tc_key_to_testcases.items():
                if len(testcases) > 1:
                    # TODO Should be trace logged
                    # LOG.debug(f"Found testcase objects that will be aggregated: {testcases}")
                    LOG.debug(
                        "Found %d testcase objects that will be aggregated for TC key: %s", len(testcases), tc_key
                    )
                    self._sanity_check_testcases(testcases)

                # Full name is N/A because it's ambiguous between testcases.
                # We expect TCs to be having the same parameterized flags at this point, this was already sanity checked.
                # If parameterized, we can't choose between full names.
                # If not parameterized, full names should be the same.
                arbitrary_tc = testcases[0]
                parameter = None
                if arbitrary_tc.parameterized():
                    if len(testcases) > 1:
                        full_name = "N/A"
                    else:
                        full_name = arbitrary_tc.full_name()
                        parameter = arbitrary_tc.parameter()
                else:
                    full_name = arbitrary_tc.full_name()
                # Simple names were also sanity checked that they are the same, choose the first.
                simple_name = arbitrary_tc.simple_name()

                # TODO fill failure dates
                failure_dates = []
                # Cannot fill known_failure / reoccurred at this point --> Jira check will be performed later!
                aggregated_test_failures.append(
                    FailedTestCaseAggregated(
                        full_name=full_name,
                        simple_name=simple_name,
                        parameterized=True,
                        parameter=parameter,
                        latest_failure=latest_failures[tc_key],
                        failure_freq=failure_freqs[tc_key],
                        failure_dates=failure_dates,
                        known_failure=None,
                        reoccurred=None,
                    )
                )
            self._aggregated_test_failures[tcf] = aggregated_test_failures

    @staticmethod
    def _sanity_check_testcases(testcases: List[FailedTestCaseAbs]):
        simple_names = set([tc.simple_name() for tc in testcases])
        full_names = set()
        parameterized = set()
        for tc in testcases:
            full_names.add(tc.full_name())
            parameterized.add(tc.parameterized())

        if len(simple_names) > 1:
            raise ValueError(
                "Invalid state. Aggregated testcases should have had the same simple name. "
                f"Testcase objects: {testcases}\n"
                f"Simple names: {simple_names}"
            )

        parameterized_lst = list(parameterized)
        parameterized_had_same_value = True if (len(parameterized_lst) == 1 and parameterized_lst[0]) else False
        # If we have more than 1 fullname, testcases should be all parameterized
        if len(full_names) > 1 and not parameterized_had_same_value:
            pass
            # TODO yarndevtoolsv2: this check does not really makes sense now
            # raise ValueError(
            #     "We have 2 different TC full names but testcases are not having the same parameterized flags. "
            #     f"Testcase objects: {testcases}"
            # )

    def create_latest_failures(
        self,
        testcase_filters: List[TestCaseFilter],
        last_n_days=None,
        only_last_results=False,
        reset_oldest_day_to_midnight=False,
    ):
        if sum([True if last_n_days else False, only_last_results]) != 1:
            raise ValueError("Either last_n_days or only_last_results mode should be enabled.")

        for tcf in testcase_filters:
            failed_testcases = self._test_failures_by_tcf[tcf]
            sorted_testcases = sorted(failed_testcases, key=lambda ftc: ftc.date(), reverse=True)
            if not sorted_testcases:
                return []

            if last_n_days:
                date_range_open = self._get_date_range_open(last_n_days, reset_oldest_day_to_midnight)
                LOG.info(f"Using date range open date to filter dates: {date_range_open}")
            else:
                date_range_open = sorted_testcases[0].date()

            for testcase in sorted_testcases:
                if testcase.date() >= date_range_open:
                    self._latest_testcases[tcf].append(testcase)

    @staticmethod
    def _get_date_range_open(last_n_days, reset_oldest_day_to_midnight=False):
        oldest_day: datetime.datetime = DateUtils.get_current_time_minus(days=last_n_days)
        if reset_oldest_day_to_midnight:
            oldest_day = DateUtils.reset_to_midnight(oldest_day)
        return oldest_day

    def cross_check_testcases_with_jiras(
        self, testcase_filters: List[TestCaseFilter], known_failures: KnownTestFailures
    ):
        if not any(True for _ in known_failures):
            raise ValueError("Testcases to jira mappings is empty!")
        encountered_known_test_failures: Set[KnownTestFailureInJira] = set()
        for tcf in testcase_filters:
            LOG.debug(f"Cross-checking testcases with known test failures from jira for filter: {tcf.short_str()}")
            for testcase in self._aggregated_test_failures[tcf]:
                known_tcf: KnownTestFailureInJira or None = None
                for known_test_failure in known_failures:
                    if known_test_failure.tc_name in testcase.simple_name:
                        encountered_known_test_failures.add(known_test_failure)
                        LOG.debug(
                            "Found matching failed testcase + known jira testcase:\n"
                            f"Failed testcase: {testcase.simple_name}, Known testcase: {known_test_failure.tc_name}"
                        )
                        testcase.known_failure = True
                        known_tcf = known_test_failure

                if testcase.known_failure:
                    if known_tcf.resolution_date and testcase.latest_failure > known_tcf.resolution_date:
                        LOG.info(f"Found reoccurred testcase failure: {testcase}")
                        testcase.reoccurred = True
                    else:
                        testcase.reoccurred = False
                else:
                    LOG.info(
                        "Found testcase that does not have corresponding jira so it is unknown. "
                        f"Testcase details: {testcase}. "
                        f"Testcase filter: {tcf.short_str()}"
                    )
                    testcase.known_failure = False
                    testcase.reoccurred = False

        all_known_test_failures = set(known_failures)
        not_encountered_known_test_failures = all_known_test_failures.difference(encountered_known_test_failures)
        if not_encountered_known_test_failures:
            LOG.warning(
                "Found known jira test failures that are not encountered for any test failures. "
                f"Not encountered: {not_encountered_known_test_failures}"
                f"Filters: {testcase_filters}"
            )
