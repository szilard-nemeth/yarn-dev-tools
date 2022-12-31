import datetime
from collections import defaultdict
from typing import List, Dict, Set, Tuple

from pythoncommons.date_utils import DateUtils

from yarndevtools.commands.unittestresultaggregator.gsheet import KnownTestFailures, KnownTestFailureInJira
from yarndevtools.commands.unittestresultaggregator.common_tmp.model import (
    TestCaseFilter,
    TestFailuresByFilters,
    FailedTestCaseAggregated,
    TestCaseKey,
    FailedTestCaseAbs,
    BuildComparisonResult,
)
import logging

LOG = logging.getLogger(__name__)

# TODO
# class AggregatedTestFailures(UserDict):


class AggregatedTestFailures:
    def __init__(self, filters: List[TestCaseFilter], test_failures: TestFailuresByFilters):
        # super().__init__()
        self._aggregated_test_failures: Dict[TestCaseFilter, List[FailedTestCaseAggregated]] = self._aggregate(
            filters, test_failures
        )

    # def __getitem__(self, key):
    #     return self.data[key]
    #
    def get(self, tcf: TestCaseFilter):
        return self._aggregated_test_failures[tcf]

    def _aggregate(self, filters: List[TestCaseFilter], test_failures: TestFailuresByFilters):
        result = {}
        for tcf in filters:
            failure_freqs: Dict[TestCaseKey, int] = {}
            latest_failures: Dict[TestCaseKey, datetime.datetime] = {}
            tc_key_to_testcases: Dict[TestCaseKey, List[FailedTestCaseAbs]] = defaultdict(list)
            aggregated_test_failures: List[FailedTestCaseAggregated] = []
            for testcase in test_failures.get_all(tcf):
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
            result[tcf] = aggregated_test_failures

        return result

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


class LatestTestFailures:
    def __init__(
        self,
        filters: List[TestCaseFilter],
        test_failures: TestFailuresByFilters,
        only_last_results=True,
    ):
        self._test_failures = test_failures
        self._latest_testcases = self._create_latest_failures(filters, only_last_results=only_last_results)

    def get(self, tcf):
        return self._test_failures.get_all(tcf)

    def _create_latest_failures(
        self,
        testcase_filters: List[TestCaseFilter],
        last_n_days=None,
        only_last_results=False,
        reset_oldest_day_to_midnight=False,
    ):
        if sum([True if last_n_days else False, only_last_results]) != 1:
            raise ValueError("Either last_n_days or only_last_results mode should be enabled.")

        result = {}
        for tcf in testcase_filters:
            failed_testcases = self._test_failures.get_all(tcf)
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
                    result[tcf].append(testcase)

        return result

    @staticmethod
    def _get_date_range_open(last_n_days, reset_oldest_day_to_midnight=False):
        oldest_day: datetime.datetime = DateUtils.get_current_time_minus(days=last_n_days)
        if reset_oldest_day_to_midnight:
            oldest_day = DateUtils.reset_to_midnight(oldest_day)
        return oldest_day


class TestFailureComparison:
    def __init__(
        self,
        filters: List[TestCaseFilter],
        test_failures: TestFailuresByFilters,
        compare_with_last: bool = True,
    ):
        self._testcase_filters = filters
        self._test_failures = test_failures
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
            failed_testcases = self._test_failures.get_all(tcf)
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


class KnownTestFailureChecker:
    def __init__(
        self,
        filters: List[TestCaseFilter],
        known_failures: KnownTestFailures,
        aggregated_test_failures: AggregatedTestFailures,
    ):
        self.filters = filters
        self.known_failures = known_failures
        self._aggregated = aggregated_test_failures
        self._cross_check_results_with_known_failures()

    def _cross_check_results_with_known_failures(self):
        if not any(True for _ in self.known_failures):
            raise ValueError("Empty known test failures!")
        encountered_known_test_failures: Set[KnownTestFailureInJira] = set()
        # TODO Optimize triple for-loop
        for tcf in self.filters:
            LOG.debug(f"Cross-checking testcases with known test failures from Jira for filter: {tcf.short_str()}")
            for testcase in self._aggregated.get(tcf):
                # for testcase in self._aggregated[tcf]:
                # TODO Simplify logic
                known_tcf: KnownTestFailureInJira or None = None
                for known_test_failure in self.known_failures:
                    if known_test_failure.tc_name in testcase.simple_name:
                        encountered_known_test_failures.add(known_test_failure)
                        LOG.debug(
                            "Found matching failed testcase + known Jira testcase:\n"
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

        all_known_test_failures = set(self.known_failures)
        not_encountered_known_test_failures = all_known_test_failures.difference(encountered_known_test_failures)
        if not_encountered_known_test_failures:
            LOG.warning(
                "Found known jira test failures that are not encountered for any test failures. "
                f"Not encountered: {not_encountered_known_test_failures}"
                f"Filters: {self.filters}"
            )
