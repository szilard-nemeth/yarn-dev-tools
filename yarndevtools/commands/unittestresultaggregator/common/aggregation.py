import datetime
from abc import ABC, abstractmethod
from collections import defaultdict, UserDict
from typing import List, Dict, Set, Tuple, Callable

from pythoncommons.date_utils import DateUtils, DATEFORMAT_DASH_COLON
from pythoncommons.object_utils import ListUtils

from yarndevtools.commands.unittestresultaggregator.gsheet import KnownTestFailures, KnownTestFailureInJira
from yarndevtools.commands.unittestresultaggregator.common.model import (
    TestCaseFilter,
    TestFailuresByFilters,
    FailedTestCaseAggregated,
    TestCaseKey,
    FailedTestCaseAbs,
    BuildComparisonResult,
    TestCaseFilters,
    AggregatedFailurePropertyFilter,
    EmailMetaData,
)
import logging

LOG = logging.getLogger(__name__)


class _PreAggregationPerFilter:
    def __init__(self, failures):
        self._test_failures: TestFailuresByFilters = failures
        self.failure_freqs: Dict[TestCaseKey, int] = {}
        self.latest_failures: Dict[TestCaseKey, datetime.datetime] = {}
        self.failures_per_tc_key: Dict[TestCaseKey, List[FailedTestCaseAbs]] = defaultdict(list)
        self.failure_dates: Dict[TestCaseKey, List[datetime.datetime]] = defaultdict(list)

    def perform(self, tcf: TestCaseFilter):
        for testcase in self._test_failures[tcf]:
            tc_key = TestCaseKey.create_from(
                tcf, testcase, use_simple_name=True, use_full_name=False, include_origin=False
            )
            self.failures_per_tc_key[tc_key].append(testcase)
            self.failure_dates[tc_key].append(testcase.date())

            if tc_key not in self.failure_freqs:
                self.failure_freqs[tc_key] = 1
                self.latest_failures[tc_key] = testcase.date()
            else:
                LOG.debug(
                    "Found already stored testcase key. "
                    f"Current testcase: {testcase}, "
                    f"Previously stored testcase failure freq: {self.failure_freqs[tc_key]}, "
                )
                self.failure_freqs[tc_key] = self.failure_freqs[tc_key] + 1

                # Only store latest testcase per TC key
                if testcase.date() > self.latest_failures[tc_key]:
                    self.latest_failures[tc_key] = testcase.date()


class _PropertyModifierAggregatorPerFilter:
    def __init__(self, pre_aggr: _PreAggregationPerFilter, sanity_checker: Callable):
        self._pre_aggr = pre_aggr
        self._sanity_checker = sanity_checker
        self.aggregated_test_failures: List[FailedTestCaseAggregated] = []

    def perform(self):
        for tc_key, testcases in self._pre_aggr.failures_per_tc_key.items():
            if len(testcases) > 1:
                # TODO Should be trace logged?
                LOG.debug(f"Found testcase objects that will be aggregated: {testcases}")
                # LOG.debug(
                #     "Found %d testcase objects that will be aggregated for TC key: %s", len(testcases), tc_key
                # )
                self._sanity_checker(testcases)

            # Full name is N/A because it's ambiguous between testcases.
            # We expect testcases to be having the same parameterized flags at this point, this was already sanity checked.
            # If not parameterized, full names should be the same.
            # If parameterized, we can't choose between full names.
            arbitrary_tc = testcases[0]
            parameterized = arbitrary_tc.parameterized()
            parameterized_more_testcases = parameterized and len(testcases) > 1

            # Simple names were also sanity checked that they are the same, choose the first.
            simple_name = arbitrary_tc.simple_name()
            full_name = "N/A" if parameterized_more_testcases else arbitrary_tc.full_name()
            parameter = arbitrary_tc.parameter() if parameterized else None

            # TODO Why parameterized is hardcoded to True?
            self.aggregated_test_failures.append(
                FailedTestCaseAggregated(
                    full_name=full_name,
                    simple_name=simple_name,
                    parameterized=True,
                    parameter=parameter,
                    latest_failure=self._pre_aggr.latest_failures[tc_key],
                    failure_freq=self._pre_aggr.failure_freqs[tc_key],
                    failure_dates=self._pre_aggr.failure_dates[tc_key],
                    # Cannot fill known_failure / reoccurred at this point --> Jira check will be performed later!
                    known_failure=None,
                    reoccurred=None,
                )
            )


class AggregatedTestFailures(UserDict):
    def __init__(self, filters: TestCaseFilters, test_failures: TestFailuresByFilters):
        super().__init__()
        self.data: Dict[TestCaseFilter, List[FailedTestCaseAggregated]] = self._aggregate(filters, test_failures)
        self._by_name: Dict[TestCaseFilter, Dict[str, FailedTestCaseAggregated]] = self._get_testcases_by_name(filters)

    def __getitem__(self, tcf):
        return self.data[tcf]

    def get_failed_testcases_by_name(self, tcf: TestCaseFilter):
        return self._by_name[tcf]

    def _aggregate(self, filters: TestCaseFilters, test_failures: TestFailuresByFilters):
        result = {}
        for tcf in filters:
            aggr_per_filter = _PreAggregationPerFilter(test_failures)
            aggr_per_filter.perform(tcf)
            aggregator = _PropertyModifierAggregatorPerFilter(aggr_per_filter, self._sanity_check_testcases)
            aggregator.perform()
            result[tcf] = aggregator.aggregated_test_failures
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
            raise ValueError(
                "Encountered 2 different testcase full names but testcases are not having the same parameterized flags. "
                f"Testcase objects: {testcases}"
            )

    def _get_testcases_by_name(self, filters):
        final_result = {}
        for tcf in filters:
            failed_testcases_by_name: Dict[str, FailedTestCaseAggregated] = {
                tc.simple_name: tc for tc in self.data[tcf]
            }
            # Sanity check size
            if len(failed_testcases_by_name) != len(self.data[tcf]):
                dupes = ListUtils.get_duplicates([tc.simple_name for tc in self.data[tcf]])
                raise ValueError(
                    "Size mismatch between aggregated test failures and procuded dict!\n"
                    "Original aggregated test failures: {}\n"
                    "Produced dict: {}\n"
                    "Filter: {}\n"
                    "Duplicates: {}".format(self.data[tcf], failed_testcases_by_name, tcf, dupes)
                )
            final_result[tcf] = failed_testcases_by_name
        return final_result

    def get_by_filters(self, tcf: TestCaseFilter, *prop_filters: AggregatedFailurePropertyFilter):
        def apply_filter(tc, propfilter: AggregatedFailurePropertyFilter):
            if not hasattr(tc, propfilter.property_name):
                raise ValueError(
                    "Invalid property filter specification. Object {} has no attr named '{}'".format(
                        tc, propfilter.property_name
                    )
                )
            prop_value = getattr(tc, propfilter.property_name)
            if propfilter.inverted:
                return not prop_value
            return True if prop_value else False

        testcase_failures: List[FailedTestCaseAggregated] = self.data[tcf]
        orig_no_of_failurs = len(testcase_failures)
        no_of_failures = orig_no_of_failurs

        for prop_filter in prop_filters:
            testcase_failures = list(filter(lambda tc: apply_filter(tc, prop_filter), testcase_failures))
            LOG.debug(
                f"Filtering with filter: {prop_filter}. "
                f"Previous length of aggregated test failures: {no_of_failures}, "
                f"New length of filtered aggregated test failures: {len(testcase_failures)}"
            )
            no_of_failures = len(testcase_failures)

        LOG.debug(
            "Returning filtered aggregated test failures. "
            f"Original length of ALL aggregated test failures: {orig_no_of_failurs}, "
            f"Length of filtered aggregated test failures: {no_of_failures}, "
            f"Applied filters: {prop_filters}"
        )
        return testcase_failures


class LatestTestFailures(UserDict):
    def __init__(
        self,
        filters: TestCaseFilters,
        test_failures: TestFailuresByFilters,
        last_n_days: int = -1,
        only_last_results: bool = False,
        reset_oldest_day_to_midnight: bool = True,
        strict_mode: bool = False,
    ):
        super().__init__()
        self._test_failures = test_failures
        self.data = self._create_latest_failures(
            filters,
            last_n_days=last_n_days,
            only_last_results=only_last_results,
            reset_oldest_day_to_midnight=reset_oldest_day_to_midnight,
            strict_mode=strict_mode,
        )

    def __getitem__(self, tcf):
        return self.data[tcf]

    def _create_latest_failures(
        self,
        filters: TestCaseFilters,
        last_n_days: int = -1,
        only_last_results: bool = False,
        reset_oldest_day_to_midnight: bool = True,
        strict_mode: bool = False,
    ):
        if sum([True if last_n_days > -1 else False, only_last_results]) != 1:
            raise ValueError("Either last_n_days or only_last_results mode should be enabled.")

        start_date = "unknown"
        result = defaultdict(list)
        for tcf in filters:
            sorted_testcases = sorted(self._test_failures[tcf], key=lambda ftc: ftc.date(), reverse=True)
            if not sorted_testcases:
                return []

            if last_n_days:
                start_date = self._get_start_date(last_n_days, reset_oldest_day_to_midnight)
                LOG.info(f"Using start date to filter dates from: {start_date}")
            else:
                start_date = sorted_testcases[0].date()

            for testcase in sorted_testcases:
                if testcase.date() >= start_date:
                    result[tcf].append(testcase)

        if not result and strict_mode:
            raise ValueError("No latest test failures found! Start date was: {}".format(start_date))

        return result

    @staticmethod
    def _get_start_date(days_back: int, reset_oldest_day_to_midnight=False):
        start_date: datetime.datetime = DateUtils.get_current_time_minus(days=days_back)
        if reset_oldest_day_to_midnight:
            start_date = DateUtils.reset_to_midnight(start_date)
        return start_date


class FailedBuildAbs(ABC):
    @classmethod
    def create_from_email(cls, email_meta: EmailMetaData):
        return FailedBuildFromEmail(email_meta)

    @abstractmethod
    def build_url(self) -> str:
        pass

    @abstractmethod
    def job_name(self) -> str:
        pass

    @abstractmethod
    def origin(self):
        pass

    @abstractmethod
    def date(self) -> datetime.datetime:
        pass


class FailedBuildFromEmail(FailedBuildAbs):
    # TODO yarndevtoolsv2 DB: Cross check with FailedTestCaseFromEmail for common fields
    def __init__(self, email_meta: EmailMetaData):
        self._email_meta: EmailMetaData = email_meta

    def build_url(self) -> str:
        return self._email_meta.build_url

    def job_name(self) -> str:
        return self._email_meta.job_name

    def origin(self):
        return self._email_meta.subject

    def date(self) -> datetime.datetime:
        return self._email_meta.date


class FailedBuilds:
    def __init__(self):
        self._by_build_url = defaultdict(list)
        self._by_date = defaultdict(list)

    def add_build(self, failed_build: FailedBuildAbs):
        self._by_build_url[failed_build.build_url()].append(failed_build)
        self._by_date[failed_build.job_name()].append(failed_build)

    def get_by_dates(self) -> Dict[str, List[FailedTestCaseAbs]]:
        res = {}
        for k, failed_builds in self._by_date.items():
            res[k] = sorted(failed_builds, key=lambda m: m.date(), reverse=True)
        return res

    def get_dates(self) -> Dict[str, List[str]]:
        result = {}
        for job_name, failed_builds in self._by_date.items():
            dates = [build.date() for build in failed_builds]
            dates = sorted(dates, reverse=True)
            result[job_name] = [DateUtils.convert_datetime_to_str(dt, DATEFORMAT_DASH_COLON) for dt in dates]
        return result


class TestFailureComparison(UserDict):
    def __init__(
        self,
        filters: TestCaseFilters,
        test_failures: TestFailuresByFilters,
        compare_with_last: bool = True,
        compare_with_n_days_old: int = -1,
    ):
        super().__init__()
        self._filters = filters
        self._test_failures = test_failures
        self.data: Dict[TestCaseFilter, BuildComparisonResult] = self._compare(
            compare_with_last=compare_with_last, compare_with_n_days_old=compare_with_n_days_old
        )

    def __getitem__(self, tcf):
        return self.data[tcf]

    def _compare(self, compare_with_last: bool = True, compare_with_n_days_old: int = -1):
        last_n_days = self._get_last_n_days_to_compare(compare_with_last, compare_with_n_days_old)
        result = {}
        for tcf in self._filters:
            LOG.debug("Creating failure comparison for testcase filter: %s", tcf)
            sorted_testcases = sorted(self._test_failures[tcf], key=lambda ftc: ftc.date(), reverse=True)
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
                fixed=[old_build_tcs[tck] for tck in fixed],
                still_failing=[latest_tcs[tck] for tck in still_failing],
                new=[latest_tcs[tck] for tck in new_failures],
            )
        return result

    @staticmethod
    def _get_last_n_days_to_compare(compare_with_last, compare_with_n_days_old):
        if (compare_with_last and compare_with_n_days_old != -1) or not any(
            [compare_with_last, compare_with_n_days_old]
        ):
            raise ValueError(
                "Either use 'compare_with_last' or 'compare_with_n_days_old' " "but not both at the same time."
            )
        last_n_days = 1 if compare_with_last != -1 else compare_with_n_days_old
        return last_n_days

    @staticmethod
    def _get_comparable_testcase_lists(
        sorted_testcases: List[FailedTestCaseAbs], last_n_days: int
    ) -> Tuple[Dict[str, FailedTestCaseAbs], Dict[str, FailedTestCaseAbs]]:
        # Find all testcases for latest build
        reference_date: datetime.datetime = sorted_testcases[0].date()
        latest_testcases, start_idx = TestFailureComparison._find_index_of_tc_with_first_different_date(
            reference_date, sorted_testcases
        )

        # Find all testcases for build to compare:
        # Either build before last build or build with specified "distance" from the latest build
        testcases_to_compare = TestFailureComparison._find_testcases_to_compare(
            sorted_testcases, reference_date, last_n_days, start_idx
        )

        # If we haven't found any other testcase, it means delta_days haven't reached the given number of days.
        # Relax criteria
        if not testcases_to_compare:
            next_date = sorted_testcases[start_idx].date()
            for i in range(start_idx, len(sorted_testcases)):
                tc = sorted_testcases[i]
                if not tc.date() == next_date:
                    break
                testcases_to_compare[tc.simple_name()] = tc

        return latest_testcases, testcases_to_compare

    @staticmethod
    def _find_testcases_to_compare(sorted_testcases, reference_date, last_n_days, start_idx):
        stored_delta: int or None = None
        testcases_to_compare: Dict[str, FailedTestCaseAbs] = {}
        for i in range(len(sorted_testcases) - 1, start_idx, -1):
            tc = sorted_testcases[i]
            delta_days = (reference_date - tc.date()).days
            if stored_delta and delta_days != stored_delta:
                break

            if delta_days <= last_n_days:
                if not stored_delta:
                    stored_delta = delta_days
                testcases_to_compare[tc.simple_name()] = tc
        return testcases_to_compare

    @staticmethod
    def _find_index_of_tc_with_first_different_date(reference_date, sorted_testcases):
        latest_testcases: Dict[str, FailedTestCaseAbs] = {}
        start_idx = 0
        while True:
            tc = sorted_testcases[start_idx]
            if tc.date() != reference_date:
                # We found a new date, will be processed with the next loop
                break

            latest_testcases[tc.simple_name()] = tc
            start_idx += 1
        return latest_testcases, start_idx


class KnownTestFailureChecker:
    def __init__(
        self,
        filters: TestCaseFilters,
        known_failures: KnownTestFailures,
        aggregated_test_failures: AggregatedTestFailures,
    ):
        self._filters = filters
        self.known_failures: KnownTestFailures = known_failures
        self._aggregated: AggregatedTestFailures = aggregated_test_failures
        self._cross_check_results_with_known_failures()

    def _cross_check_results_with_known_failures(self):
        if not any(True for _ in self.known_failures):
            raise ValueError("Empty known test failures!")
        encountered_known_failures: Set[KnownTestFailureInJira] = set()
        for tcf in self._filters:
            # Init all testcase to not known failure + not reoccurred by default
            for testcase in self._aggregated[tcf]:
                testcase.known_failure = False
                testcase.reoccurred = False

            LOG.debug(f"Cross-checking testcases with known failures for filter: {tcf.short_str()}")

            # Create intersection between current aggregated test failures and known failures to get relevant testcases
            failed_aggr_testcases_by_name = self._aggregated.get_failed_testcases_by_name(tcf)
            keys = set(failed_aggr_testcases_by_name.keys()).intersection(set(self.known_failures.by_name.keys()))
            relevant_testcases = [failed_aggr_testcases_by_name[k] for k in keys]

            for testcase in relevant_testcases:
                known_failures = self.known_failures.by_name[testcase.simple_name]

                known = True if len(known_failures) > 0 else False
                for known_failure in known_failures:
                    encountered_known_failures.add(known_failure)
                    LOG.debug(
                        "Found known failure in failed testcases:\n"
                        f"Failed testcase: {testcase.simple_name}, "
                        f"Known failure: {known_failure.tc_name}"
                    )
                    testcase.known_failure = True

                    if known_failure.resolution_date and testcase.latest_failure > known_failure.resolution_date:
                        LOG.info(f"Found reoccurred testcase failure: {testcase}")
                        testcase.reoccurred = True

                if not known:
                    LOG.info(
                        "Found new unknown test failure (that does not have reported Jira).\n "
                        f"Testcase details: {testcase}. "
                        f"Testcase filter: {tcf.short_str()}"
                    )

        all_known_test_failures = set(self.known_failures)
        not_encountered_known_failures = all_known_test_failures.difference(encountered_known_failures)
        if not_encountered_known_failures:
            LOG.warning(
                "Found known failures that are not encountered for any test failures. "
                f"Not encountered: {not_encountered_known_failures}"
                f"Filters: {self._filters}"
            )
