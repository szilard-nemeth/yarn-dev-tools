import logging
import os
import unittest

from pythoncommons.constants import ExecutionMode
from pythoncommons.logging_setup import SimpleLoggingSetup
from pythoncommons.project_utils import ProjectRootDeterminationStrategy, ProjectUtils

from tests.test_utilities import TestUtilities
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.commands.unittestresultaggregator.common import TestCaseFilter, MatchExpression, AggregateFilter
from yarndevtools.commands.unittestresultaggregator.unit_test_result_aggregator import TestCaseFilters
from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME

CDP_7X = "CDPD-7.x"
CDP_71X = "CDPD-7.1.x"
ALL_EXPRESSION = ".*"
MR_EXPRESSION = "MR::org.apache.hadoop.mapreduce"
YARN_EXPRESSION = "YARN::org.apache.hadoop.yarn"
MR_PATTERN = ".*org\\.apache\\.hadoop\\.mapreduce.*"
YARN_PATTERN = ".*org\\.apache\\.hadoop\\.yarn.*"

LOG = logging.getLogger(__name__)
SOME_PARENT_DIR = "some_parent_dir"
REPO_ROOT_DIRNAME = "some_repo_root_dirname"
TEST_DIR_NAME = "somedir"


class TestTestCaseFilters(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ProjectUtils.set_root_determine_strategy(ProjectRootDeterminationStrategy.COMMON_FILE)
        ProjectUtils.get_test_output_basedir(YARNDEVTOOLS_MODULE_NAME)
        SimpleLoggingSetup.init_logger(
            project_name=CommandType.UNIT_TEST_RESULT_AGGREGATOR.real_name,
            logger_name_prefix=YARNDEVTOOLS_MODULE_NAME,
            execution_mode=ExecutionMode.TEST,
            console_debug=True,
        )

        match_expressions = [YARN_EXPRESSION, MR_EXPRESSION]
        aggr_filters = [CDP_71X, CDP_7X]
        cls.filters = TestCaseFilters(
            TestCaseFilters.convert_raw_match_expressions_to_objs(match_expressions), aggr_filters
        )

    @classmethod
    def tearDownClass(cls) -> None:
        TestUtilities.tearDownClass(cls.__name__, command_type=CommandType.UNIT_TEST_RESULT_AGGREGATOR)

    def setUp(self):
        self.test_instance = self

    def tearDown(self) -> None:
        pass

    @classmethod
    def _ensure_env_var_is_present(cls, env_name):
        if env_name not in os.environ:
            raise ValueError(f"Please set '{env_name}' env var and re-run the test!")

    @staticmethod
    def simple_matched_line_filters():
        expected_filters = [
            TestCaseFilter(MatchExpression("YARN", YARN_EXPRESSION, YARN_PATTERN), None, aggregate=False),
            TestCaseFilter(MatchExpression("MR", MR_EXPRESSION, MR_PATTERN), None, aggregate=False),
            TestCaseFilter(MatchExpression("Failed testcases", ALL_EXPRESSION, ALL_EXPRESSION), None, aggregate=False),
        ]
        return expected_filters

    @staticmethod
    def all_aggregation_filters():
        # 6 filters altogether
        expected_filters = [
            # 2 filters for YARN: YARN-cdp7x-aggregated, YARN-cdp71x-aggregated,
            TestCaseFilter(
                MatchExpression("YARN", YARN_EXPRESSION, YARN_PATTERN), AggregateFilter(CDP_7X), aggregate=True
            ),
            TestCaseFilter(
                MatchExpression("YARN", YARN_EXPRESSION, YARN_PATTERN), AggregateFilter(CDP_71X), aggregate=True
            ),
            # 2 filters for MR: MR-cdp7x-aggregated, MR-cdp71x-aggregated,
            TestCaseFilter(MatchExpression("MR", MR_EXPRESSION, MR_PATTERN), AggregateFilter(CDP_7X), aggregate=True),
            TestCaseFilter(MatchExpression("MR", MR_EXPRESSION, MR_PATTERN), AggregateFilter(CDP_71X), aggregate=True),
            # 2 aggregated filters for YARN / MR: YARN-aggregated, MR-aggregated
            TestCaseFilter(MatchExpression("YARN", YARN_EXPRESSION, YARN_PATTERN), None, aggregate=True),
            TestCaseFilter(MatchExpression("MR", MR_EXPRESSION, MR_PATTERN), None, aggregate=True),
        ]
        return expected_filters

    def test_created_filters_simple_matched_line(self):
        expected_filters = self.simple_matched_line_filters()
        self.assertCountEqual(self.filters._SIMPLE_MATCHED_LINE_FILTERS, expected_filters)
        self.assertCountEqual(self.filters.get_non_aggregate_filters(), expected_filters)

    def test_created_filters_aggregation(self):
        expected_filters = self.all_aggregation_filters()
        self.assertCountEqual(self.filters._AGGREGATION_FILTERS, expected_filters)
        self.assertCountEqual(self.filters.get_aggregate_filters(), expected_filters)

    def test_created_filters_latest_failures(self):
        expected_filters = [
            # 2 filters for YARN: YARN-cdp7x-aggregated, YARN-cdp71x-aggregated,
            TestCaseFilter(
                MatchExpression("YARN", YARN_EXPRESSION, YARN_PATTERN), AggregateFilter(CDP_7X), aggregate=True
            ),
            TestCaseFilter(
                MatchExpression("YARN", YARN_EXPRESSION, YARN_PATTERN), AggregateFilter(CDP_71X), aggregate=True
            ),
            # 2 filters for MR: MR-cdp7x-aggregated, MR-cdp71x-aggregated,
            TestCaseFilter(MatchExpression("MR", MR_EXPRESSION, MR_PATTERN), AggregateFilter(CDP_7X), aggregate=True),
            TestCaseFilter(MatchExpression("MR", MR_EXPRESSION, MR_PATTERN), AggregateFilter(CDP_71X), aggregate=True),
        ]
        self.assertCountEqual(self.filters.LATEST_FAILURE_FILTERS, expected_filters)

    def test_created_filters_testcases_to_jiras(self):
        expected_filters = self.all_aggregation_filters()
        self.assertCountEqual(self.filters.TESTCASES_TO_JIRAS_FILTERS, expected_filters)

    def test_created_filters_all_valid_filters(self):
        expected_filters = self.all_aggregation_filters() + self.simple_matched_line_filters()
        self.assertCountEqual(self.filters.ALL_VALID_FILTERS, expected_filters)

    def test_union_of_aggregated_and_non_aggregated_is_valid_filters(self):
        all_valid = self.filters.ALL_VALID_FILTERS
        aggregate = self.filters.get_aggregate_filters()
        non_aggregate = self.filters.get_non_aggregate_filters()
        self.assertCountEqual(all_valid, aggregate + non_aggregate)

        # Test complements - intersection of non_aggregate vs. aggregate sets is empty
        self.assertCountEqual(set(aggregate).intersection(set(non_aggregate)), set())
