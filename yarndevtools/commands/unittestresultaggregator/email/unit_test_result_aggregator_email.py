import logging
from collections import defaultdict
from typing import List, Dict, Tuple

from googleapiwrapper.gmail_domain import GmailMessage
from pythoncommons.file_utils import FileUtils
from pythoncommons.project_utils import ProjectUtils
from pythoncommons.string_utils import RegexUtils

from yarndevtools.commands.unittestresultaggregator.common import (
    MATCHTYPE_ALL_POSTFIX,
    AGGREGATED_WS_POSTFIX,
    TestCaseFilter,
    TestCaseFilters,
    FailedTestCaseAggregated,
    get_key_by_testcase_filter,
    EmailMetaData,
    MATCH_ALL_LINES_EXPRESSION,
    MatchExpression,
    FailedTestCases,
    BuildComparisonResult,
    FailedTestCaseFactory,
    FailedTestCaseAbs,
    KnownTestFailures,
)
from yarndevtools.commands.unittestresultaggregator.email.common import (
    EmailBasedUnitTestResultAggregatorConfig,
    UnitTestResultAggregatorEmailParserUtils,
    EmailUtilsForAggregators,
)
from yarndevtools.commands.unittestresultaggregator.representation import UnitTestResultOutputManager, SummaryGenerator
from yarndevtools.commands_common import CommandAbs
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.yarn_dev_tools_config import YarnDevToolsConfig

CMD = CommandType.UNIT_TEST_RESULT_AGGREGATOR_EMAIL
LOG = logging.getLogger(__name__)


# TODO yarndevtoolsv2: consider extracting common aggregation logic from this class / or create abstraction layer?
class TestcaseFilterResults:
    def __init__(self, testcase_filters: TestCaseFilters, known_failures: KnownTestFailures):
        self._match_all_lines: bool = TestcaseFilterResults._should_match_all_lines(testcase_filters)
        self._testcase_filters: TestCaseFilters = testcase_filters
        self._known_failures: KnownTestFailures = known_failures
        self._failed_testcases: FailedTestCases = FailedTestCases()
        self._failed_testcases.init_with_testcase_filters(self._testcase_filters.ALL_VALID_FILTERS)

        # This is a temporary dict - usually for a context of a message
        self._matched_lines_dict: Dict[str, List[str]] = {}
        self._str_key_to_testcase_filter: Dict[str, TestCaseFilter] = {}

    @staticmethod
    def _should_match_all_lines(testcase_filters):
        match_all_lines: bool = testcase_filters.match_all_lines()
        LOG.info(
            "**Matching all lines"
            if match_all_lines
            else f"**Matching lines with regex pattern: {testcase_filters.match_expressions}"
        )
        return match_all_lines

    def start_new_context(self):
        # Prepare matched_lines dict with all required empty-lists for ALL filters
        self._matched_lines_dict = defaultdict(list)
        filters: List[TestCaseFilter] = self._testcase_filters.ALL_VALID_FILTERS
        for tcf in filters:
            self._matched_lines_dict[self._get_matched_lines_key(tcf)] = []

        # Do sanity check
        generated_keys = [self._get_matched_lines_key(tcf) for tcf in filters]
        unique_keys = set(generated_keys)
        if len(filters) != len(unique_keys):
            raise ValueError(
                "Mismatch in number of testcase filter objects and generated keys. "
                f"Filters: {filters}, "
                f"Generated keys: {generated_keys}, "
                f"Unique keys: {unique_keys}."
            )

    def match_line(self, line, mail_subject: str):
        matches_any_pattern, matched_expression = self._does_line_match_any_match_expression(line, mail_subject)
        if self._match_all_lines or matches_any_pattern:
            self._matched_lines_dict[MATCHTYPE_ALL_POSTFIX].append(line)
            self._add_match_to_matched_lines_dict(line, matched_expression, aggregate_values=[True, False])

            for aggr_filter in self._testcase_filters.aggregate_filters:
                if aggr_filter.val in mail_subject:
                    LOG.debug(
                        f"Found matching email subject for aggregation filter '{aggr_filter}': "
                        f"Subject: {mail_subject}"
                    )
                    tcf = TestCaseFilter(matched_expression, aggr_filter)
                    self._matched_lines_dict[self._get_matched_lines_key(tcf)].append(line)

    def _add_match_to_matched_lines_dict(self, line, matched_expression, aggregate_values: List[bool]):
        for aggr_value in aggregate_values:
            tcf = TestCaseFilter(matched_expression, aggr_filter=None, aggregate=aggr_value)
            self._matched_lines_dict[self._get_matched_lines_key(tcf)].append(line)

    def _does_line_match_any_match_expression(self, line, mail_subject: str) -> Tuple[bool, MatchExpression or None]:
        for match_expression in self._testcase_filters.match_expressions:
            if RegexUtils.ensure_matches_pattern(line, match_expression.pattern):
                LOG.debug(f"Matched line: {line} [Mail subject: {mail_subject}]")
                return True, match_expression
        LOG.debug(f"Line did not match for any pattern: {line}")
        # TODO in strict mode, unmatching lines should not be allowed
        return False, None

    def _get_matched_lines_key(self, tcf: TestCaseFilter) -> str:
        if tcf.match_expr == MATCH_ALL_LINES_EXPRESSION:
            key = MATCHTYPE_ALL_POSTFIX + f"_{AGGREGATED_WS_POSTFIX}" if tcf.aggregate else MATCHTYPE_ALL_POSTFIX
            self._str_key_to_testcase_filter[key] = TestCaseFilter(MATCH_ALL_LINES_EXPRESSION, None)
            return key
        key = get_key_by_testcase_filter(tcf)
        if key not in self._str_key_to_testcase_filter:
            self._str_key_to_testcase_filter[key] = tcf
        return key

    def finish_context(self, message: GmailMessage):
        LOG.info("Finishing context...")
        LOG.debug(f"Keys of of matched lines: {self._matched_lines_dict.keys()}")

        for key, matched_lines in self._matched_lines_dict.items():
            if not matched_lines:
                continue
            tcf: TestCaseFilter = self._str_key_to_testcase_filter[key]
            for matched_line in matched_lines:
                email_meta = EmailMetaData(message.msg_id, message.thread_id, message.subject, message.date)
                failed_testcase = FailedTestCaseFactory.create_from_email(matched_line, email_meta)
                self._failed_testcases.add_failure(tcf, failed_testcase)

        self._failed_testcases.print_keys()
        # Make sure temp dict is not used until next cycle
        self._matched_lines_dict = None

    def finish_processing_all(self):
        self.print_objects()

        for tcf in self._testcase_filters.ALL_VALID_FILTERS:
            self._failed_testcases.init_comparison_results(tcf)

        self._failed_testcases.aggregate(self._testcase_filters.get_aggregate_filters())
        self._failed_testcases.create_latest_failures(
            self._testcase_filters.LATEST_FAILURE_FILTERS, only_last_results=True
        )
        self._failed_testcases.create_changed_failures_comparison(
            self._testcase_filters.LATEST_FAILURE_FILTERS, compare_with_last=True
        )
        self._failed_testcases.cross_check_testcases_with_jiras(
            self._testcase_filters.TESTCASES_TO_JIRAS_FILTERS, self._known_failures
        )

    def get_failed_testcases_by_filter(self, tcf: TestCaseFilter) -> List[FailedTestCaseAbs]:
        return self._failed_testcases.get(tcf)

    def get_latest_failed_testcases_by_filter(self, tcf: TestCaseFilter) -> List[FailedTestCaseAbs]:
        return self._failed_testcases.get_latest_testcases(tcf)

    def get_build_comparison_result_by_filter(self, tcf: TestCaseFilter) -> BuildComparisonResult:
        return self._failed_testcases.get_build_comparison_results(tcf)

    def get_aggregated_testcases_by_filter(
        self, tcf: TestCaseFilter, filter_unknown=False, filter_reoccurred=False
    ) -> List[FailedTestCaseAggregated]:
        local_vars = locals()
        applied_filters = [name for name in local_vars if name.startswith("filter_") and local_vars[name]]
        filtered_tcs = self._failed_testcases.get_aggregated_testcases(tcf)
        original_length = len(filtered_tcs)
        prev_length = original_length
        if filter_unknown:
            filtered_tcs = list(filter(lambda tc: not tc.known_failure, filtered_tcs))
            LOG.debug(
                f"Filtering for unknown TCs. "
                f"Previous length of aggregated TCs: {prev_length}, "
                f"New length of filtered aggregated TCs: {len(filtered_tcs)}"
            )
            prev_length = len(filtered_tcs)
        if filter_reoccurred:
            filtered_tcs = list(filter(lambda tc: tc.reoccurred, filtered_tcs))
            LOG.debug(
                f"Filtering for reoccurred TCs. "
                f"Previous length of aggregated TCs: {prev_length}, "
                f"New length of filtered aggregated TCs: {len(filtered_tcs)}"
            )
            prev_length = len(filtered_tcs)

        LOG.debug(
            "Returning filtered aggregated TCs. "
            f"Original length of ALL aggregated TCs: {original_length}, "
            f"Length of filtered aggregated TCs: {prev_length}, "
            f"Applied filters: {applied_filters}"
        )
        return filtered_tcs

    def print_objects(self):
        pass
        # TODO should be trace logged
        # LOG.debug(f"All failed testcase objects: {self._failed_testcases}")


class EmailBasedUnitTestResultAggregator(CommandAbs):
    def __init__(self, args, parser, output_dir: str):
        super().__init__()
        self.config = EmailBasedUnitTestResultAggregatorConfig(parser, args, output_dir)
        self._email_utils = EmailUtilsForAggregators(self.config, CMD)
        self.known_test_failures = self._email_utils.fetch_known_test_failures()
        self.gmail_wrapper = self._email_utils.setup_gmail_wrapper()

    @staticmethod
    def create_parser(subparsers):
        UnitTestResultAggregatorEmailParserUtils.create_parser(
            subparsers, CMD, func_to_execute=EmailBasedUnitTestResultAggregator.execute, add_gsheet_args=True
        )

    @staticmethod
    def execute(args, parser=None):
        output_dir = ProjectUtils.get_output_child_dir(CMD.output_dir_name)
        aggregator = EmailBasedUnitTestResultAggregator(args, parser, output_dir)
        FileUtils.create_symlink_path_dir(
            CMD.session_link_name,
            aggregator.config.session_dir,
            YarnDevToolsConfig.PROJECT_OUT_ROOT,
        )
        aggregator.run()

    def run(self):
        LOG.info(f"Starting Unit test result aggregator. Config: \n{str(self.config)}")
        gmail_query_result = self._email_utils.perform_gmail_query()
        result = TestcaseFilterResults(self.config.testcase_filters, self.known_test_failures)
        self._email_utils.process_gmail_results(
            gmail_query_result,
            result,
            split_body_by=self.config.email_content_line_sep,
            skip_lines_starting_with=self.config.skip_lines_starting_with,
        )
        self._post_process(gmail_query_result, result)

    def _post_process(self, query_result, tc_filter_results):
        output_manager = UnitTestResultOutputManager(
            self.config.session_dir, self.config.console_mode, self.known_test_failures.gsheet_wrapper
        )
        SummaryGenerator.process_testcase_filter_results(tc_filter_results, query_result, self.config, output_manager)
