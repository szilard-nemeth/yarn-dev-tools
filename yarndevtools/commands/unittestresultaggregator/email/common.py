import logging
from collections import defaultdict
from pprint import pformat
from typing import List, Dict, Tuple, Iterable

from googleapiwrapper.common import ServiceType
from googleapiwrapper.gmail_api import GmailWrapper, ThreadQueryResults
from googleapiwrapper.google_auth import GoogleApiAuthorizer
from googleapiwrapper.google_sheet import GSheetWrapper
from pythoncommons.string_utils import RegexUtils
from pythoncommons.url_utils import UrlUtils

from yarndevtools.cdsw.constants import SECRET_PROJECTS_DIR
from yarndevtools.commands.unittestresultaggregator.common.aggregation import (
    AggregatedTestFailures,
    LatestTestFailures,
    TestFailureComparison,
    KnownTestFailureChecker,
)
from yarndevtools.commands.unittestresultaggregator.common.model import (
    BuildComparisonResult,
    FailedTestCaseAggregated,
    TestCaseFilter,
    TestCaseFilterDefinitions,
    FailedTestCaseAbs,
    FailedTestCaseFactory,
    FinalAggregationResults,
    EmailMetaData,
    TestCaseFilters,
    AggregatedFailurePropertyFilter,
    EmailContentProcessor,
)
from yarndevtools.commands.unittestresultaggregator.constants import (
    OperationMode,
    MATCH_ALL_LINES_EXPRESSION,
    MatchExpression,
)
from yarndevtools.commands.unittestresultaggregator.gsheet import KnownTestFailures
from yarndevtools.common.common_model import JenkinsJobUrl

LOG = logging.getLogger(__name__)

SUBJECT = "subject:"
DEFAULT_LINE_SEP = "\\r\\n"


class EmailContentAggregationResults:
    # TODO yarndevtoolsv2 refactor: consider extracting common aggregation logic from this class / or create abstraction layer?
    def __init__(self, testcase_filter_defs: TestCaseFilterDefinitions, known_failures: KnownTestFailures):
        self._match_all_lines: bool = self._should_match_all_lines(testcase_filter_defs)
        self._testcase_filter_defs: TestCaseFilterDefinitions = testcase_filter_defs
        self._known_failures: KnownTestFailures = known_failures
        self._aggregation_results: FinalAggregationResults = FinalAggregationResults(
            self._testcase_filter_defs.ALL_VALID_FILTERS
        )

        # This is a temporary dict - usually for a context of a message
        self._matched_lines_dict: Dict[TestCaseFilter, List[str]] = {}
        self._all_matching_tcf = TestCaseFilter(MATCH_ALL_LINES_EXPRESSION, None)

    @staticmethod
    def _should_match_all_lines(testcase_filter_defs):
        match_all_lines: bool = testcase_filter_defs.match_all_lines()
        LOG.info(
            "**Matching all lines"
            if match_all_lines
            else f"**Matching lines with regex pattern: {testcase_filter_defs.match_expressions}"
        )
        return match_all_lines

    def start_new_context(self):
        # Prepare matched_lines dict with all required empty-lists for ALL filters
        self._matched_lines_dict = defaultdict(list)
        filters: TestCaseFilters = self._testcase_filter_defs.ALL_VALID_FILTERS
        for tcf in filters:
            self._matched_lines_dict[tcf] = []

        # Do sanity check
        generated_keys = [tcf.key() for tcf in filters]
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
            self._matched_lines_dict[self._all_matching_tcf].append(line)
            self._add_match_to_matched_lines_dict(line, matched_expression, aggregate_values=[True, False])

            for aggr_filter in self._testcase_filter_defs.aggregate_filters:
                if aggr_filter.val in mail_subject:
                    LOG.debug(
                        f"Found matching email subject for aggregation filter '{aggr_filter}': "
                        f"Subject: {mail_subject}"
                    )
                    tcf = TestCaseFilter(matched_expression, aggr_filter, aggregate=True)
                    self._matched_lines_dict[tcf].append(line)

    def _add_match_to_matched_lines_dict(self, line, matched_expression, aggregate_values: List[bool]):
        for aggr_value in aggregate_values:
            tcf = TestCaseFilter(matched_expression, aggr_filter=None, aggregate=aggr_value)
            self._matched_lines_dict[tcf].append(line)

    def _does_line_match_any_match_expression(self, line, mail_subject: str) -> Tuple[bool, MatchExpression or None]:
        for match_expression in self._testcase_filter_defs.match_expressions:
            if RegexUtils.ensure_matches_pattern(line, match_expression.pattern):
                LOG.debug(f"Matched line: {line} [Mail subject: {mail_subject}]")
                return True, match_expression
        LOG.debug(f"Line did not match for any pattern: {line}")
        # TODO in strict mode, unmatching lines should not be allowed
        return False, None

    def finish_context(self, email_meta):
        LOG.info("Finishing context...")
        LOG.debug(f"Keys of of matched lines: {self._matched_lines_dict.keys()}")

        for tcf, matched_lines in self._matched_lines_dict.items():
            if not matched_lines:
                continue
            for matched_line in matched_lines:
                failed_testcase = FailedTestCaseFactory.create_from_email(matched_line, email_meta)
                self._aggregation_results.add_failure(tcf, failed_testcase)
        self._aggregation_results.save_failed_build(email_meta)

        self._aggregation_results.print_keys()
        # Make sure temp dict is not used until next cycle
        self._matched_lines_dict = None

    def finish_processing_all(self):
        self.print_objects()

        self._aggregation_results._aggregated = AggregatedTestFailures(
            self._testcase_filter_defs.get_aggregate_filters(),
            self._aggregation_results.test_failures,
        )
        self._aggregation_results._latest_failures = LatestTestFailures(
            self._testcase_filter_defs.LATEST_FAILURE_FILTERS,
            self._aggregation_results.test_failures,
            only_last_results=True,
        )
        self._aggregation_results._comparison = TestFailureComparison(
            self._testcase_filter_defs.LATEST_FAILURE_FILTERS,
            self._aggregation_results.test_failures,
            compare_with_last=True,
        )
        self._aggregation_results._known_failure_checker = KnownTestFailureChecker(
            self._testcase_filter_defs.TESTCASES_TO_JIRAS_FILTERS,
            self._known_failures,
            self._aggregation_results._aggregated,
        )

    def get_failures(self, tcf: TestCaseFilter) -> List[FailedTestCaseAbs]:
        return self._aggregation_results.get_failure(tcf)

    def get_latest_failures(self, tcf: TestCaseFilter) -> List[FailedTestCaseAbs]:
        return self._aggregation_results.get_latest_failures(tcf)

    def get_build_comparison(self, tcf: TestCaseFilter) -> BuildComparisonResult:
        return self._aggregation_results.get_build_comparison(tcf)

    def get_aggregated_testcases_by_filters(
        self, tcf: TestCaseFilter, *prop_filters: AggregatedFailurePropertyFilter
    ) -> List[FailedTestCaseAggregated]:
        return self._aggregation_results.get_aggregated_failures_by_filter(tcf, *prop_filters)

    def print_objects(self):
        builds_with_dates = self._aggregation_results._failed_builds.get_dates()
        LOG.debug("Printing available builds per job...")
        for job_name, dates in builds_with_dates.items():
            LOG.debug("Job: %s, builds: %s", job_name, dates)

        # TODO should be trace logged
        # LOG.debug(f"All failed testcase objects: {self._failed_testcases}")


class EmailUtilsForAggregators:
    def __init__(self, config, command_type):
        self.config = config
        self.command_type = command_type
        self.gmail_wrapper = None

    def init_gmail(self):
        self.gmail_wrapper = self.setup_gmail_wrapper()

    def setup_gmail_wrapper(self):
        google_auth = GoogleApiAuthorizer(
            ServiceType.GMAIL,
            project_name=f"{self.command_type.output_dir_name}",
            secret_basedir=SECRET_PROJECTS_DIR,
            account_email=self.config.account_email,
        )
        return GmailWrapper(google_auth, output_basedir=self.config.email_cache_dir)

    def fetch_known_test_failures(self):
        if self.config.operation_mode == OperationMode.GSHEET:
            gsheet_wrapper = GSheetWrapper(self.config.gsheet_options)
            return KnownTestFailures(gsheet_wrapper=gsheet_wrapper, gsheet_jira_table=self.config.gsheet_jira_table)
        return None

    def get_gmail_query(self):
        original_query = self.config.gmail_query
        if self.config.smart_subject_query and original_query.startswith(SUBJECT):
            real_subject = original_query.split(SUBJECT)[1]
            logical_expressions = [" and ", " or "]
            if any(x in real_subject.lower() for x in logical_expressions):
                LOG.warning(f"Detected logical expression in query, won't modify original query: {original_query}")
                return original_query
            if " " in real_subject and real_subject[0] != '"':
                fixed_subject = f'"{real_subject}"'
                new_query = SUBJECT + fixed_subject
                LOG.info(
                    f"Fixed Gmail query string.\n"
                    f"Original query string: {original_query}\n"
                    f"New query string: {new_query}"
                )
                return new_query
        return original_query

    def perform_gmail_query(self):
        query_result: ThreadQueryResults = self.gmail_wrapper.query_threads(
            query=self.get_gmail_query(), limit=self.config.request_limit, expect_one_message_per_thread=True
        )
        LOG.info(
            f"Gmail thread query result summary:\n"
            f"--> Number of threads: {query_result.no_of_threads}\n"
            f"--> Number of messages: {query_result.no_of_messages}\n"
            f"--> Number of unique subjects: {len(query_result.unique_subjects)}\n"
            f"--> Unique subjects: {pformat(query_result.unique_subjects)}"
        )
        return query_result

    @staticmethod
    def check_if_line_is_valid(line, skip_lines_starting_with):
        for skip_str in skip_lines_starting_with:
            if line.startswith(skip_str):
                return False
        return True

    @staticmethod
    def process_gmail_results(
        query_result: ThreadQueryResults,
        result: EmailContentAggregationResults,
        split_body_by: str,
        skip_lines_starting_with: List[str],
        email_content_processors: Iterable[EmailContentProcessor] = None,
    ):
        if not email_content_processors:
            email_content_processors = []

        for message in query_result.threads.messages:
            LOG.debug("Processing message: %s", message.subject)

            for msg_part in message.get_all_plain_text_parts():
                lines = msg_part.body_data.split(split_body_by)
                lines = list(map(lambda line: line.strip(), lines))
                email_meta = EmailUtilsForAggregators._create_email_meta(message)
                for processor in email_content_processors:
                    processor.process(message, email_meta, lines)

                result.start_new_context()
                for line in lines:
                    # TODO this compiles the pattern over and over again --> Create a new helper function that receives a compiled pattern
                    if not EmailUtilsForAggregators.check_if_line_is_valid(line, skip_lines_starting_with):
                        LOG.warning(f"Skipping invalid line: {line} [Mail subject: {message.subject}]")
                        continue
                    result.match_line(line, message.subject)

                result.finish_context(email_meta)
        result.finish_processing_all()

    @staticmethod
    def _create_email_meta(message):
        build_url = UrlUtils.extract_from_str(message.subject)
        if not build_url:
            return EmailMetaData(message.msg_id, message.thread_id, message.subject, message.date, None, None)
        jenkins_url = JenkinsJobUrl(build_url)
        return EmailMetaData(
            message.msg_id, message.thread_id, message.subject, message.date, build_url, jenkins_url.job_name
        )
