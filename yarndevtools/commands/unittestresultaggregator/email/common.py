import logging
from pprint import pformat
from typing import List, Iterable

from googleapiwrapper.common import ServiceType
from googleapiwrapper.gmail_api import GmailWrapper, ThreadQueryResults
from googleapiwrapper.gmail_domain import GmailMessage
from googleapiwrapper.google_auth import GoogleApiAuthorizer
from googleapiwrapper.google_sheet import GSheetWrapper
from pythoncommons.url_utils import UrlUtils

from yarndevtools.cdsw.constants import SECRET_PROJECTS_DIR
from yarndevtools.commands.unittestresultaggregator.common.aggregation import (
    FailedBuildAbs,
    AggregationResults,
)
from yarndevtools.commands.unittestresultaggregator.common.model import (
    EmailMetaData,
    EmailContentProcessor,
)
from yarndevtools.commands.unittestresultaggregator.constants import (
    OperationMode,
)
from yarndevtools.commands.unittestresultaggregator.gsheet import KnownTestFailures
from yarndevtools.common.common_model import JenkinsJobUrl

LOG = logging.getLogger(__name__)

SUBJECT = "subject:"
DEFAULT_LINE_SEP = "\\r\\n"


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
        result: AggregationResults,
        split_body_by: str,
        skip_lines_starting_with: List[str],
        email_content_processors: Iterable[EmailContentProcessor] = None,
    ):
        if not email_content_processors:
            email_content_processors = []

        for message in query_result.threads.messages:
            email_meta = EmailUtilsForAggregators._create_email_meta(message)
            failed_build = FailedBuildAbs.create_from_email(email_meta)
            LOG.debug("Processing message: %s", failed_build.origin())

            for msg_part in message.get_all_plain_text_parts():
                lines = msg_part.body_data.split(split_body_by)
                lines = list(map(lambda line: line.strip(), lines))

                for processor in email_content_processors:
                    processor.process(email_meta, lines)

                result.start_new_context()
                for line in lines:
                    if not EmailUtilsForAggregators.check_if_line_is_valid(line, skip_lines_starting_with):
                        LOG.warning(f"Skipping invalid line: {line} [Mail subject: {failed_build.origin()}]")
                        continue
                    result.match_testcase(line, failed_build.job_name())

                result.finish_context(failed_build)
        result.finish_processing_all()

    @staticmethod
    def _create_email_meta(message: GmailMessage):
        build_url = UrlUtils.extract_from_str(message.subject)
        if not build_url:
            return EmailMetaData(message.msg_id, message.thread_id, message.subject, message.date, None, None)
        jenkins_url = JenkinsJobUrl(build_url)
        return EmailMetaData(
            message.msg_id, message.thread_id, message.subject, message.date, build_url, jenkins_url.job_name
        )
