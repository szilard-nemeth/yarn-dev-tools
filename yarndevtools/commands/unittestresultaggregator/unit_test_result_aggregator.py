import copy
import datetime
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict

from googleapiwrapper.common import ServiceType
from googleapiwrapper.gmail_api import GmailWrapper, GmailThreads
from googleapiwrapper.google_auth import GoogleApiAuthorizer
from googleapiwrapper.google_sheet import GSheetOptions, GSheetWrapper
from pythoncommons.file_utils import FileUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils
from pythoncommons.result_printer import BasicResultPrinter
from pythoncommons.string_utils import RegexUtils

LOG = logging.getLogger(__name__)

DEFAULT_LINE_SEP = "\\r\\n"


class OperationMode(Enum):
    GSHEET = "GSHEET"
    PRINT = "PRINT"


class UnitTestResultAggregatorConfig:
    def __init__(self, parser, args, output_dir: str):
        self._validate(parser, args)
        self.operation_mode = args.operation_mode
        self.validate_operation_mode()
        self.operation_mode = args.operation_mode
        self.request_limit = args.request_limit if hasattr(args, "request_limit") and args.request_limit else 1000000
        self.output_dir = ProjectUtils.get_session_dir_under_child_dir(FileUtils.basename(output_dir))
        self.full_cmd: str = OsUtils.determine_full_command_filtered(filter_password=True)

    def validate_operation_mode(self):
        if self.operation_mode == OperationMode.PRINT:
            LOG.info("Using operation mode: %s", OperationMode.PRINT)
        elif self.operation_mode == OperationMode.GSHEET:
            LOG.info("Using operation mode: %s", OperationMode.GSHEET)
        else:
            raise ValueError(
                "Unknown state! Operation mode should be either "
                "{} or {} but it is {}".format(OperationMode.PRINT, OperationMode.GSHEET, self.operation_mode)
            )

        # TODO
        # self.full_email_conf: FullEmailConfig = FullEmailConfig(args)
        # self.jenkins_url = args.jenkins_url
        # self.job_name = args.job_name
        # self.num_prev_days = args.num_prev_days
        # tc_filters_raw = args.tc_filters if hasattr(args, "tc_filters") and args.tc_filters else []
        # self.tc_filters: List[TestcaseFilter] = [TestcaseFilter(*tcf.split(":")) for tcf in tc_filters_raw]
        # if not self.tc_filters:
        #     LOG.warning("TESTCASE FILTER IS NOT SET!")
        #
        # self.send_mail: bool = not args.skip_mail
        # self.enable_file_cache: bool = not args.disable_file_cache

    @staticmethod
    def _validate(parser, args):
        # TODO check existence + readability of secret file!!
        if args.gsheet and (
            args.gsheet_client_secret is None or args.gsheet_spreadsheet is None or args.gsheet_worksheet is None
        ):
            parser.error(
                "--gsheet requires the following arguments: "
                "--gsheet-client-secret, --gsheet-spreadsheet and --gsheet-worksheet."
            )

        if args.do_print:
            args.operation_mode = OperationMode.PRINT
        elif args.gsheet:
            args.operation_mode = OperationMode.GSHEET
            args.gsheet_options = GSheetOptions(
                args.gsheet_client_secret, args.gsheet_spreadsheet, args.gsheet_worksheet
            )
        else:
            LOG.info(f"Unknown operation mode! Current args: {args}")
        LOG.info(f"Using operation mode: {args.operation_mode}")
        return args

    def __str__(self):
        return (
            f"Full command was: {self.full_cmd}\n"
            f"Request limit: {self.request_limit}\n"
            # TODO
            # f"Jenkins job name: {self.job_name}\n"
            # f"Number of days to check: {self.num_prev_days}\n"
            # f"Testcase filters: {self.tc_filters}\n"
        )


@dataclass
class MatchedLinesFromMessage:
    message_id: str
    thread_id: str
    subject: str
    date: datetime.datetime
    lines: List[str] = field(default_factory=list)


class UnitTestResultAggregator:
    def __init__(self, args, parser, output_dir):
        self.config = UnitTestResultAggregatorConfig(parser, args, output_dir)
        if self.config.operation_mode == OperationMode.GSHEET:
            self.gsheet_wrapper_normal = GSheetWrapper(args.gsheet_options)
            gsheet_options = copy.copy(args.gsheet_options)
            gsheet_options.worksheet = gsheet_options.worksheet + "_aggregated"
            self.gsheet_wrapper_aggregated = GSheetWrapper(gsheet_options)

        # TODO pass argument: credentials_filename
        self.authorizer = GoogleApiAuthorizer(ServiceType.GMAIL)
        self.gmail_wrapper = GmailWrapper(self.authorizer)

    def run(self):
        LOG.info("Starting Unit test result aggregator. " "Config: \n" f"{str(self.config)}")
        # TODO Query mapreduce failures to separate sheet
        # TODO implement caching of emails in json files
        # TODO Split by [] --> Example: org.apache.hadoop.yarn.util.resource.TestResourceCalculator.testDivisionByZeroRatioNumeratorAndDenominatorIsZero[1]
        query = 'subject:"YARN Daily unit test report"'
        # TODO Add these to postprocess config object (including mimetype filtering)
        regex = ".*org\\.apache\\.hadoop\\.yarn.*"
        skip_lines_starting_with = ["Failed testcases:", "FILTER:"]

        # TODO this query below produced some errors: Uncomment & try again
        # query = "YARN Daily branch diff report"
        threads: GmailThreads = self.gmail_wrapper.query_threads_with_paging(
            query=query, limit=self.config.request_limit
        )
        # TODO write a generator function to GmailThreads that generates List[GmailMessageBodyPart]
        raw_data = self.filter_data_by_regex_pattern(threads, regex, skip_lines_starting_with)
        self.process_data(raw_data)

    def filter_data_by_regex_pattern(self, threads, regex, skip_lines_starting_with, line_sep=DEFAULT_LINE_SEP):
        matched_lines: List[MatchedLinesFromMessage] = []
        for message in threads.messages:
            msg_parts = message.get_all_plain_text_parts()
            for msg_part in msg_parts:
                lines = msg_part.body.split(line_sep)
                matched_lines_of_msg: List[str] = []
                for line in lines:
                    line = line.strip()
                    # TODO this compiles the pattern over and over again --> Create a new helper function that receives a compiled pattern
                    if not self._check_if_line_is_valid(line, skip_lines_starting_with):
                        LOG.warning(f"Skipping line: {line}")
                        continue
                    if RegexUtils.ensure_matches_pattern(line, regex):
                        LOG.debug(f"[PATTERN: {regex}] Matched line: {line}")
                        matched_lines_of_msg.append(line)

                matched_lines.append(
                    MatchedLinesFromMessage(
                        message.msg_id, message.thread_id, message.subject, message.date, matched_lines_of_msg
                    )
                )
        LOG.debug(f"[RAW DATA] Matched lines: {matched_lines}")
        return matched_lines

    def process_data(self, raw_data: List[MatchedLinesFromMessage]):
        truncate = self.config.operation_mode == OperationMode.PRINT
        header = ["Date", "Subject", "Testcase", "Message ID", "Thread ID"]
        converted_data: List[List[str]] = DataConverter.convert_data_to_rows(raw_data, truncate=truncate)
        self.print_results_table(header, converted_data)

        if self.config.operation_mode == OperationMode.GSHEET:
            LOG.info("Updating Google sheet with data...")
            header_aggregated = ["Testcase", "Frequency of failures", "Latest failure"]
            aggregated_data: List[List[str]] = DataConverter.convert_data_to_aggregated_rows(raw_data)
            self.update_gsheet(header, converted_data)
            self.update_gsheet_aggregated(header_aggregated, aggregated_data)

    @staticmethod
    def _check_if_line_is_valid(line, skip_lines_starting_with):
        valid_line = True
        for skip_str in skip_lines_starting_with:
            if line.startswith(skip_str):
                valid_line = False
                break
        return valid_line

    @staticmethod
    def print_results_table(header, data):
        BasicResultPrinter.print_table(data, header)

    def update_gsheet(self, header, data):
        self.gsheet_wrapper_normal.write_data(header, data, clear_range=False)

    def update_gsheet_aggregated(self, header, data):
        self.gsheet_wrapper_aggregated.write_data(header, data, clear_range=False)


class DataConverter:
    SUBJECT_MAX_LENGTH = 50
    LINE_MAX_LENGTH = 80

    @staticmethod
    def convert_data_to_rows(raw_data: List[MatchedLinesFromMessage], truncate: bool = False) -> List[List[str]]:
        converted_data: List[List[str]] = []
        truncate_subject: bool = truncate
        truncate_lines: bool = truncate

        for matched_lines in raw_data:
            for testcase_name in matched_lines.lines:
                subject = matched_lines.subject
                if truncate_subject and len(matched_lines.subject) > DataConverter.SUBJECT_MAX_LENGTH:
                    subject = DataConverter._truncate_str(
                        matched_lines.subject, DataConverter.SUBJECT_MAX_LENGTH, "subject"
                    )
                if truncate_lines:
                    testcase_name = DataConverter._truncate_str(
                        testcase_name, DataConverter.LINE_MAX_LENGTH, "testcase"
                    )
                row: List[str] = [
                    str(matched_lines.date),
                    subject,
                    testcase_name,
                    matched_lines.message_id,
                    matched_lines.thread_id,
                ]
                converted_data.append(row)
        return converted_data

    @staticmethod
    def convert_data_to_aggregated_rows(raw_data: List[MatchedLinesFromMessage]) -> List[List[str]]:
        failure_freq: Dict[str, int] = {}
        latest_failure: Dict[str, datetime.datetime] = {}
        for matched_lines in raw_data:
            for testcase_name in matched_lines.lines:
                if testcase_name not in failure_freq:
                    failure_freq[testcase_name] = 1
                    latest_failure[testcase_name] = matched_lines.date
                else:
                    failure_freq[testcase_name] = failure_freq[testcase_name] + 1
                    if latest_failure[testcase_name] < matched_lines.date:
                        latest_failure[testcase_name] = matched_lines.date

        converted_data: List[List[str]] = []
        for tc, freq in failure_freq.items():
            last_failed = latest_failure[tc]
            row: List[str] = [tc, freq, str(last_failed)]
            converted_data.append(row)
        return converted_data

    @staticmethod
    def _truncate_str(value: str, max_len: int, field_name: str):
        orig_value = value
        truncated = value[0:max_len] + "..."
        LOG.debug(
            f"Truncated {field_name}: "
            f"Original value: '{orig_value}', "
            f"Original length: {len(orig_value)}, "
            f"New value (truncated): {truncated}, "
            f"New length: {max_len}"
        )
        return truncated

    @staticmethod
    def _truncate_date(date):
        original_date = date
        date_obj = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
        truncated = date_obj.strftime("%Y-%m-%d")
        LOG.debug(f"Truncated date. " f"Original value: {original_date}," f"New value (truncated): {truncated}")
        return truncated
