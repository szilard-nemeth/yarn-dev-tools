import logging

from pythoncommons.file_utils import FileUtils
from pythoncommons.project_utils import ProjectUtils

from yarndevtools.commands.unittestresultaggregator.email.common import (
    EmailBasedUnitTestResultAggregatorConfig,
    UnitTestResultAggregatorEmailParserUtils,
    EmailUtilsForAggregators,
    TestcaseFilterResults,
)
from yarndevtools.commands.unittestresultaggregator.representation import UnitTestResultOutputManager, SummaryGenerator
from yarndevtools.commands_common import CommandAbs
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.yarn_dev_tools_config import YarnDevToolsConfig

CMD = CommandType.UNIT_TEST_RESULT_AGGREGATOR_EMAIL
LOG = logging.getLogger(__name__)


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
