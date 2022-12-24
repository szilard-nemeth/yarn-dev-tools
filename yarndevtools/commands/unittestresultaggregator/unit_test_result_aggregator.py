import logging

from pythoncommons.file_utils import FileUtils
from pythoncommons.project_utils import ProjectUtils

from yarndevtools.commands.unittestresultaggregator.db.model import (
    UTResultAggregatorDatabase,
    DBWriterEmailContentProcessor,
)
from yarndevtools.commands.unittestresultaggregator.db.parser import DatabaseUnitTestResultAggregatorParser
from yarndevtools.commands.unittestresultaggregator.email.common import (
    EmailUtilsForAggregators,
    EmailContentAggregationResults,
)
from yarndevtools.commands.unittestresultaggregator.email.config import EmailBasedUnitTestResultAggregatorConfig
from yarndevtools.commands.unittestresultaggregator.email.parser import UnitTestResultAggregatorEmailParserUtils
from yarndevtools.commands.unittestresultaggregator.representation import UnitTestResultOutputManager, SummaryGenerator
from yarndevtools.commands_common import CommandAbs
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.yarn_dev_tools_config import YarnDevToolsConfig

CMD = CommandType.UNIT_TEST_RESULT_AGGREGATOR
LOG = logging.getLogger(__name__)


class UnitTestResultAggregator(CommandAbs):
    # TODO yarndevtoolsv2 DB: Gsheet should be a secondary 'DB', all data should be written to mongoDB first
    # TODO yarndevtoolsv2 DB: This class should aggregate email content data (collection: email_data) with Jenkins reports (collection: reports)
    def __init__(self, args, parser, output_dir: str):
        super().__init__()
        self.config = EmailBasedUnitTestResultAggregatorConfig(parser, args, output_dir)
        self._email_utils = EmailUtilsForAggregators(self.config, CMD)
        self._email_utils.init_gmail()
        self._known_test_failures = self._email_utils.fetch_known_test_failures()

        # TODO yarndevtoolsv2 DB: check for execution mode and only expect mongo config if required
        # if self.config.
        self._db = UTResultAggregatorDatabase(self.config.mongo_config)

    @staticmethod
    def create_parser(subparsers):
        # TODO yarndevtoolsv2: Choose parser based on execution mode
        # TODO yarndevtoolsv2 DB: Add all email-related options under a subparser --> if not specified, email data won't be loaded at all
        DatabaseUnitTestResultAggregatorParser.setup(subparsers)
        UnitTestResultAggregatorEmailParserUtils.create_parser(
            subparsers, CMD, func_to_execute=UnitTestResultAggregator.execute, add_gsheet_args=True
        )

    @staticmethod
    def execute(args, parser=None):
        # TODO yarndevtoolsv2 DB: implement DB-based execution
        output_dir = ProjectUtils.get_output_child_dir(CMD.output_dir_name)
        aggregator = UnitTestResultAggregator(args, parser, output_dir)
        FileUtils.create_symlink_path_dir(
            CMD.session_link_name,
            aggregator.config.session_dir,
            YarnDevToolsConfig.PROJECT_OUT_ROOT,
        )
        aggregator.run()

    def run(self):
        # TODO yarndevtoolsv2 DB: implement DB-based execution
        LOG.info(f"Starting Unit test result aggregator. Config: \n{str(self.config)}")
        gmail_query_result = self._email_utils.perform_gmail_query()
        result = EmailContentAggregationResults(self.config.testcase_filter_defs, self._known_test_failures)
        # TODO yarndevtoolsv2 DB: implement force mode flag that always scans all emails
        # TODO yarndevtoolsv2 DB: store dates of emails as well to mongodb: Write start date, end date, missing dates between start and end date
        # TODO yarndevtoolsv2 DB: Do not query gmail if not forced / required: Only query gmail results from a certain date that don't have mongo results
        # TODO yarndevtoolsv2 DB: Only add DBWriterEmailContentProcessor if execution mode dictates
        self._email_utils.process_gmail_results(
            gmail_query_result,
            result,
            split_body_by=self.config.email_content_line_sep,
            skip_lines_starting_with=self.config.skip_lines_starting_with,
            email_content_processors=[DBWriterEmailContentProcessor(self._db)],
        )

        # TODO yarndevtoolsv2 DB: only invoke this if required
        self._generate_summary_and_outputs(gmail_query_result, result)

    def _generate_summary_and_outputs(self, query_result, aggr_results):
        output_manager = UnitTestResultOutputManager(
            self.config.session_dir, self.config.console_mode, self._known_test_failures.gsheet_wrapper
        )
        SummaryGenerator.process_aggregation_results(aggr_results, query_result, self.config, output_manager)
