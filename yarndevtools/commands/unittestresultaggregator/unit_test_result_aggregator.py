import logging

from pythoncommons.file_utils import FileUtils
from pythoncommons.project_utils import ProjectUtils

from yarndevtools.commands.unittestresultaggregator.common.parser import UnitTestResultAggregatorCommonParserParams
from yarndevtools.commands.unittestresultaggregator.constants import ExecutionMode
from yarndevtools.commands.unittestresultaggregator.db.model import (
    UTResultAggregatorDatabase,
    DBWriterEmailContentProcessor,
    JenkinsJobBuildDataAndEmailContentJoiner,
)
from yarndevtools.commands.unittestresultaggregator.db.parser import UnitTestResultAggregatorDatabaseParserParams
from yarndevtools.commands.unittestresultaggregator.email.common import (
    EmailUtilsForAggregators,
)
from yarndevtools.commands.unittestresultaggregator.common.aggregation import AggregationResults
from yarndevtools.commands.unittestresultaggregator.email.config import UnitTestResultAggregatorConfig
from yarndevtools.commands.unittestresultaggregator.email.parser import UnitTestResultAggregatorEmailParserParams
from yarndevtools.commands.unittestresultaggregator.representation import UnitTestResultOutputManager, SummaryGenerator
from yarndevtools.commands_common import CommandAbs
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.yarn_dev_tools_config import YarnDevToolsConfig

CMD = CommandType.UNIT_TEST_RESULT_AGGREGATOR
LOG = logging.getLogger(__name__)


class UnitTestResultAggregator(CommandAbs):
    def __init__(self, args, parser, output_dir: str):
        super().__init__()
        # TODO yarndevtoolsv2 DB: should combine config instances: email + DB
        self.config = UnitTestResultAggregatorConfig(parser, args, output_dir)
        self._email_utils = EmailUtilsForAggregators(self.config, CMD)
        self._email_utils.init_gmail()
        self._known_test_failures = self._email_utils.fetch_known_test_failures()

        if self.config.should_use_db:
            self._db = UTResultAggregatorDatabase(self.config.mongo_config)
            self._joiner = JenkinsJobBuildDataAndEmailContentJoiner(self._db)

    @staticmethod
    def create_parser(subparsers):
        parser = subparsers.add_parser(
            CMD.name,
            help="Aggregates unit test results."
            "Example: "
            "--gsheet "
            "--gsheet-client-secret /Users/snemeth/.secret/dummy.json "
            "--gsheet-spreadsheet 'Failed testcases parsed from emails [generated by script]' "
            "--gsheet-worksheet 'Failed testcases'",
        )

        UnitTestResultAggregatorCommonParserParams.add_params(parser, add_gsheet_args=True)
        UnitTestResultAggregatorDatabaseParserParams.add_params(parser)
        UnitTestResultAggregatorEmailParserParams.add_params(parser)
        parser.set_defaults(func=UnitTestResultAggregator.execute)

    @staticmethod
    def execute(args, parser=None):
        output_dir = ProjectUtils.get_output_child_dir(CMD.output_dir_name)
        aggregator = UnitTestResultAggregator(args, parser, output_dir)
        FileUtils.create_symlink_path_dir(
            CMD.session_link_name,
            aggregator.config.session_dir,
            YarnDevToolsConfig.PROJECT_OUT_ROOT,
        )
        aggregator.run()

    def run(self):
        LOG.info(f"Starting Unit test result aggregator. Config: \n{str(self.config)}")

        email_content_processors = []
        if self.config.should_store_email_content_to_db:
            email_content_processors = [DBWriterEmailContentProcessor(self._db)]

        if (
            self.config.execution_mode == ExecutionMode.DB_ONLY
        ):  # TODO should use self.config.should_use_db later and fetch emails first
            result = AggregationResults(self.config.testcase_filter_defs, self._known_test_failures)
            self._joiner.join(result)

        if self.config.should_fetch_mails:
            gmail_query_result = self._email_utils.perform_gmail_query()
            # TODO yarndevtoolv2 DB: Create abstract version of EmailContentAggregationResults with 2 implementations: Email, DB
            result = AggregationResults(self.config.testcase_filter_defs, self._known_test_failures)
            self._email_utils.process_gmail_results(
                gmail_query_result,
                result,
                split_body_by=self.config.email_content_line_sep,
                skip_lines_starting_with=self.config.skip_lines_starting_with,
                email_content_processors=email_content_processors,
            )

            if self.config.should_generate_summary:
                self._generate_summary_and_outputs(gmail_query_result, result)

    def _generate_summary_and_outputs(self, query_result, aggr_results):
        output_manager = UnitTestResultOutputManager(
            self.config.session_dir, self.config.console_mode, self._known_test_failures.gsheet_wrapper
        )
        # TODO yarndevtoolsv2 DB: Gsheet should be a secondary 'DB', all data should be written to mongoDB first
        SummaryGenerator.process_aggregation_results(aggr_results, query_result, self.config, output_manager)
