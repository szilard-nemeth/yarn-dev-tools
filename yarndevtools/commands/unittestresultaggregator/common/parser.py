from yarndevtools.commands.unittestresultaggregator.constants import SummaryMode, ExecutionMode
from yarndevtools.commands_common import ArgumentParserUtils, GSheetArguments


class UnitTestResultAggregatorCommonParserParams:
    @staticmethod
    def add_params(parser, add_gsheet_args=True):
        if add_gsheet_args:
            gsheet_group = GSheetArguments.add_gsheet_arguments(parser)

            gsheet_group.add_argument(
                "--gsheet-compare-with-jira-table",
                dest="gsheet_compare_with_jira_table",
                type=str,
                help="This should be provided if comparison of failed testcases with reported jira table must be performed. "
                "The value is a name to a worksheet, for example 'testcases with jiras'.",
            )

        parser.add_argument(
            "--execution-mode",
            dest="execution_mode",
            type=ExecutionMode,
            choices=list(ExecutionMode),
            help="Execution mode of aggregation",
        )

        parser.add_argument(
            "-m",
            "--match-expression",
            required=False,
            type=ArgumentParserUtils.matches_match_expression_pattern,
            nargs="+",
            help="Line matcher expression, this will be converted to a regex. "
            "For example, if expression is org.apache, the regex will be .*org\\.apache\\.* "
            "Only lines in the mail content matching for this expression will be considered as a valid line.",
        )

        parser.add_argument(
            "--abbreviate-testcase-package",
            dest="abbrev_testcase_package",
            type=str,
            help="Whether to abbreviate testcase package names in outputs in order to save screen space. "
            "The specified string will be abbreviated with the starting letters."
            "For example, specifying 'org.apache.hadoop.yarn' will be converted to 'o.a.h.y' "
            "when printing testcase names to any destination.",
        )
        parser.add_argument(
            "--summary-mode",
            dest="summary_mode",
            type=str,
            choices=[sm.value for sm in SummaryMode],
            default=SummaryMode.HTML.value,
            help="Summary file(s) will be written in this mode. Defaults to HTML.",
        )

        parser.add_argument(
            "--aggregate-filters",
            dest="aggregate_filters",
            required=True,
            type=str,
            nargs="+",
            help="Execute some post filters on the email results. "
            "The resulted emails and testcases for each filter will be aggregated to "
            "a separate worksheet with name <WS>_aggregated_<aggregate-filter> where WS is equal to the "
            "value specified by the --gsheet-worksheet argument.",
        )

        exclusive_group = parser.add_mutually_exclusive_group(required=True)
        exclusive_group.add_argument(
            "-p", "--print", action="store_true", dest="do_print", help="Print results to console", required=False
        )

        exclusive_group.add_argument(
            "-g",
            "--gsheet",
            action="store_true",
            dest="gsheet",
            default=False,
            required=False,
            help="Export values to Google sheet. Additional gsheet arguments need to be specified!",
        )
