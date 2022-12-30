from typing import Callable

from yarndevtools.commands.unittestresultaggregator.constants import SummaryMode
from yarndevtools.commands.unittestresultaggregator.email.processor import DEFAULT_LINE_SEP
from yarndevtools.commands_common import GSheetArguments, MongoArguments, ArgumentParserUtils
from yarndevtools.common.shared_command_utils import CommandType


class UnitTestResultAggregatorEmailParserParams:
    @staticmethod
    def add_params(parser):
        parser.add_argument(
            "--account-email",
            required=True,
            type=str,
            help="Email address of Gmail account that will be used to Gmail API authentication and fetching data.",
        )

        parser.add_argument(
            "-q",
            "--gmail-query",
            required=True,
            type=str,
            help="Gmail query string that will be used to get emails to parse.",
        )

        parser.add_argument(
            "--smart-subject-query",
            action="store_true",
            default=False,
            help="Whether to fix Gmail queries like: 'Subject: YARN Daily unit test report', "
            "where the subject should have been between quotes.",
        )

        parser.add_argument(
            "-s",
            "--skip-lines-starting-with",
            required=False,
            type=str,
            nargs="+",
            help="If lines starting with these strings, they will not be considered as a line to parse",
        )

        parser.add_argument(
            "-l",
            "--request-limit",
            dest="request_limit",
            type=int,
            help="Limit the number of API requests",
        )

        parser.add_argument("--email-content-line-separator", type=str, default=DEFAULT_LINE_SEP)

        parser.add_argument(
            "--truncate-subject",
            dest="truncate_subject",
            type=str,
            help="Whether to truncate subject in outputs. The specified string will be cropped "
            "from the full value of subject strings when printing them to any destination.",
        )
