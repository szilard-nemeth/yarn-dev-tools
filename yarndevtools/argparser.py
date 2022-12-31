import logging
import sys

from yarndevtools.commands.backporter import Backporter
from yarndevtools.commands.branchcomparator.branch_comparator import BranchComparator
from yarndevtools.commands.format_patch_saver import FormatPatchSaver
from yarndevtools.commands.patch_saver import PatchSaver
from yarndevtools.commands.review_branch_creator import ReviewBranchCreator
from yarndevtools.commands.reviewsheetbackportupdater.review_sheet_backport_updater import ReviewSheetBackportUpdater
from yarndevtools.commands.reviewsync.reviewsync import ReviewSync
from yarndevtools.commands.send_latest_command_data_in_mail import SendLatestCommandDataInEmail
from yarndevtools.commands.unittestresultaggregator.unit_test_result_aggregator import UnitTestResultAggregator

# TODO
# from yarndevtools.commands.unittestresultaggregator.db.unit_test_result_aggregator_db import (
#     DatabaseUnitTestResultAggregator
# )
from yarndevtools.commands.unittestresultfetcher.unit_test_result_fetcher import (
    UnitTestResultFetcher,
)
from yarndevtools.commands.upstream_jira_patch_differ import UpstreamJiraPatchDiffer
from yarndevtools.commands.upstream_pr_fetcher import UpstreamPRFetcher
from yarndevtools.commands.upstreamumbrellafetcher.upstream_jira_umbrella_fetcher import UpstreamJiraUmbrellaFetcher
from yarndevtools.commands.zip_latest_command_data import ZipLatestCommandData

LOG = logging.getLogger(__name__)

if sys.version_info[:2] >= (3, 7):
    from argparse import ArgumentParser
else:
    LOG.info("Detected python version: " + str(sys.version_info[:2]))
    LOG.info("Replacing ArgumentParser with DelegatedArgumentParser for compatibility reasons.")
    from cdsw_compat import DelegatedArgumentParser as ArgumentParser


# TODO Migrate to click
class ArgParser:
    @staticmethod
    def parse_args():
        """This function parses and return arguments passed in"""

        # Top-level parser
        parser = ArgumentParser()

        # Subparsers
        subparsers = parser.add_subparsers(
            title="subcommands",
            description="valid subcommands",
            help="Available subcommands",
            required=True,
            dest="command",
        )
        PatchSaver.create_parser(subparsers)
        ReviewBranchCreator.create_parser(subparsers)
        Backporter.create_parser(subparsers)
        UpstreamPRFetcher.create_parser(subparsers)
        FormatPatchSaver.create_parser(subparsers)
        UpstreamJiraPatchDiffer.create_parser(subparsers)
        UpstreamJiraUmbrellaFetcher.create_parser(subparsers)
        BranchComparator.create_parser(subparsers)
        ZipLatestCommandData.create_parser(subparsers)
        SendLatestCommandDataInEmail.create_parser(subparsers)
        UnitTestResultFetcher.create_parser(subparsers)
        ReviewSheetBackportUpdater.create_parser(subparsers)
        ReviewSync.create_parser(subparsers)
        UnitTestResultAggregator.create_parser(subparsers)

        # Normal arguments
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            dest="verbose",
            default=None,
            required=False,
            help="More verbose log (including gitpython verbose logs)",
        )
        parser.add_argument(
            "-lt",
            "--trace",
            action="store_true",
            dest="logging_trace",
            default=False,
            required=False,
            help="Set log level to TRACE",
        )

        parser.add_argument(
            "-ld",
            "--debug",
            action="store_true",
            dest="logging_debug",
            default=False,
            required=False,
            help="Set log level to DEBUG",
        )

        parser.add_argument(
            "-lp",
            "--logging-pythoncommons",
            dest="logging_level_pythoncommons",
            type=str,
            default=None,
            required=False,
            help="Set log level of pythoncommons",
        )

        parser.add_argument(
            "-lg",
            "--logging-googleapiwrapper",
            dest="logging_level_googleapiwrapper",
            type=str,
            default=None,
            required=False,
            help="Set log level of google api wrapper",
        )

        args = parser.parse_args()
        if args.verbose:
            print("Args: " + str(args))
        return args, parser
