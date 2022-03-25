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
from yarndevtools.commands.unittestresultaggregator.unit_test_result_aggregator import (
    UnitTestResultAggregator,
)
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
        # TODO Move execute calls to create_parser?
        PatchSaver.create_parser(subparsers, PatchSaver.execute)
        ReviewBranchCreator.create_parser(subparsers, ReviewBranchCreator.execute)
        Backporter.create_parser(subparsers, Backporter.execute)
        UpstreamPRFetcher.create_parser(subparsers, UpstreamPRFetcher.execute)
        FormatPatchSaver.create_parser(subparsers, FormatPatchSaver.execute)
        UpstreamJiraPatchDiffer.create_parser(subparsers, UpstreamJiraPatchDiffer.execute)
        UpstreamJiraUmbrellaFetcher.create_parser(subparsers, UpstreamJiraUmbrellaFetcher.execute)
        BranchComparator.create_parser(subparsers, BranchComparator.execute)
        ZipLatestCommandData.create_parser(subparsers, ZipLatestCommandData.execute)
        SendLatestCommandDataInEmail.create_parser(subparsers, SendLatestCommandDataInEmail.execute)
        UnitTestResultFetcher.create_parser(subparsers, UnitTestResultFetcher.execute)
        ReviewSheetBackportUpdater.create_parser(subparsers, ReviewSheetBackportUpdater.execute)
        ReviewSync.create_parser(subparsers, ReviewSync.execute)
        UnitTestResultAggregator.create_parser(subparsers, UnitTestResultAggregator.execute)

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
            "-d",
            "--debug",
            action="store_true",
            dest="debug",
            default=None,
            required=False,
            help="Turn on console debug level logs",
        )

        args = parser.parse_args()
        if args.verbose:
            print("Args: " + str(args))
        return args, parser
