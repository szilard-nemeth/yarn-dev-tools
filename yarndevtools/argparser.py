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
    def parse_args(yarn_dev_tools):
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
        # TODO Move yarn_dev_tools methods to CommandAbs
        PatchSaver.create_parser(subparsers, yarn_dev_tools.save_patch)
        ReviewBranchCreator.create_parser(subparsers, yarn_dev_tools.create_review_branch)
        Backporter.create_parser(subparsers, yarn_dev_tools.backport_c6)
        UpstreamPRFetcher.create_parser(subparsers, yarn_dev_tools.upstream_pr_fetch)
        FormatPatchSaver.create_parser(subparsers, yarn_dev_tools.save_patches)
        UpstreamJiraPatchDiffer.create_parser(subparsers, yarn_dev_tools.diff_patches_of_jira)
        UpstreamJiraUmbrellaFetcher.create_parser(subparsers, yarn_dev_tools.fetch_jira_umbrella_data)
        BranchComparator.create_parser(subparsers, yarn_dev_tools.branch_comparator)
        ZipLatestCommandData.create_parser(subparsers, yarn_dev_tools.zip_latest_command_data)
        SendLatestCommandDataInEmail.create_parser(subparsers, yarn_dev_tools.send_latest_command_data)
        UnitTestResultFetcher.create_parser(subparsers, yarn_dev_tools.fetch_send_jenkins_test_report)
        ReviewSheetBackportUpdater.create_parser(subparsers, yarn_dev_tools.review_sheet_backport_updater)
        ReviewSync.create_parser(subparsers, yarn_dev_tools.reviewsync)
        UnitTestResultAggregator.create_parser(subparsers, yarn_dev_tools.unit_test_result_aggregator)

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
