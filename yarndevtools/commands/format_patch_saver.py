import logging
import os
from os.path import expanduser
from typing import Callable

from git import InvalidGitRepositoryError
from pythoncommons.date_utils import DateUtils
from pythoncommons.file_utils import FileUtils

from pythoncommons.git_wrapper import GitWrapper

from yarndevtools.commands_common import CommandAbs
from yarndevtools.common.shared_command_utils import CommandType

LOG = logging.getLogger(__name__)


class FormatPatchSaver(CommandAbs):
    """
    A class used to export git-format-patch files from a git repository to a specified target directory.

    Attributes
    ----------
    base_refspec : str
        The refspec to use as the base git reference for format-patch comparison.
        Specified with args.
    other_refspec : str
        The refspec to use as the ending git reference for format-patch comparison.
        Specified with args.
    dest_basedir : str
        Base directory of the format-patch result files.
        Specified with args.
    dest_dir_prefix : str
        Jira ID of the upstream jira to backport.
        Specified with args.
    working_dir : str
        Path to the git repository.
    dir_suffix : str
        The final directory to put the results into.
    repo : GitWrapper
        A GitWrapper object, representing the repository.
    patch_file_dest_dir : str
        A path, pointing to the final directory where the format-patch results will be placed.

    Methods
    -------
    run()
        Executes this command.
        The steps are roughly are:
        1. Ensure that the provided working directory is a directory that contains a git repository.
        2. Validate refspecs, ensuring that the two refspecs are different and pointing to a valid commit or branch.
        3. Ensure that the destination directory is created.
        4. Execute git format-patch and save the result files to the target directory.
    """

    def __init__(self, args, working_dir, dir_suffix):
        # Coming from args
        self.base_refspec = args.base_refspec
        self.other_refspec = args.other_refspec
        self.dest_basedir = args.dest_basedir
        self.dest_dir_prefix = args.dest_dir_prefix

        self.working_dir = working_dir
        self.dir_suffix = dir_suffix

        # Dynamic attributes
        self.repo = None
        self.patch_file_dest_dir = None

    @staticmethod
    def create_parser(subparsers):
        parser = subparsers.add_parser(
            CommandType.SAVE_DIFF_AS_PATCHES.name,
            help="Diffs branches and creates patch files with "
            "git format-patch and saves them to a directory."
            "Example: <command> master gpu",
        )
        parser.add_argument("base_refspec", type=str, help="Git base refspec to diff with.")
        parser.add_argument("other_refspec", type=str, help="Git other refspec to diff with.")
        parser.add_argument("dest_basedir", type=str, help="Destination basedir.")
        parser.add_argument("dest_dir_prefix", type=str, help="Directory as prefix to export the patch files to.")
        parser.set_defaults(func=FormatPatchSaver.execute)

    @staticmethod
    def execute(args, parser=None):
        format_patch_saver = FormatPatchSaver(args, os.getcwd(), DateUtils.get_current_datetime())
        format_patch_saver.run()

    def run(self):
        # TODO check if git is clean (no modified, unstaged files, etc)
        self.ensure_git_repository()
        self.validate_refspecs()
        self.ensure_dest_dir_is_created()
        self.run_format_patch()

    def ensure_git_repository(self):
        try:
            repo = GitWrapper(self.working_dir)
            self.repo = repo
        except InvalidGitRepositoryError:
            raise ValueError(f"Current working directory is not a git repo: {self.working_dir}")

    def validate_refspecs(self):
        if self.base_refspec == self.other_refspec:
            raise ValueError(
                f"Specified base refspec '{self.base_refspec}' is the same as other refspec '{self.other_refspec}'"
            )

        exists = self.repo.is_branch_exist(self.base_refspec)
        if not exists:
            raise ValueError(f"Specified base refspec is not valid: {self.base_refspec}")

        exists = self.repo.is_branch_exist(self.other_refspec)
        if not exists:
            raise ValueError(f"Specified other refspec is not valid: {self.base_refspec}")

    def ensure_dest_dir_is_created(self):
        dest_basedir = expanduser(self.dest_basedir)
        self.patch_file_dest_dir = FileUtils.join_path(dest_basedir, self.dest_dir_prefix, self.dir_suffix)
        FileUtils.ensure_dir_created(self.patch_file_dest_dir)

    def run_format_patch(self):
        refspec = f"{self.base_refspec}..{self.other_refspec}"
        LOG.info("Saving git patches based on refspec '%s', to directory: %s", refspec, self.patch_file_dest_dir)
        self.repo.format_patch(refspec, output_dir=self.patch_file_dest_dir, full_index=True)
