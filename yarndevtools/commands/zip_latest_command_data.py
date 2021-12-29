import logging
from typing import List
from pythoncommons.file_utils import FileUtils
from pythoncommons.zip_utils import ZipFileUtils

from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.constants import (
    LATEST_DATA_ZIP_LINK_NAME,
)

LOG = logging.getLogger(__name__)


class Config:
    def __init__(self, args, input_files: List[str], project_basedir, cmd_type: CommandType):
        self.input_files = input_files
        self.output_dir = args.dest_dir if "dest_dir" in args else None
        self.project_out_root = project_basedir
        self.ignore_filetypes = args.ignore_filetypes if "ignore_filetypes" in args else []
        self.dest_filename = self._get_dest_filename(args, cmd_type)

    @staticmethod
    def _get_dest_filename(args, cmd_type: CommandType):
        fname = args.dest_filename
        if fname:
            LOG.info(f"Using overridden destination filename: {fname}")
        else:
            fname = Config._get_filename_by_command(cmd_type)
        return fname

    @staticmethod
    def _get_filename_by_command(cmd_type: CommandType):
        return f"command_data_{cmd_type.real_name}.zip"


class ZipLatestCommandData:
    def __init__(self, args, project_basedir: str):
        self.cmd_type: CommandType = CommandType.from_str(args.cmd_type)

        # Log link name examples:
        # latest-log-unit_test_result_aggregator-INFO.log
        # latest-log-unit_test_result_aggregator-DEBUG.log
        self.input_files = self._check_input_files(
            [self.cmd_type.log_link_name + "*", self.cmd_type.session_link_name], project_basedir
        )
        self.config = Config(args, self.input_files, project_basedir, self.cmd_type)

    def _check_input_files(self, input_files: List[str], project_basedir: str):
        LOG.info(f"Checking provided input files. Command: {self.cmd_type}, Files: {input_files}")

        resolved_files = []
        for fname in input_files:
            if "*" in fname:
                fname = fname.replace("*", ".*")
                found_files = FileUtils.find_files(
                    project_basedir, regex=fname, single_level=True, full_path_result=True
                )
                LOG.info("Found files for pattern '%s': %s", fname, found_files)
                resolved_files.extend(found_files)
            else:
                resolved_files.append(FileUtils.join_path(project_basedir, fname))
        not_found_files = []

        # Sanity check
        for f in resolved_files:
            exists = FileUtils.does_file_exist(f)
            if not exists:
                not_found_files.append(f)
        if len(not_found_files) > 0:
            raise ValueError(f"The following files could not be found: {not_found_files}")
        LOG.info(f"Listing resolved input files. Command: {self.cmd_type}, Files: {resolved_files}")
        return resolved_files

    def run(self):
        LOG.info(
            "Starting zipping latest command data... \n "
            f"PLEASE NOTE THAT ACTUAL OUTPUT DIR AND DESTINATION FILES CAN CHANGE, IF NOT SPECIFIED\n"
            f"Output dir: {self.config.output_dir}\n"
            f"Input files: {self.config.input_files}\n "
            f"Destination filename: {self.config.dest_filename}\n "
            f"Ignore file types: {self.config.ignore_filetypes}\n "
        )

        zip_file_name, temp_dir_dest = ZipFileUtils.create_zip_file_advanced(
            self.config.input_files, self.config.dest_filename, self.config.ignore_filetypes, self.config.output_dir
        )
        FileUtils.create_symlink_path_dir(LATEST_DATA_ZIP_LINK_NAME, zip_file_name, self.config.project_out_root)

        # Create the latest link for the command as well
        FileUtils.create_symlink_path_dir(
            self.cmd_type.command_data_zip_name, zip_file_name, self.config.project_out_root
        )

        # Save command data file per command to home dir when temp dir mode is being used
        if temp_dir_dest:
            zip_file_name_real: str = f"{self.cmd_type.command_data_name}-real.zip"
            target_file_path = FileUtils.join_path(self.config.project_out_root, FileUtils.basename(zip_file_name_real))
            FileUtils.copy_file(zip_file_name, target_file_path)
