import logging
import tempfile
from io import BufferedWriter
from typing import List
from pythoncommons.file_utils import FileUtils
from pythoncommons.string_utils import StringUtils
from pythoncommons.zip_utils import ZipFileUtils

from yarndevtools.argparser import CommandType
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
        return f"command_data_{cmd_type.name}.zip"


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

        input_files: List[str] = self.config.input_files
        sum_len_all_files: int = 0
        all_ignores_files: int = 0
        if self.config.ignore_filetypes:
            input_files = []
            # TODO move this whole thing to pythoncommons
            for input_file in self.config.input_files:
                if FileUtils.is_dir(input_file):
                    all_files = FileUtils.find_files(input_file, regex=".*", full_path_result=True)
                    sum_len_all_files += len(all_files)
                    files_to_ignore = set()
                    for ext in self.config.ignore_filetypes:
                        new_files_to_ignore = FileUtils.find_files(input_file, extension=ext, full_path_result=True)
                        all_ignores_files += len(new_files_to_ignore)
                        LOG.debug(
                            f"Found {len(new_files_to_ignore)} files to ignore in directory '{input_file}': "
                            f"{StringUtils.list_to_multiline_string(files_to_ignore)}"
                        )
                        files_to_ignore.update(new_files_to_ignore)

                    files_to_keep = list(set(all_files).difference(files_to_ignore))
                    tmp_dir: tempfile.TemporaryDirectory = tempfile.TemporaryDirectory()
                    tmp_dir_path = tmp_dir.name
                    FileUtils.copy_files_to_dir(files_to_keep, tmp_dir_path, cut_path=input_file)
                    input_files.append(tmp_dir_path)
                else:
                    input_files.append(input_file)
                    sum_len_all_files += 1

        temp_dir_dest: bool = True if not self.config.output_dir or self.config.output_dir.startswith("/tmp") else False
        if self.config.output_dir:
            dest_filepath = FileUtils.join_path(self.config.output_dir, self.config.dest_filename)
            zip_file: BufferedWriter = ZipFileUtils.create_zip_file(input_files, dest_filepath, compress=True)
        else:
            zip_file: BufferedWriter = ZipFileUtils.create_zip_as_tmp_file(
                input_files, self.config.dest_filename, compress=True
            )

        zip_file_name = zip_file.name
        no_of_files_in_zip: int = ZipFileUtils.get_number_of_files_in_zip(zip_file_name)
        if self.config.ignore_filetypes and (sum_len_all_files - all_ignores_files) != no_of_files_in_zip:
            raise ValueError(
                f"Unexpected number of files in zip. "
                f"All files: {sum_len_all_files}, "
                f"all ignored files: {all_ignores_files}, "
                f"number of files in zip: {no_of_files_in_zip}, "
                f"zip file: {zip_file_name}"
            )

        LOG.info(
            f"Finished writing command data to zip file: {zip_file_name}, "
            f"size: {FileUtils.get_file_size(zip_file_name)}"
        )
        FileUtils.create_symlink_path_dir(LATEST_DATA_ZIP_LINK_NAME, zip_file_name, self.config.project_out_root)

        # Create a latest link for the command as well
        FileUtils.create_symlink_path_dir(
            self.cmd_type.command_data_zip_name, zip_file_name, self.config.project_out_root
        )

        # Save command data file per command to home dir when temp dir mode is being used
        if temp_dir_dest:
            zip_file_name_real: str = f"{self.cmd_type.command_data_name}-real.zip"
            target_file_path = FileUtils.join_path(self.config.project_out_root, FileUtils.basename(zip_file_name_real))
            FileUtils.copy_file(zip_file_name, target_file_path)
