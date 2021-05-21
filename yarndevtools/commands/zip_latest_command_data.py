import logging
import tempfile
from typing import List
from pythoncommons.file_utils import FileUtils
from pythoncommons.string_utils import StringUtils
from pythoncommons.zip_utils import ZipFileUtils

from yarndevtools.argparser import CommandType
from yarndevtools.constants import (
    LATEST_LOG_LINK_NAME,
    LATEST_DATA_ZIP_LINK_NAME,
)

LOG = logging.getLogger(__name__)


class Config:
    def __init__(self, args, input_files: List[str], project_basedir):
        self.input_files = input_files
        self.output_dir = args.dest_dir if "dest_dir" in args else None
        self.dest_filename = args.dest_filename
        self.project_out_root = project_basedir
        self.ignore_filetypes = args.ignore_filetypes if "ignore_filetypes" in args else []


class ZipLatestCommandData:
    def __init__(self, args, project_basedir: str):
        cmd_type_str: str = args.cmd_type
        self.cmd_type: CommandType = CommandType.from_str(cmd_type_str)
        input_files = [LATEST_LOG_LINK_NAME, self.cmd_type.session_link_name]
        self.input_files = self._validate(input_files, project_basedir)
        self.config = Config(args, self.input_files, project_basedir)

    def _validate(self, input_files: List[str], project_basedir: str):
        return self._check_input_files(input_files, project_basedir)

    def _check_input_files(self, input_files: List[str], project_basedir: str):
        LOG.info(f"Checking provided input files. Command: {self.cmd_type}, Files: {input_files}")
        resolved_files = [FileUtils.join_path(project_basedir, f) for f in input_files]
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

        if self.config.output_dir:
            dest_filepath = FileUtils.join_path(self.config.output_dir, self.config.dest_filename)
            zip_file = ZipFileUtils.create_zip_file(input_files, dest_filepath)
        else:
            zip_file = ZipFileUtils.create_zip_as_tmp_file(input_files, self.config.dest_filename)

        no_of_files_in_zip: int = ZipFileUtils.get_number_of_files_in_zip(zip_file.name)
        if self.config.ignore_filetypes and (sum_len_all_files - all_ignores_files) != no_of_files_in_zip:
            raise ValueError(
                f"Unexpected number of files in zip. "
                f"All files: {sum_len_all_files}, "
                f"all ignored files: {all_ignores_files}, "
                f"number of files in zip: {no_of_files_in_zip}, "
                f"zip file: {zip_file.name}"
            )

        LOG.info(
            f"Finished writing command data to zip file: {zip_file.name}, " f"size: {FileUtils.get_file_size(zip_file)}"
        )
        FileUtils.create_symlink_path_dir(LATEST_DATA_ZIP_LINK_NAME, zip_file.name, self.config.project_out_root)
