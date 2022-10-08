#!/usr/bin/python3
import logging
import os
import shutil
import site
import subprocess
import sys
from typing import List

LOG = logging.getLogger(__name__)
CDSW_BASEDIR = os.path.join("/home", "cdsw")
YARN_DEV_TOOLS_JOBS_BASEDIR = os.path.join(CDSW_BASEDIR, "jobs")  # Same as CommonDirs.YARN_DEV_TOOLS_JOBS_BASEDIR

MODULE_MODE_GLOBAL = "global"
MODULE_MODE_USER = "user"
ACCEPTED_PYTHON_MODULE_MODES = [MODULE_MODE_USER, MODULE_MODE_GLOBAL]  # Same as values of PythonModuleMode
PYTHON_MODULE_MODE_ENV_VAR = "PYTHON_MODULE_MODE"  # Same as CdswEnvVar.PYTHON_MODULE_MODE
INSTALL_REQUIREMENTS_ENV_VAR = "INSTALL_REQUIREMENTS"  # Same as CdswEnvVar.INSTALL_REQUIREMENTS
TEST_EXECUTION_MODE_ENV_VAR = "TEST_EXEC_MODE"  # Same as CdswEnvVar.TEST_EXECUTION_MODE
YARNDEVTOOLS_MODULE_NAME = "yarndevtools"
DEFAULT_TEST_EXECUTION_MODE = "cloudera"  # Same as TestExecMode.CLOUDERA.value


class Reloader:
    YARN_DEV_TOOLS_MODULE_ROOT = None
    CONFIGS_ROOT_DIR = None
    INSTALL_REQUIREMENTS_SCRIPT = None

    @classmethod
    def start(cls):
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
        cls._setup_paths()
        cls._install_requirements_if_needed()
        LOG.info("{} Finished execution succesfully".format(sys.argv[0]))

    @classmethod
    def _setup_paths(cls):
        module_root = cls.get_python_module_root()
        cls.YARN_DEV_TOOLS_MODULE_ROOT = os.path.join(module_root, YARNDEVTOOLS_MODULE_NAME)
        cls.CONFIGS_ROOT_DIR = os.path.join(cls.YARN_DEV_TOOLS_MODULE_ROOT, "cdsw", "job_configs")
        cls.INSTALL_REQUIREMENTS_SCRIPT = os.path.join(
            cls.YARN_DEV_TOOLS_MODULE_ROOT, "cdsw", "scripts", "install-requirements.sh"
        )
        LOG.info("YARN dev tools module root is: %s", cls.YARN_DEV_TOOLS_MODULE_ROOT)
        cls._check_mandatory_scripts()

    @classmethod
    def _check_mandatory_scripts(cls):
        if not os.path.isfile(cls.INSTALL_REQUIREMENTS_SCRIPT):
            raise ValueError(
                "Cannot find file {}. Make sure you ran the initial-cdsw-setup.sh script once!".format(
                    cls.INSTALL_REQUIREMENTS_SCRIPT
                )
            )

    @classmethod
    def _install_requirements_if_needed(cls):
        install_requirements = True
        if INSTALL_REQUIREMENTS_ENV_VAR in os.environ:
            env_var_value = os.environ[INSTALL_REQUIREMENTS_ENV_VAR]
            if env_var_value == "True" or env_var_value is True:
                install_requirements = True
            else:
                install_requirements = False
        if install_requirements:
            cls._run_install_requirements_script()
        else:
            LOG.warning("Skipping installation of Python requirements as per configuration!")

    @classmethod
    def get_python_module_root(cls):
        # For CDSW execution, user python module mode is preferred.
        # For test execution, it depends on how the initial-cdsw-setup.sh script was executed in the container.
        python_module_mode = MODULE_MODE_USER
        if PYTHON_MODULE_MODE_ENV_VAR in os.environ:
            python_module_mode = os.environ[PYTHON_MODULE_MODE_ENV_VAR]
            LOG.info("Found python module mode from env var '%s': %s", PYTHON_MODULE_MODE_ENV_VAR, python_module_mode)

        if python_module_mode not in ACCEPTED_PYTHON_MODULE_MODES:
            raise ValueError(
                "Accepted python module modes: {}. Provided module mode: {}".format(
                    ACCEPTED_PYTHON_MODULE_MODES, python_module_mode
                )
            )

        LOG.info("Using Python module mode: %s", python_module_mode)
        if python_module_mode == MODULE_MODE_GLOBAL:
            python_site = site.getsitepackages()[0]
            LOG.info("Using global python-site basedir: %s", python_site)
        elif python_module_mode == MODULE_MODE_USER:
            python_site = site.USER_SITE
            LOG.info("Using user python-site basedir: %s", python_site)
        else:
            raise ValueError("Invalid python module mode: {}".format(python_module_mode))

        return python_site

    @classmethod
    def _run_install_requirements_script(cls, exit_on_nonzero_exitcode=False):
        """
        Do not exit on non-zero exit code as pip can fail to remove residual package files on NFS.
        See: https://github.com/pypa/pip/issues/6327
        :param exit_on_nonzero_exitcode:
        :return:
        """
        exec_mode = DEFAULT_TEST_EXECUTION_MODE
        if TEST_EXECUTION_MODE_ENV_VAR in os.environ:
            exec_mode = os.environ[TEST_EXECUTION_MODE_ENV_VAR]
        cls._run_script(
            cls.INSTALL_REQUIREMENTS_SCRIPT, args=[exec_mode], exit_on_nonzero_exitcode=exit_on_nonzero_exitcode
        )
        cls._copy_job_configs_to_cdsw_jobs_root()

    @classmethod
    def _copy_job_configs_to_cdsw_jobs_root(cls):
        # IMPORTANT: CDSW is able to launch linked scripts, but cannot modify and save the job's form because it thinks
        # the linked script is not there.
        LOG.info("Copying jobs to place...")
        for subdir, dirs, files in os.walk(cls.CONFIGS_ROOT_DIR):
            for file in files:
                filepath = subdir + os.sep + file
                if filepath.endswith(".py"):
                    cls.copy_file(filepath, os.path.join(YARN_DEV_TOOLS_JOBS_BASEDIR, file))

    @classmethod
    def remove_dir(cls, dir, force=False):
        if force:
            shutil.rmtree(dir, ignore_errors=True)
        else:
            os.rmdir(dir)

    @classmethod
    def create_new_dir(cls, path):
        if not os.path.exists(path):
            os.makedirs(path)
        else:
            raise ValueError("Directory already exist: %s", path)

    @classmethod
    def copy_file(cls, src, dest):
        LOG.info(f"Copying file: {src} -> {dest}")
        shutil.copyfile(src, dest)

    @classmethod
    def _run_script(cls, script, args: List[str], exit_on_nonzero_exitcode=True):
        LOG.info("Running script: %s", script)
        proc = subprocess.Popen(["/bin/bash", "-x", script, *args], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        _ = proc.communicate()
        if proc.returncode != 0 and exit_on_nonzero_exitcode:
            raise ValueError(f"Failed to execute {script}")
