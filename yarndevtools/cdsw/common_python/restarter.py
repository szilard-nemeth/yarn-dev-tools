import logging
import os
import sys

from pythoncommons.os_utils import OsUtils

from yarndevtools.cdsw.common_python.constants import CdswEnvVar

LOG = logging.getLogger(__name__)


class Restarter:
    @staticmethod
    def restart_execution(cdsw_runner_script_path):
        """
         Variable values in case of CDSW run:
         1. sys.executable: /usr/local/bin/python3
         2. sys.argv: ['/usr/local/bin/ipython3']
         3. final command: ['/usr/local/bin/python3',
                   '/home/cdsw/.local/lib/python3.8/site-packages/yarndevtools/cdsw/unit-test-result-reporting/cdsw_runner.py']

         Variable values under normal run (e.g. from test_branchdiff_reporter.py):
         1. sys.executable: /usr/local/bin/python3
         2. sys.argv: ['/home/cdsw/jobs/downstream-branchdiff-reporting/cdsw_runner.py']
         3. final command: ['/usr/local/bin/python3', '/usr/local/lib/python3.8/site-packages/yarndevtools/cdsw/downstream-branchdiff-reporting/cdsw_runner.py']
        :return:
        """

        # Let's pick two random environment variables that are starting with name "CDSW_" to decide if we are running on CDSW
        real_cdsw_env = OsUtils.get_env_value("CDSW_PROJECT", None) and OsUtils.get_env_value("CDSW_ENGINE_URL", None)
        if real_cdsw_env:
            LOG.info("Detected real CDSW environment")
            if not sys.argv:
                raise ValueError("Was expecting sys.argv to be not empty! Current value: {}".format(sys.argv))
            elif len(sys.argv) > 1:
                raise ValueError("Was expecting sys.argv to have a length of 1! Current value: {}".format(sys.argv))
            argv0 = sys.argv[0]
            if "ipython" not in argv0:
                raise ValueError("Was expecting sys.argv[] to contain 'ipython'! Current value: {}".format(argv0))
            executable = sys.executable
            command_args = [argv0, cdsw_runner_script_path]
        else:
            LOG.info("Detected artificial CDSW environment")
            executable = sys.executable
            command_args = [cdsw_runner_script_path]

        LOG.info(
            "Restarting Python process. " "sys.executable: %s, sys.argv: %s, executable: %s, command args: %s",
            sys.executable,
            sys.argv,
            executable,
            command_args,
        )
        # Prevent running the restart process forever
        OsUtils.set_env_value(CdswEnvVar.RESTART_PROCESS_WHEN_REQUIREMENTS_INSTALLED.value, False)
        os.execvp(executable, command_args)
