#!/usr/bin/env python3
import os
import sys

# THESE FUNCTION DEFINITIONS AND CALL TO fix_pythonpast MUST PRECEDE THE IMPORT OF libreloader: from libreloader import reload_dependencies
PYTHONPATH_ENV_VAR = "PYTHONPATH"


def get_pythonpath():
    return os.environ[PYTHONPATH_ENV_VAR]


def set_env_value(env, value):
    os.environ[env] = value


def fix_pythonpath(additional_dir):
    pypath = PYTHONPATH_ENV_VAR
    if pypath in os.environ:
        print(f"Old {pypath}: {get_pythonpath()}")
        set_env_value(pypath, f"{get_pythonpath()}:{additional_dir}")
        print(f"New {pypath}: {get_pythonpath()}")
    else:
        print(f"Old {pypath}: not set")
        set_env_value(pypath, additional_dir)
        print(f"New {pypath}: {get_pythonpath()}")
    sys.path.append(additional_dir)
    print("Fixed sys.path: " + str(sys.path))
    print("Fixed PYTHONPATH: " + str(os.environ["PYTHONPATH"]))


scripts_dir = os.path.join("/home", "cdsw", "scripts")
jobs_dir = os.path.join("/home", "cdsw", "jobs")
fix_pythonpath(scripts_dir)

# NOW IT'S SAFE TO IMPORT LIBRELOADER
# IGNORE FLAKE8: E402 module level import not at top of file
from libreloader import reload_dependencies  # DO NOT REMOVE !! # noqa: E402

print(f"Name of the script      : {sys.argv[0]=}")
print(f"Arguments of the script : {sys.argv[1:]=}")
if len(sys.argv) != 2:
    raise ValueError("Should only have one argument, the name of the job!")

reload_dependencies.Reloader.start()

# Start the CDSW runner
job_name = sys.argv[1]
sys.argv.append("--config-dir")
sys.argv.append(jobs_dir)
cdsw_runner_path = os.path.join(scripts_dir, "cdsw_runner.py")
exec(open(cdsw_runner_path).read())
