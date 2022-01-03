#!/usr/bin/env python3
import os
import sys

import libreloader.reload_dependencies  # DO NOT REMOVE !!

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


print(f"Name of the script      : {sys.argv[0]=}")
print(f"Arguments of the script : {sys.argv[1:]=}")
if len(sys.argv) != 2:
    raise ValueError("Should only have one argument, the name of the job!")

downloaded_scripts_dir = os.path.join(os.path.expanduser("~"), "cdsw", "downloaded_scripts")
fix_pythonpath(downloaded_scripts_dir)
job_name = sys.argv[1]
script_path = os.path.join(os.path.expanduser("~"), "cdsw", "jobs", job_name, "cdsw_runner.py")
exec(open(script_path).read())
