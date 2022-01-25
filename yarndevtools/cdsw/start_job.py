#!/usr/bin/env python3
import os
import sys

# THESE FUNCTION DEFINITIONS AND CALL TO fix_pythonpast MUST PRECEDE THE IMPORT OF libreloader: from libreloader import reload_dependencies
PYTHONPATH_ENV_VAR = "PYTHONPATH"


def get_pythonpath():
    return os.environ[PYTHONPATH_ENV_VAR]


def set_env_value(env, value):
    os.environ[env] = value


def add_to_pythonpath(additional_dir):
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
    print("Fixed PYTHONPATH: " + str(os.environ[pypath]))


# Only used script is the libreloader from /home/cdsw/scripts/
cdsw_home_dir = os.path.join("/home", "cdsw")
scripts_dir = os.path.join(cdsw_home_dir, "scripts")
jobs_dir = os.path.join(cdsw_home_dir, "jobs")
add_to_pythonpath(scripts_dir)

# NOW IT'S SAFE TO IMPORT LIBRELOADER
# IGNORE FLAKE8: E402 module level import not at top of file
from libreloader import reload_dependencies  # DO NOT REMOVE !! # noqa: E402
from libreloader.reload_dependencies import YARNDEVTOOLS_MODULE_NAME, Reloader  # DO NOT REMOVE !! # noqa: E402

print(f"Name of the script      : {sys.argv[0]=}")
print(f"Arguments of the script : {sys.argv[1:]=}")
if len(sys.argv) != 2:
    raise ValueError("Should only have one argument, the name of the job!")

reload_dependencies.Reloader.start()

# Get the Python module root
module_root = reload_dependencies.Reloader.get_python_module_root()
yarn_dev_tools_module_root = os.path.join(module_root, YARNDEVTOOLS_MODULE_NAME)
cdsw_runner_path = os.path.join(yarn_dev_tools_module_root, "cdsw", "common", "cdsw_runner.py")
print("YARN dev tools module root is: %s", Reloader.YARN_DEV_TOOLS_MODULE_ROOT)


# Start the CDSW runner
job_name = sys.argv[1]
sys.argv.append("--config-dir")
sys.argv.append(jobs_dir)
exec(open(cdsw_runner_path).read())
