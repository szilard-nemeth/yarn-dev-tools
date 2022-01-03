#!/usr/bin/env python3
import os
import sys

import libreloader.reload_dependencies  # DO NOT REMOVE !!

print(f"Name of the script      : {sys.argv[0]=}")
print(f"Arguments of the script : {sys.argv[1:]=}")
if len(sys.argv) != 2:
    raise ValueError("Should only have one argument, the name of the job!")

job_name = sys.argv[1]
script_path = os.path.join(os.path.expanduser("~"), "cdsw", "jobs", job_name, "cdsw_runner.py")
exec(open(script_path).read())
