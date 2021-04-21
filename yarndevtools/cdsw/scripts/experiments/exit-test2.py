import subprocess
import shlex
import sys


def run_cmd(cmd):
    args = shlex.split(cmd)
    proc = subprocess.run(args, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return proc


def check_proc(proc):
    print("args: " + str(proc.args))
    print("stdout: " + str(proc.stdout))
    print("stderr: " + str(proc.stderr))
    print("exit code: " + str(proc.returncode))
    if proc.returncode != 0:
        print("Exiting with exit code: " + str(proc.returncode))
        sys.exit(proc.returncode)


if __name__ == "__main__":
    try:
        proc = run_cmd("lsss -la")
        check_proc(proc)
    except Exception:
        print("Exiting with raising an arbitrary exception")
        raise Exception("random exception")

    proc = run_cmd("ls -la")
    check_proc(proc)
