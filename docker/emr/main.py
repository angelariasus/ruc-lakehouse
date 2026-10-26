"""Entry point of the job image on EMR Serverless.

EMR Serverless runs a script, not a module, so every job run starts here and
names the module to run, with the same arguments as locally:

    entryPoint:          local:///opt/ruc-lakehouse/main.py
    entryPointArguments: ["ruc_lakehouse.jobs.bronze", "2026-10-25"]
"""

import runpy
import sys

if __name__ == "__main__":
    module, *args = sys.argv[1:]
    sys.argv = [module, *args]
    runpy.run_module(module, run_name="__main__", alter_sys=True)
