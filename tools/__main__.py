#! /usr/bin/env python
#
# Tool launcher
#
import sys

from aou_cloud.tools import run_tool



def run():
    """
    Developer Tools
    """
    lib_paths = ["tools", "../tools", "tools", "../../tools"]
    import_path = "tools"
    return run_tool(lib_paths, import_path)


# --- Main Program Call ---
sys.exit(run())
