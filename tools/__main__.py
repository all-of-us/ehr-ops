#! /usr/bin/env python
#
# Tool launcher
#
import os
import sys

from aou_cloud.tools import run_tool
from aou_cloud.services.system_utils import git_project_root

# Put all content root directories here.
CONTENT_ROOT_PATHS = [
    'app_engine_std'
]


def run():
    """
    Developer Tools
    """
    # Add all project content root directories to Python search path.
    project_path = git_project_root()
    for cr in CONTENT_ROOT_PATHS:
        path = os.path.join(project_path, cr)
        sys.path.append(path)

    lib_paths = ["tools", "../tools", "tools", "../../tools"]
    import_path = "tools"
    return run_tool(lib_paths, import_path)


# --- Main Program Call ---
sys.exit(run())
