#! /bin/env python
#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Template for RDR tool python program.
#
import logging
import sys
import time

from tools import GCPProcessContext, GCPEnvConfigObject

from aou_cloud.services.system_utils import print_progress_bar

_logger = logging.getLogger('aou_cloud')

# Tool_cmd and tool_desc name are required.
# TODO: Change 'tool_cmd' and 'tool_desc' once you create a copy of this file.
tool_cmd = "example-progress-bar"
tool_desc = "example tool with progress bar"


class ExampleProgressBarClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        # Ensure we are pointed at the dev environment if running locally.
        self.gcp_env.override_project('aou-ehr-ops-curation-test')

        # Show a loop that also gives a visual update on progress
        total_cnt = 100
        for x in range(total_cnt):
            # TODO: Do work here
            # range starts with zero (zero-based), add one to display count.
            display_cnt = x + 1
            print_progress_bar(display_cnt, total_cnt, f'{display_cnt}/{total_cnt}', suffix='complete')
            time.sleep(0.1)

        return 0


def run():
    # Set global debug value and setup application logging.
    GCPProcessContext.setup_logging(tool_cmd)
    parser = GCPProcessContext.get_argparser(tool_cmd, tool_desc)

    # TODO:  Setup additional program arguments here.
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args) as gcp_env:
        process = ExampleProgressBarClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
