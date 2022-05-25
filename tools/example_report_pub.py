#! /bin/env python
#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Template for RDR tool python program.
#
import logging
import sys

from tools import GCPProcessContext, GCPEnvConfigObject


_logger = logging.getLogger('aou_cloud')

# Tool_cmd and tool_desc name are required.
tool_cmd = "example-report-pub"
tool_desc = "publish the example sheet report"


class ExampleReportPubClass(object):
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

        # These imports must be here so we import them while our current working directory is 'app_engine_std'.
        from app_engine_std.services.app_context_manager import AppEnvContextManager
        from app_engine_std.cron.app.jobs.example_report import ExampleSheetReport

        # Use the App Engine context manager here so the report object has the correct environment object.
        with AppEnvContextManager(self.gcp_env.project) as gcp_env:
            obj = ExampleSheetReport(self.args, gcp_env)
            obj.run()

        return 0


def run():
    # Set global debug value and setup application logging.
    GCPProcessContext.setup_logging(tool_cmd)
    parser = GCPProcessContext.get_argparser(tool_cmd, tool_desc)

    # TODO:  Setup additional program arguments here.
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args, working_dir='app_engine_std') as gcp_env:
        process = ExampleReportPubClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
