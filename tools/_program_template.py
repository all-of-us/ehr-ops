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
# TODO: Change 'tool_cmd' and 'tool_desc' once you create a copy of this file.
tool_cmd = "template"
tool_desc = "put tool help description here"


class ProgramTemplateClass(object):
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

        """
        Example: Make database connection. The connect_database() function will automatically detect
                 an existing 'cloud_sql_proxy' connection or start a new one.
        """
        # db_conn = self.gcp_env.connect_database(database='drc')


        """
        Example: Get the Configurator account used in a GCP project.

        Calling `get_gcp_configurator_account` will return the configurator service account.
        """
        # account = self.gcp_env.get_gcp_configurator_account(self.gcp_env.project)

        """
        Note: The difference between args.project and gcp_env.project.

            The difference is args.project is the project ID passed to the program, the gcp_env.project
            value is the project ID the GCP Context Manager has configured and is using for all cloud
            operations.  Depending on circumstances they may not always be the same.  Its best to
            always use gcp_env.project in your code unless you know you want to specifically use
            args.project.
        """

        """
        Example: Enabling colors in terminals.
            Using colors in terminal output is supported by using the self.gcp_env.terminal_colors
            object.  Errors and Warnings are automatically set to Red and Yellow respectively.
            The terminal_colors object has many predefined colors, but custom colors may be used
            as well. See rdr_service/services/TerminalColors for more information.
        """
        # clr = self.gcp_env.terminal_colors
        # _logger.info(clr.fmt('This is a blue info line.', clr.fg_bright_blue))
        # _logger.info(clr.fmt('This is a custom color line', clr.custom_fg_color(156)))


        # TODO: write program main process here after setting 'tool_cmd' and 'tool_desc'...
        return 0


def run():
    # Set global debug value and setup application logging.
    GCPProcessContext.setup_logging(tool_cmd)
    parser = GCPProcessContext.get_argparser(tool_cmd, tool_desc)

    # TODO:  Setup additional program arguments here.
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args) as gcp_env:
        process = ProgramTemplateClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
