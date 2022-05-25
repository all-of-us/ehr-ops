#! /bin/env python
#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Example tool to query PDR PostgreSQL database
#
import logging
import sys

from tools import GCPProcessContext, GCPEnvConfigObject

_logger = logging.getLogger('aou_cloud')

# Tool_cmd and tool_desc name are required.

tool_cmd = "example-pdr-sql"
tool_desc = "run an example postgresql sql query in pdr"


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
        Example: Make database connection. The connect_database() function will automatically detect
                 an existing 'cloud_sql_proxy' connection or start a new one.
        :return: Exit code value
        """
        # If running locally, set the active GCP project to 'aou-ehr-ops-curation-test'.
        self.gcp_env.override_project('aou-ehr-ops-curation-test')
        # Make a PDR PostgreSQL database connection.
        db_conn = self.gcp_env.db_connect_database(database='drc', user='ehr_ops')

        #
        # Postgres SQL Example 1: query using the gcp_env.db_execute() function.
        #
        sql = """
            select p.site_id, s.google_group, count(1) as total 
            from pdr.mv_participant p inner join pdr.mv_site s on p.site_id = s.site_id
            where p.hpo_id = %s 
            group by p.site_id, s.google_group 
            order by s.google_group
        """
        hpo_id = 1
        args = [hpo_id]
        # By default, db_execute() will use the first opened database connection.
        results = self.gcp_env.db_execute(sql, args)

        # Prepare to use terminal colors.
        clr = self.gcp_env.terminal_colors
        _logger.info(clr.fmt(f'\nShowing participant counts for HPO {hpo_id}', clr.custom_fg_color(156)))
        _logger.info('=' * 90)
        for row in results:
            _logger.info(clr.fmt(f'  {row.google_group:40} : {row.total}'))

        # Postgres SQL Example 2: Query using a database connection context manager object and fetchone().
        #                         Cursor object is automatically closed correctly this way.
        with db_conn.cursor() as cursor:
            sql = """select count(1) as total from pdr.mv_participant p where p.hpo_id = %s"""
            result = cursor.execute(sql, args).fetchone()
            _logger.info('-' * 90)
            _logger.info(clr.fmt(f'  {"total":40} : {result.total}'))
            _logger.info('=' * 90)

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
