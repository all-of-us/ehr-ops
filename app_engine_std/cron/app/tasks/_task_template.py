#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging

from fastapi.responses import JSONResponse
from starlette import status

from services.base_app_cloud_task import BaseAppCloudTask


_logger = logging.getLogger('aou_cloud')

# TODO: Rename class and add to __all__ list in __init__.py.
class TemplateCloudTask(BaseAppCloudTask):
    """
    Simple starter template for Cloud Task. These Cloud Tasks are usually created and called by
    Cron jobs that need to split up tasks to run in parallel.
    """
    # Name is an all lower case url friendly name for the job and should be unique.
    # TODO: Change 'task_name' once you create a copy of this file.
    task_name: str = 'unknown'

    def run(self):
        """
        Entry point for cron cloud task job.
        :returns: JSONResponse
        """
        # Notes:
        #   self.payload: The request POST payload is stored in this property.
        #   self.gcp_env: The app context helper class is stored in this property.

        # Ensure we are pointed at PDR dev environment if running locally.
        self.gcp_env.override_project('aou-ehr-ops-curation-test')

        """ Example: Connect to database and run a simple query """
        # self.gcp_env.connect_database('drc', replica=True, project=project)
        # sql = "select * from pdr.mv_participant where participant_id = %s limit 1"
        # args = (123456789, )
        # results = self.gcp_env.db_fetch_all(sql, args)
        # if results:
        #     participant = results[0]
        #     _logger.info(f'Found participant id {participant.participant_id}.')

        """ Example: Use psycopg cursor to insert record """
        # sql = "insert into pdr_ops.test (id, val) values (%s, %s)"
        # args = (123, 'abc')
        # with self.gcp_env.db_conn.cursor() as cursor:
        #     cursor.execute(sql, args)
        #     self.gcp_env.db_conn.commit()

        return JSONResponse(status_code=status.HTTP_200_OK,
                            content=f'Cron task {self.gcp_env.project}.{self.task_name} has completed.')
