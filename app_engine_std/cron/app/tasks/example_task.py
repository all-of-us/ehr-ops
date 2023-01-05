#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging
import psycopg

from fastapi.responses import JSONResponse
from starlette import status

from services.base_app_cloud_task import BaseAppCloudTask


_logger = logging.getLogger('aou_cloud')


class ExampleCloudTask(BaseAppCloudTask):
    """
    Simple starter template for Cloud Task. These Cloud Tasks are usually created and called by
    Cron jobs that need to split up tasks to run in parallel.
    """
    # Name is an all lower case url friendly name for the job and should be unique.
    task_name: str = 'example-task'

    def run(self):
        """
        Entry point for cron cloud task job.
        :returns: JSONResponse
        """
        # Notes:
        #   self.payload: The request POST payload is stored in this property.
        #   self.gcp_env: The app context helper class is stored in this property.

        # Ensure we are pointed at dev environment if running locally.
        self.gcp_env.override_project('aou-ehr-ops-curation-test')

        _logger.info(f'Example Task: Payload : {self.payload}.')

        """ Example: Connect to database and run a simple query """
        db_conn:psycopg.Connection = self.gcp_env.db_connect_database('drc', replica=True, project=self.gcp_env.project)
        with db_conn.cursor() as cursor:
            sql = "select count(1) as total from pdr.mv_participant_all"
            result = cursor.execute(sql).fetchone()

            _logger.info(f'Example Task: Total PDR participants {result.total}.')

        return JSONResponse(status_code=status.HTTP_200_OK,
                            content=f'Cron task {self.gcp_env.project}.{self.task_name} has completed.')
