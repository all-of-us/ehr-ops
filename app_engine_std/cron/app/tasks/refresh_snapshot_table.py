#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging

from fastapi.responses import JSONResponse
from python_easy_json import JSONObject
from starlette import status

from services.base_app_cloud_task import BaseAppCloudTask

from google.cloud import bigquery
import re

_logger = logging.getLogger('aou_cloud')


class RefreshSnapshotTaskPayload(JSONObject):
    """ A simple JSON payload model helper class """
    dataset: str = None
    table: str = None


class RefreshSnapshotTableTask(BaseAppCloudTask):
    """
    Simple starter template for Cloud Task. These Cloud Tasks are usually created and called by
    Cron jobs that need to split up tasks to run in parallel.
    """
    # Name is an all lower case url friendly name for the job and should be unique.
    # TODO: Change 'task_name' once you create a copy of this file.
    task_name: str = 'refresh-snapshot-table'
    payload: RefreshSnapshotTaskPayload = None

    def run(self):
        """
        Entry point for cron cloud task job.
        :returns: JSONResponse
        """
        # Ensure we are pointed at the dev environment if running locally.
        self.gcp_env.override_project('aou-ehr-ops-curation-test')
        client = bigquery.Client(project=self.gcp_env.project)

        staging_dataset = self.payload.dataset
        table = self.payload.table

        _logger.info(f'Refreshing snapshot table {staging_dataset}.{table}')

        snapshot_table_id = f"{self.gcp_env.project}.{staging_dataset}.snapshot_{re.match('mv_(.+)', table)[1]}"

        job_config = bigquery.QueryJobConfig(
            destination=snapshot_table_id,
            write_disposition='WRITE_APPEND')

        sql = f"""select *, current_timestamp() as snapshot_ts from {staging_dataset}.{table}"""

        query_job = client.query(sql, job_config=job_config)
        query_job.result()

        _logger.info(f"Results loaded to table {snapshot_table_id}")

        return JSONResponse(status_code=status.HTTP_200_OK,
                            content=f'Cron task {self.gcp_env.project}.{self.task_name} has completed.')
