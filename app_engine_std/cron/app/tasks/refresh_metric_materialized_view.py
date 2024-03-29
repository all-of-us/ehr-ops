#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging
import re

from fastapi.responses import JSONResponse
from python_easy_json import JSONObject
from starlette import status

from services.base_app_cloud_task import ManagedAppCloudTask

from google.cloud import bigquery

_logger = logging.getLogger('aou_cloud')


class RefreshMVTaskPayload(JSONObject):
    """ A simple JSON payload model helper class """
    resources_dataset: str = None
    staging_dataset: str = None
    view: str = None
    snapshot: bool = False


# TODO: Rename class and add to __all__ list in __init__.py.
class RefreshMaterializedViewTask(ManagedAppCloudTask):
    """
    Simple starter template for Cloud Task. These Cloud Tasks are usually created and called by
    Cron jobs that need to split up tasks to run in parallel.
    """
    # Name is an all lower case url friendly name for the job and should be unique.
    # TODO: Change 'task_name' once you create a copy of this file.
    task_name: str = 'refresh-materialized-view'
    payload: RefreshMVTaskPayload = None

    def _run(self):
        """
        Entry point for cron cloud task job.
        :returns: JSONResponse
        """
        # Notes:
        #   self.payload: The request POST payload is stored in this property.
        #   self.gcp_env: The app context helper class is stored in this property.

        # Ensure we are pointed at PDR dev environment if running locally.
        self.gcp_env.override_project('aou-ehr-ops-curation-test')
        client = bigquery.Client(project=self.gcp_env.project)

        resources_dataset = self.payload.resources_dataset
        staging_dataset = self.payload.staging_dataset
        view = self.payload.view
        snapshot = False

        if "snapshot" in self.payload.to_dict():
            snapshot = self.payload.snapshot

        _logger.info("TASK PAYLOAD")
        _logger.info(self.payload)

        # Refresh metric from view

        _logger.info(f'Refreshing view {resources_dataset}.{view}')

        materialized_view_id = f"mv_{re.match('v_(.+)', view)[1]}"
        materialized_view_id_fq = f"{self.gcp_env.project}.{staging_dataset}.{materialized_view_id}"
        _logger.info(f"MATERIALIZED VIEW: {materialized_view_id_fq}")

        job_config = bigquery.QueryJobConfig(
            destination=materialized_view_id_fq,
            write_disposition='WRITE_TRUNCATE')

        sql = f"""
            SELECT
                *
            FROM `{self.gcp_env.project}.{resources_dataset}.{view}`
        """

        query_job = client.query(sql, job_config=job_config)
        query_job.result()

        _logger.info(f"Metric loaded to table {materialized_view_id_fq}")

        # Snapshot metric if selected
        if snapshot:

            snapshot_table_id = \
                f"{self.gcp_env.project}.{staging_dataset}.snapshot_{re.match('mv_(.+)', materialized_view_id)[1]}"

            _logger.info(f'Refreshing snapshot table {snapshot_table_id}')

            job_config = bigquery.QueryJobConfig(
                destination=snapshot_table_id,
                write_disposition='WRITE_APPEND')

            sql = f"""select *, current_timestamp() as snapshot_ts from {staging_dataset}.{materialized_view_id}"""

            query_job = client.query(sql, job_config=job_config)
            query_job.result()

            _logger.info(f"Snapshot loaded to table {snapshot_table_id}")

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=
            f'Cron task {self.gcp_env.project}.{self.task_id} has completed.')
