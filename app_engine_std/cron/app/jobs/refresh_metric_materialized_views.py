#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging

from fastapi.responses import JSONResponse
from starlette import status
from aou_cloud.services.gcp_cloud_tasks import GCPCloudTask

from ._base_job import BaseCronJob
from aou_cloud.services.gcp_bigquery import BigQueryJob

_logger = logging.getLogger('aou_cloud')


# TODO: Rename class and add to __all__ list in __init__.py.
class RefreshMaterializedViewsJob(BaseCronJob):
    """ Simple starter template for Cron job """
    # Name is an all lower case url friendly name for the job and should be unique.
    # TODO: Change 'job_name' once you create a copy of this file.
    job_name: str = 'refresh_materialized_views'

    def get_views(self, project, dataset):
        sql = """
            SELECT
                table_catalog, table_schema, table_name
            FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
            WHERE table_type='VIEW'
        """.format(project_id=project, dataset_id=dataset)

        job = BigQueryJob(sql, project=project, dataset=dataset)

        views = []
        for batch in job:
            for row in batch:
                views.append(row)

        return views

    def run(self):
        """
        Entry point for cron job.
        :returns: JSONResponse
        """
        # Notes:
        #   self.payload: The request POST payload is stored in this property.
        #   self.gcp_env: The app context helper class is stored in this property.

        # Ensure we are pointed at the dev environment if running locally.
        self.gcp_env.override_project('aou-ehr-ops-curation-test')

        _logger.info('Created Cloud Tasks to refresh materialized views')

        dataset = 'ehr_ops_metrics_staging'

        views = self.get_views(self.gcp_env.project, dataset)

        for view in views:
            view_name = view['table_name']
            payload = {
                'dataset': 'aou-ehr-ops-curation-test',
                'view': view_name
            }

            GCPCloudTask().execute('/task/refresh-materialized-view',
                                   queue='cron-default',
                                   payload=payload,
                                   project_id=self.gcp_env.project)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=f'Job {self.gcp_env.project}.{self.job_name} has completed.'
        )
