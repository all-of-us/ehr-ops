#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging

from fastapi.responses import JSONResponse
from starlette import status
from aou_cloud.services.gcp_cloud_tasks import GCPCloudTask
import glob
import os

from ._base_job import BaseCronJob


_logger = logging.getLogger('aou_cloud')


class CreateViewsJob(BaseCronJob):
    """ Simple starter template for Cron job """
    # Name is an all lower case url friendly name for the job and should be unique.
    job_name: str = 'create-views'

    def run(self):
        """
        Entry point for cron job.
        :returns: JSONResponse
        """
        # Ensure we are pointed at the dev environment if running locally.
        self.gcp_env.override_project('aou-ehr-ops-curation-test')
        dashboard = 'NIH Grant Award Metrics'
        _logger.info('Creating Cloud Tasks to create views for dashboard: {}'.format(dashboard))

        dashboard_metrics_queries = [q for q in glob.glob('../../../../dashboard_metrics/{}/.compiled/sql/*.sql'
                                                          .format(dashboard), recursive=True)]

        for query_path in dashboard_metrics_queries:
            _path, file_name = os.path.split(query_path)
            file_name = file_name.replace('.sql', '')
            query_script = open(query_path, 'r').read()
            payload = {
                'dataset': 'ehr_ops_metrics_staging',
                'query': query_script,
                'view_table': file_name
            }
            GCPCloudTask().execute('/task/create-views', queue='cron-default', payload=payload,
                                   project_id=self.gcp_env.project)

        return JSONResponse(status_code=status.HTTP_200_OK,
                            content=f'Job {self.gcp_env.project}.{self.job_name} has completed.')
