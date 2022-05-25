#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging

from fastapi.responses import JSONResponse
from starlette import status
from aou_cloud.services.gcp_cloud_tasks import GCPCloudTask

from ._base_job import BaseCronJob


_logger = logging.getLogger('aou_cloud')


class RefreshSnaphotTablesJob(BaseCronJob):
    """ Simple starter template for Cron job """
    # Name is an all lower case url friendly name for the job and should be unique.
    job_name: str = 'refresh-snapshot-tables'

    def run(self):
        """
        Entry point for cron job.
        :returns: JSONResponse
        """
        # Ensure we are pointed at the dev environment if running locally.
        self.gcp_env.override_project('aou-ehr-ops-curation-test')

        _logger.info('Creating Cloud Tasks to refresh snapshot tables')

        snapshot_tables = [
            'dc_1',
            'dc_2',
            'dc_3',
            'dc_4'
        ]

        for table in snapshot_tables:
            payload = {
                'dataset': 'dev_testing',
                'table': table
            }
            GCPCloudTask().execute('/task/refresh-snapshot-table', queue='cron-default', payload=payload,
                                   project_id=self.gcp_env.project)

        return JSONResponse(status_code=status.HTTP_200_OK,
                            content=f'Job {self.gcp_env.project}.{self.job_name} has completed.')
