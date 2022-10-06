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


class RefreshSnapshotTablesJob(BaseCronJob):
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

        mv_tables = [
            'mv_covid_mapping', 'mv_dc_1', 'mv_dc_2', 'mv_dc_3', 'mv_dc_4',
            'mv_duplicates', 'mv_all_eligible_participants',
            'mv_eligible_participants_ehr', 'mv_gc_1_standard',
            'mv_physical_meas',
            'mv_table_counts_with_upload_timestamp_for_hpo_sites',
            'mv_unit_route_failure', 'mv_visit_id_failure', 'mv_nih_dc_1',
            'mv_nih_dc_2', 'mv_nih_dc_3', 'mv_nih_dc_4', 'mv_nih_gc_1',
            'mv_in_person_participants'
        ]

        for table in mv_tables:
            payload = {'dataset': 'ehr_ops_metrics_staging', 'table': table}
            GCPCloudTask().execute('/task/refresh-snapshot-table',
                                   queue='cron-default',
                                   payload=payload,
                                   project_id=self.gcp_env.project)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=f'Job {self.gcp_env.project}.{self.job_name} has completed.'
        )
