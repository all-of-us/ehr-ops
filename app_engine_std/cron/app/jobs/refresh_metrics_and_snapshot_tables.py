#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging
import traceback

from fastapi.responses import JSONResponse
from fastapi import HTTPException
from starlette import status
from aou_cloud.services.gcp_google_pubsub import GCPGooglePubSubTopic

from ._base_job import BaseCronJob
from .refresh_metric_materialized_views import RefreshMaterializedViewsJob
from .refresh_snapshot_tables import RefreshSnapshotTablesJob

_logger = logging.getLogger('aou_cloud')

PUB_SUB_COMPLETED_TOPIC = "metric-load-completed"
PUB_SUB_FAILED_TOPIC = "metric-load-failed"


# TODO: Rename class and add to __all__ list in __init__.py.
class RefreshMetricsAndSnapshotTables(BaseCronJob):
    """ Execute the refresh_metric_materialized_views and refresh_snapshot_tables jobs sequentially """
    # Name is an all lower case url friendly name for the job and should be unique.
    # TODO: Change 'job_name' once you create a copy of this file.
    job_name: str = 'refresh_metrics_and_snapshot_tables'

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

        try:
            refresh_metrics_job = RefreshMaterializedViewsJob(
                self.gcp_env, None)
            refresh_snapshot_tables_job = RefreshSnapshotTablesJob(
                self.gcp_env, None)

            # Execute refresh metrics job
            resp = refresh_metrics_job.run()

            if not resp.status_code == status.HTTP_200_OK:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=resp)

            # Execute refresh snapshots job
            resp = refresh_snapshot_tables_job.run()
            if not resp.status_code == status.HTTP_200_OK:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=resp)

            # Send pubsub message

            topic = GCPGooglePubSubTopic(self.gcp_env.project,
                                         PUB_SUB_COMPLETED_TOPIC)
            topic.publish("completed")

            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content=
                f'Job {self.gcp_env.project}.{self.job_name} has completed.')
        except Exception:
            topic = GCPGooglePubSubTopic(self.gcp_env.project,
                                         PUB_SUB_FAILED_TOPIC)
            topic.publish("failed")

            _logger.error(traceback.format_exc())

            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content=
                f'Job {self.gcp_env.project}.{self.job_name} has failed.')
