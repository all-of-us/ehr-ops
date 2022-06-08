#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging

from fastapi.responses import JSONResponse
from starlette import status
from aou_cloud.services.gcp_bigquery import BigQueryJob

from ._base_task import BaseCronTask


_logger = logging.getLogger('aou_cloud')


class RefreshSnapshotTableTask(BaseCronTask):
    """
    Simple starter template for Cloud Task. These Cloud Tasks are usually created and called by
    Cron jobs that need to split up tasks to run in parallel.
    """
    # Name is an all lower case url friendly name for the job and should be unique.
    # TODO: Change 'task_name' once you create a copy of this file.
    task_name: str = 'refresh-snapshot-table'

    def run(self):
        """
        Entry point for cron cloud task job.
        :returns: JSONResponse
        """
        # Ensure we are pointed at the dev environment if running locally.
        self.gcp_env.override_project('aou-ehr-ops-curation-test')

        dataset = self.payload.dataset
        table = self.payload.table

        _logger.info(f'Refreshing snapshot table {dataset}.{table}')

        sql = f"""
            insert into {dataset}.snapshot_update_history (id, created, table) values (
                case when (select max(id)+1 from {dataset}.snapshot_update_history) is null then 1 else
                   (select max(id)+1 from {dataset}.snapshot_update_history) end,
                CURRENT_DATETIME,
                '{table}'
            )
          """

        job = BigQueryJob(sql, dataset=dataset, project=self.gcp_env.project)
        job.start_job()

        return JSONResponse(status_code=status.HTTP_200_OK,
                            content=f'Cron task {self.gcp_env.project}.{self.task_name} has completed.')
