#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging

from fastapi.responses import JSONResponse
from starlette import status
from google.cloud import bigquery

from services.base_app_cloud_task import BaseAppCloudTask

_logger = logging.getLogger('aou_cloud')


class RefreshCDRAnalysisTableTask(BaseAppCloudTask):
    """
    Simple starter template for Cloud Task. These Cloud Tasks are usually created and called by
    Cron jobs that need to split up tasks to run in parallel.
    """
    # Name is an all lower case url friendly name for the job and should be unique.
    # TODO: Change 'task_name' once you create a copy of this file.
    task_name: str = 'refresh-cdr-analysis-table'

    def run(self):
        """
        Entry point for cron cloud task job.
        :returns: JSONResponse
        """
        # Ensure we are pointed at the dev environment if running locally.
        # UNCOMMENT self.gcp_env.override_project('INSERT_PROJECT_ID_HERE')
        project = self.gcp_env.project
        client = bigquery.Client(project=project)

        dataset = self.payload.dataset
        table = self.payload.table
        query = self.payload.query

        _logger.info(f'Creating of Updating table {dataset}.{table}')
        check_table_exists_query = f"""
            SELECT
                COUNT(1) AS table_exists
            FROM
               `{dataset}.__TABLES_SUMMARY__`
            WHERE
                table_id='{table}'
        """
        table_count = client.query(check_table_exists_query)
        table_count = table_count.result()
        for row in table_count:
            table_exist = row.table_exists
            print(table_exist)
        if table_exist == 0:
            create_table_job_config = bigquery.QueryJobConfig(destination=f'{project}.{dataset}.{table}')
            create_table = client.query(query, job_config=create_table_job_config)
            create_table.result()
            _logger.info(f'Created table {dataset}.{table}')
        else:
            try:
                append_table_job_config = bigquery.QueryJobConfig(destination=f'{project}.{dataset}.{table}',
                                                                  write_disposition='WRITE_APPEND')
                append_table = client.query(query, job_config=append_table_job_config)
                append_table.result()
                _logger.info(f'Updated table {dataset}.{table}')
            except Exception as e:
                _logger.info(f'Error updating table {dataset}.{table}')
                raise e

        return JSONResponse(status_code=status.HTTP_200_OK,
                            content=f'Cron task {self.gcp_env.project}.{self.task_name} has completed.')
