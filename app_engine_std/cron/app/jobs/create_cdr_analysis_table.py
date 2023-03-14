#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import glob
import logging
import traceback
import os
from pathlib import Path
from jinja2 import Template
import json
from dotenv import dotenv_values

from fastapi.responses import JSONResponse
from starlette import status
from aou_cloud.services.gcp_cloud_tasks import GCPCloudTask, Task, TaskAppEngineHttpRequest
from aou_cloud.services.gcp_bigquery import BigQueryJob

from services.base_app_cron_job import BaseAppCronJob
from google.cloud import bigquery

_logger = logging.getLogger('aou_cloud')
CDR_METRICS_PATH = Path(__file__).parent / "cdr_analysis"


class CreatCDRAnalysisTableJob(BaseAppCronJob):
    """ Execute the python script to create aggregate tables for CDR analysis """
    # Name is an all lower case url friendly name for the job and should be unique.
    # TODO: Change 'job_name' once you create a copy of this file.
    job_name: str = 'create_cdr_analysis_table'

    def get_unioned_ehr_dataset(self, project):
        """
        Query the BQ dataset to get the list of unioned ehr dataset of each CDR version
        :return: the list of unioned ehr dataset
        """
        client = bigquery.Client(project=project)
        sql = """
        SELECT
            schema_name
        FROM
            `{project_id}.INFORMATION_SCHEMA.SCHEMATA`
        WHERE
            schema_name LIKE '%q%unioned_ehr'
            """.format(project_id=project)
        results = client.query(sql)
        cdr_unioned_ehr_dataset = []
        for row in results:
            cdr_unioned_ehr_dataset.append(row.schema_name)
        return cdr_unioned_ehr_dataset

    def run(self):
        """
        Entry point for cron job.
        :returns: JSONResponse
        """
        # Notes:
        #   self.payload: The request POST payload is stored in this property.
        #   self.gcp_env: The app context helper class is stored in this property.

        # Ensure we are pointed at the dev environment if running locally.
        project = self.gcp_env.project
        client = bigquery.Client(project=project)
        app_config = self.gcp_env.get_app_config(project=project)

        dataset_parameters = app_config.dataset_parameters.to_dict()

        cdr_queries = [q for q in glob.glob(os.path.join(CDR_METRICS_PATH, '*.sql'),
                                            recursive=True)]
        cdr_dataset = self.get_unioned_ehr_dataset(project=dataset_parameters['curation_project'])
        dataset = dataset_parameters['ehr_ops_staging_dataset']

        for i in cdr_queries:
            cdr_table_name = os.path.basename(i).split('.')[0]
            client.delete_table(f'{project}.{dataset}.{cdr_table_name}', not_found_ok=True)
            _logger.info("Deleted table '{}'.".format(cdr_table_name))
            query_script = open(i, 'r').read()
            t = Template(query_script)
            for u_ehr in cdr_dataset:
                dataset_parameters['unioned_ehr_dataset'] = u_ehr
                dataset_parameters['cdr_version'] = "'" + str(u_ehr) + "'"
                templated_query = t.render(**dataset_parameters)
                payload = {'dataset': dataset,
                           'query': templated_query,
                           'table': cdr_table_name}
                GCPCloudTask().execute('/task/refresh-cdr-analysis-table', queue='cron-default', payload=payload,
                                       project_id=self.gcp_env.project)

            return JSONResponse(status_code=status.HTTP_200_OK,
                                content=f'Job {self.gcp_env.project}.{self.job_name} has completed.')
