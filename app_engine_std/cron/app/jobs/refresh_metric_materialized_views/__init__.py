#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging
import json
from pathlib import Path

from fastapi.responses import JSONResponse
from starlette import status
from aou_cloud.services.gcp_cloud_tasks import GCPCloudTask

from services.base_app_cron_job import ManagedAppCronJob
from aou_cloud.services.gcp_bigquery import BigQueryJob

import networkx as nx

_logger = logging.getLogger('aou_cloud')

DEPENDENCY_FILE = Path(__file__).parent / 'dependencies.json'
PUB_SUB_COMPLETED_TOPIC = "metric-load-completed"
PUB_SUB_FAILED_TOPIC = "metric-load-failed"


# TODO: Rename class and add to __all__ list in __init__.py.
class RefreshMaterializedViewsJob(ManagedAppCronJob):
    """ Simple starter template for Cron job """
    # Name is an all lower case url friendly name for the job and should be unique.
    # TODO: Change 'job_name' once you create a copy of this file.
    job_name: str = 'refresh_materialized_views'
    task_list: dict = {}
    publish_response: bool = True
    pub_sub_success_topic: str = PUB_SUB_COMPLETED_TOPIC
    pub_sub_failed_topic: str = PUB_SUB_FAILED_TOPIC

    def __init__(
        self,
        gcp_env,
        payload=None,
    ):
        super().__init__(gcp_env, payload=payload)
        self._initialize_task_list()

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
                views.append(row.table_name)

        return views

    def _initialize_task_list(self):
        self.gcp_env.override_project('aou-ehr-ops-curation-test')

        resources_dataset = 'ehr_ops_resources'
        staging_dataset = 'ehr_ops_metrics_staging'

        views = self.get_views(self.gcp_env.project, resources_dataset)
        dependencies = json.load(open(DEPENDENCY_FILE, 'r'))
        task_list = {}
        for view in views:
            task_id = f'materialize_{view}'
            task_uri = '/task/refresh-materialized-view'

            payload = {
                'resources_dataset':
                resources_dataset,
                'staging_dataset':
                staging_dataset,
                'view':
                view,
                'depends_on':
                [f'materialize_{d}' for d in dependencies[view]['depends_on']]
                if view in dependencies and 'depends_on' in dependencies[view]
                else [],
                'snapshot':
                dependencies[view]['snapshot'] if view in dependencies
                and 'snapshot' in dependencies[view] else False
            }

            task_args = (task_uri)
            task_kwargs = {
                'uri': task_uri,
                'queue': 'cron-default',
                'payload': payload,
                'project_id': self.gcp_env.project
            }

            task_list[task_id] = {
                'task_args': task_args,
                'task_kwargs': task_kwargs
            }

        self.task_list = task_list