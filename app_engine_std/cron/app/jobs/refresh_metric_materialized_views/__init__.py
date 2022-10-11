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

from .._base_job import BaseCronJob
from aou_cloud.services.gcp_bigquery import BigQueryJob

import networkx as nx

_logger = logging.getLogger('aou_cloud')

DEPENDENCY_FILE = Path(__file__).parent / 'dependencies.json'


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
                views.append(row.table_name)

        return views

    def _order_by_dependencies(self, views, dependency_dict):
        """Order a list of view names by a dictionary of dependencies

        Args:
            views (list[str]): List of view names
            dependency_dict (dict): Dictionary with 'depends_on' relationships
        """

        g = nx.DiGraph()
        for view_name in views:
            g.add_node(view_name)

        for view_name, attr in dependency_dict.items():
            if view_name in views:
                dependencies = attr['depends_on']
                for dependency in dependencies:
                    if dependency in views:
                        g.add_edge(view_name, dependency)

        sorted_views = list(reversed(list(nx.topological_sort(g))))

        return sorted_views

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

        resources_dataset = 'ehr_ops_resources'
        staging_dataset = 'ehr_ops_metrics_staging'

        views = self.get_views(self.gcp_env.project, resources_dataset)

        dependency_dict = json.load(open(DEPENDENCY_FILE, 'r'))
        views = self._order_by_dependencies(views, dependency_dict)

        for view in views:
            view_name = view
            payload = {
                'resources_dataset': resources_dataset,
                'staging_dataset': staging_dataset,
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
