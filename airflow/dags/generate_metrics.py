from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks import bigquery
from airflow.models import Variable
import pendulum
import networkx as nx
import json
from pathlib import Path
import re
from google.cloud import bigquery as python_client_bigquery

DEPENDENCIES_FILE = Path(__file__) / 'dependencies.json'


def parse_dependencies():
    with open(DEPENDENCIES_FILE, 'r') as f:
        dependencies = json.load(f)

    g = nx.Graph()

    for task_id, details in dependencies.items():
        if 'depends_on' in details:
            for dependency in details['depends_on']:
                g.add_edge(task_id, dependency)

    return g


def order_tasks(task_dict):
    dependency_graph = parse_dependencies()
    assert nx.is_directed_acyclic_graph(dependency_graph)

    for task_id, prev_task_id in dependency_graph.edges:
        if task_id in task_dict and prev_task_id in task_dict:
            task_dict[prev_task_id] >> task_dict[task_id]


@dag(schedule_interval=None,
     start_date=pendulum.datetime(2023, 1, 1, tz="UTC"))
def generate_metrics():
    CURATION_PROJECT_ID = 'aou-res-curation-prod'
    EHR_OPS_PROJECT_ID = 'aou-ehr-ops-curation-test'
    EHR_OPS_RESOURCES_DATASET_ID = 'ehr_ops_resources'
    EHR_OPS_METRICS_STAGING_DATASET_ID = 'ehr_ops_metrics_staging'
    EHR_OPS_DATASET_ID = 'ehr_ops'

    # Pull list of metric views
    @task(task_id=f'retrieve_metric_view_list')
    def retrieve_metric_view_list():
        hook = bigquery.BigQueryHook(gcp_conn_id='aou_ehr_ops_curation_test',
                                     use_legacy_sql=False)

        client = python_client_bigquery.Client(
            project=hook._get_field('project'),
            credentials=hook._get_credentials())

        sql = """
            SELECT
                table_catalog, table_schema, table_name
            FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
            WHERE table_type='VIEW'
        """.format(project_id=EHR_OPS_PROJECT_ID,
                   dataset_id=EHR_OPS_RESOURCES_DATASET_ID)

        results = client.query(sql)
        view_names = [row["table_name"] for row in results]

        return view_names

    # Decide dependency order of views to execute

    # Execute each view in `ehr_ops_resources` and save results as tables

    @task
    def build_metrics(view_id):
        materialized_view_id = f"mv_{re.match('v_(.+)', view_id)[1]}"

        hook = bigquery.BigQueryHook(gcp_conn_id='aou_ehr_ops_curation_test',
                                     use_legacy_sql=False)

        metrics_query_tmpl = """
        SELECT
            *
        FROM `{project_id}.{dataset_id}.{view_id}`
        """

        metrics_query = metrics_query_tmpl.format(
            project_id=EHR_OPS_PROJECT_ID,
            dataset_id=EHR_OPS_RESOURCES_DATASET_ID,
            view_id=view_id)

        destination_dataset_table = f'{EHR_OPS_PROJECT_ID}.{EHR_OPS_METRICS_STAGING_DATASET_ID}.{materialized_view_id}'
        hook.run_query(metrics_query,
                       destination_dataset_table=destination_dataset_table,
                       write_disposition='WRITE_TRUNCATE')

    build_metrics.expand(view_id=retrieve_metric_view_list())

    # Message success or failure to `metric-load-completed` and `metric-load-failed` PubSub topics

    # Snapshot tables


generate_metrics = generate_metrics()