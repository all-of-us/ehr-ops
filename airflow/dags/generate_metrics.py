from airflow.decorators import dag, task, task_group
from airflow.providers.google.cloud.hooks import bigquery
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.models import Variable
import pendulum
import networkx as nx
import json
from pathlib import Path
import re
# from google.cloud import bigquery as python_client_bigquery

import os

os.environ["no_proxy"] = "*"

DEPENDENCIES_FILE = Path(__file__).parent / 'dependencies.json'


def parse_dependencies():
    with open(DEPENDENCIES_FILE, 'r') as f:
        dependencies = json.load(f)

    g = nx.DiGraph()

    for task_id, details in dependencies.items():
        if 'depends_on' in details:
            for dependency in details['depends_on']:
                g.add_edge(dependency, task_id)

    return g


def order_tasks(task_dict):
    dependency_graph = parse_dependencies()
    # assert nx.is_directed_acyclic_graph(dependency_graph)

    for dependency, task_id in dependency_graph.edges:
        if task_id in task_dict and dependency in task_dict:
            task_dict[dependency] >> task_dict[task_id]


@dag(schedule_interval=None,
     start_date=pendulum.datetime(2023, 1, 1, tz="UTC"))
def generate_metrics():
    EHR_OPS_PROJECT_ID = Variable.get('ehr_ops_project_id')
    EHR_OPS_RESOURCES_DATASET_ID = Variable.get('ehr_ops_resources_dataset_id')
    EHR_OPS_METRICS_STAGING_DATASET_ID = Variable.get(
        'ehr_ops_metrics_staging_dataset_id')
    PUBSUB_SUCCESS_TOPIC = Variable.get('pubsub_success_topic')
    PUBSUB_FAILURE_TOPIC = Variable.get('pubsub_failure_topic')

    # Get list of metric views from variables
    view_list = Variable.get("bq_view_list", deserialize_json=True)

    @task_group()
    def main_block():
        task_group_dict = {}
        for view in view_list:
            view_id = view['view_name']
            group_id = f'update_metrics_{view_id}'

            build_task_id = f'build_metrics_{view_id}'
            snapshot_task_id = f'snapshot_metrics_{view_id}'

            # This was added intentionally to cause failure
            # if view_id == 'v_ehr_rdr_participant':
            #     view_id = view_id + '_bad_name_bad_name'

            @task(task_id=build_task_id)
            def build_metrics(view_id):
                materialized_view_id = f"mv_{re.match('v_(.+)', view_id)[1]}"
                print('view_id ', view_id)
                hook = bigquery.BigQueryHook(
                    gcp_conn_id='aou_ehr_ops_curation_test',
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

                destination_dataset_table = f'{EHR_OPS_METRICS_STAGING_DATASET_ID}.{materialized_view_id}'
                hook.run_query(
                    metrics_query,
                    destination_dataset_table=destination_dataset_table,
                    write_disposition='WRITE_TRUNCATE')

            @task(task_id=snapshot_task_id)
            def snapshot_metrics(view_id):
                snapshot_table_id = f"snapshot_{re.match('v_(.+)', view_id)[1]}"
                materialized_view_id = f"mv_{re.match('v_(.+)', view_id)[1]}"

                hook = bigquery.BigQueryHook(
                    gcp_conn_id='aou_ehr_ops_curation_test',
                    use_legacy_sql=False)

                snapshot_query_tmpl = """
                SELECT
                    *, current_timestamp() as snapshot_ts
                FROM `{project_id}.{dataset_id}.{materialized_view_id}`
                """

                snapshot_query = snapshot_query_tmpl.format(
                    project_id=EHR_OPS_PROJECT_ID,
                    dataset_id=EHR_OPS_METRICS_STAGING_DATASET_ID,
                    materialized_view_id=materialized_view_id)

                destination_dataset_table = f'{EHR_OPS_METRICS_STAGING_DATASET_ID}.{snapshot_table_id}'
                hook.run_query(
                    snapshot_query,
                    destination_dataset_table=destination_dataset_table,
                    write_disposition='WRITE_APPEND')

            @task_group(group_id=group_id)
            def update_metrics(view_id):
                t1 = build_metrics(view_id)
                t2 = snapshot_metrics(view_id)

                t1 >> t2

            task_group_dict[group_id] = update_metrics(view_id)

        order_tasks(task_group_dict)

    #Uncomment for pubsub

    pubsub_publish_success = PubSubPublishMessageOperator(
        task_id="publish_success",
        project_id=EHR_OPS_PROJECT_ID,
        topic=PUBSUB_SUCCESS_TOPIC,
        messages=[{
            "data": b"success"
        }],
        trigger_rule="all_success")

    pubsub_publish_failure = PubSubPublishMessageOperator(
        task_id="publish_failure",
        project_id=EHR_OPS_PROJECT_ID,
        topic=PUBSUB_FAILURE_TOPIC,
        messages=[{
            "data": b"failed"
        }],
        trigger_rule="one_failed")

    main_block() >> [pubsub_publish_success, pubsub_publish_failure]

    # main_block()


generate_metrics = generate_metrics()

if __name__ == '__main__':
    dag.test()