from google.cloud import bigquery
import os
from pathlib import Path
from google.api_core.exceptions import NotFound


#TODO:
#Put the SCOPE and PROJECT_ID in the environment variables

SCOPES = ['https://www.googleapis.com/auth/bigquery']
PROJECT_ID = 'aou-ehr-ops-curation-prod'
DATASET = 'ehr_ops_resources'

client = bigquery.Client(project=PROJECT_ID)

#TODO: Add a list of dashboards
dashboard = 'EHR Ops General Data Quality Dashboard'
current_path = Path('.').absolute()
root_path = current_path.parent
dashboard_metrics_path = root_path / 'dashboard_metrics'
sql_path = dashboard_metrics_path / dashboard / '.compiled' / 'sql'

dashboard_view_queries = list(sql_path.glob('**/v_*.sql'))
nih_dependency = ['v_ehr_flag.sql',
                  'v_ehr_rdr_participant.sql',
                  'v_ehr_gc_overall.sql',
                  'v_ehr_dc_overall.sql']

if dashboard == 'NIH Grant Award Metrics':
    for order in nih_dependency:
        for query_path in dashboard_view_queries:
            if order in str(query_path):
                view_name = order.replace('.sql', '')
                query_script = open(query_path, 'r').read()
                view_id = f'{PROJECT_ID}.{DATASET}.{view_name}'
                print(view_id, 'is updated')
                view = bigquery.Table(view_id)
                view.view_query = query_script
                job = client.update_table(view, ['view_query'])
else:
    for query_path in dashboard_view_queries:
        _path, view_name = os.path.split(query_path)
        view_name = view_name.replace('.sql', '')
        query_script = open(query_path, 'r').read()
        view_id = f'{PROJECT_ID}.{DATASET}.{view_name}'
        view = bigquery.Table(view_id)
        view.view_query = query_script
        try:
            job = client.update_table(view, ['view_query'])
            print(view_id, ' is updated')
        except NotFound:
            view = client.create_table(view)
            print(view_id, ' is created')
        else:
            pass
