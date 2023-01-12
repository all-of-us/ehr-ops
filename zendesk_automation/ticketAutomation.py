import os
from unittest import case
import utils
import pandas as pd
from zenpy import Zenpy
from zenpy.lib.api_objects import Ticket, User, Comment, BaseObject
from google.cloud import bigquery
from googleapiclient.discovery import build
from google.oauth2 import service_account
import json

# Zenpy credentials
credentials = {
    "email": utils.EMAIL,
    "token": utils.ZENPY_TOKEN,
    "subdomain": utils.SUBDOMAIN
}

# Google scopes
DEFAULT_SCOPES = [
    'https://www.googleapis.com/auth/devstorage.read_only',
    'https://www.googleapis.com/auth/bigquery.read_only',
    'https://www.googleapis.com/auth/drive'
]

def tag_intersection(zenpy_client, status_list, tag_list):
    # create set of ticket ids that will be intersected on each search
    ticket_ids = {}
    for index, tag in enumerate(tag_list):
        search = zenpy_client.search(status=status_list, type="ticket", tags=tag)

        if index == 0:
            ticket_ids = set(search)
        
        else:
            ticket_ids.intersection(search)

    return ticket_ids

comment_body = '''
This ticket has not been resolved. Please make the necessary changes or reach out to our team if you have questions or concerns.

Thanks, 
EHR Ops Team
'''     

def get_ticket_body(table_name, metric, metric_value, hpo_name):
    # gc1, pysical measurement, covid mapping, measurement integration
    # only gc1 is by table
    ticket_body = ""
    if metric == 'GC1':
        ticket_body = f'''
        Hi {hpo_name}, 
        
        In your latest submission, your GC-1 rate was {metric_value}{table_name}, which is below our acceptance threshold. GC-1 measures conformance to OMOP standard concepts and is a priority for data quality. 
        
        There is additional information linked here, along with SQL queries to help you identify the issue: https://aou-ehr-ops.zendesk.com/hc/en-us/articles/1500012365822-NIH-Grant-Award-Metrics-

        You can access our EHR Ops dashboard here: https://drc.aouanalytics.org/#/views/EHROpsGeneralDataQualityDashboard/Home

        Please fix this data quality issue and resubmit. 

        Thanks, 
        EHR Ops Team 
        '''
    
    elif metric == 'physical_measurement':
        ticket_body = f'''
        Hi {hpo_name}, 
        
        In your latest submission, the physical measurements you submitted were either marked as missing or fell below our threshold. 
        This is a high priority data quality metric: particularly height, weight, blood pressure, and heart rate. 
        
        There is additional information linked here, along with SQL queries to help you identify the issue: https://aou-ehr-ops.zendesk.com/hc/en-us/articles/1500012365842-Physical-measurements
        
        You can access our EHR Ops dashboard here: https://drc.aouanalytics.org/#/views/EHROpsGeneralDataQualityDashboard/Home
        
        Please fix this data quality issue and resubmit. 
        
        Thanks,
        EHR Ops Team 

        '''

    elif metric == 'covid_mapping':
        ticket_body = f'''
        Hi {hpo_name}, 
        
        Thanks, 
        EHR Ops Team 
        
        '''

    # elif metric == 'measurement_integration':
    #     ticket_body = ''''''

    return ticket_body

def zenpy_obj_to_json(filename, request):
    with open(f"{filename}.json", "w") as outfile:
        outfile.write('[')
        for i, each in enumerate(request):
            outfile.write(each.to_json())
            if i < len(request)-1:
                outfile.write(',')
        outfile.write(']')

# Create google sheets credentials
key_file_location = utils.KEYFILE_LOC
scopes=['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
creds = service_account.Credentials.from_service_account_file(key_file_location, scopes=scopes)

# Authenticate and construct service.
service = build('sheets', 'v4', credentials = creds)

# Get Site Contact data
# sheet = service.spreadsheets()
# site_contact_result = sheet.values().get(spreadsheetId='1aS9etPRp1pHpms-uL1mnwoqK7B6uuWfXgU3MabbCyFE',
#                             range='Site Contacts').execute()
# site_contact_values = site_contact_result.get('values')
# site_contact_df = pd.DataFrame(site_contact_values[1:], columns=site_contact_values[0])

# Get TEST Site Contact data
sheet = service.spreadsheets()
site_contact_result = sheet.values().get(spreadsheetId=utils.SITE_CONTACT_ID,
                            range='Site Contacts').execute()
site_contact_values = site_contact_result.get('values')
site_contact_df = pd.DataFrame(site_contact_values[1:], columns=site_contact_values[0])

# Get Submission Tracking data
submission_tracking_result = sheet.values().get(spreadsheetId=utils.SUBMISSION_TRACK_ID,
                            range='Mapping').execute()
submission_tracking_values = submission_tracking_result.get('values')
submission_tracking_df = pd.DataFrame(submission_tracking_values[1:], columns=submission_tracking_values[0])



# Set Google Application Credentials to key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= utils.GOOGLE_APP_CREDS

zenpy_client = Zenpy(**credentials)

request = zenpy_client.tickets(tags=["auto-test-tickets"])
zenpy_obj_to_json("mock", request)

# Create bigquery client
bigquery_client = bigquery.Client()

# get list of hpo_ids
hpo_list = site_contact_df.loc[site_contact_df['hpo_id'] != '']['hpo_id'].to_list()

# get symmetric difference between hpo_list and list to exclude
exclude = ['vcu', 'wash_u_stl']
included_hpos = set(hpo_list) ^ set(exclude)

metrics = ['gc1']

# CALCULATE EACH METRIC
def ticket_automation():
    for metric in metrics:

        # GC1 query/calculation for included hpos
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("included_hpos", "STRING", included_hpos)
            ]
        )
        f = open(f'{metric}.sql')
        QUERY = f.read()
        # API request
        query_job = bigquery_client.query(QUERY, job_config=job_config)
        scores_df = query_job.result().to_dataframe()
        filled_scores_df = scores_df.fillna(0)

        # If metrics are returned
        if filled_scores_df.shape[0] > 0:
            # TODO: should I just remove the metric name from the column names so the variables can be generic for each metric?
            # Find all hpos with scores below the threshold
            low_scores_df = filled_scores_df[(filled_scores_df[f'condition_{metric}'] < 0.9) | (filled_scores_df[f'drug_{metric}'] < 0.9) | (filled_scores_df[f'measurement_{metric}'] < 0.9) | (filled_scores_df[f'observation_{metric}'] < 0.9) | (filled_scores_df[f'procedure_{metric}'] < 0.9) | (filled_scores_df[f'visit_{metric}'] < 0.9) | (filled_scores_df[f'_{metric}'] < 0.9)]
            
            # For each hpo with a low score evaluate which table has the low metric
            for index, row in low_scores_df.iterrows():
                src_hpo_id = row['src_hpo_id']

                # Dictionary of current hpo's tables and respective calculation
                scores = {f'_{metric}': row[f'_{metric}'], 'condition': row[f'condition_{metric}'], 'drug': row[f'drug_{metric}'], 'measurement': row[f'measurement_{metric}'], 'observation': row[f'observation_{metric}'], 'procedure': row[f'procedure_{metric}'], 'visit': row[f'visit_{metric}']}
                
                for name, score in scores.items():
                    
                    # If the score is low (whole row is returned for the hpo_id) begin search & comment or create ticket workflow
                    if score < 0.9:
                        # search for pending and open tickets with GC-1 and hpo_id
                        table_name = name.split(f'_{metric}')[0]
                        if len(table_name) < 1 or metric != 'gc1':
                            tag_list = [src_hpo_id, metric, "auto-test-tickets"]
                        else:
                            tag_list = [src_hpo_id, table_name, metric, "auto-test-tickets"]
                        
                        ticket_status = ['open', 'pending']
                        search = tag_intersection(ticket_status, tag_list)

                        # TODO: Decide what to do if multiple tickets are returned by the search
                        # If the search does return a ticket then add a comment to the existing ticket
                        if len(search) > 0:
                            for ticket_obj in search:
                                ticket_id = ticket_obj.id
                                ticket = zenpy_client.tickets(id=ticket_id)
                                print(f'Commenting on Zendesk ticket with ID {ticket_id}...')
                                # ticket.comment = Comment(body=comment_body, public=False)
                                # zenpy_client.tickets.update(ticket)

                        # If the search does not return a ticket then create a new ticket
                        else:
                            hpo_row=submission_tracking_df.loc[submission_tracking_df['hpo_id'] == src_hpo_id]
                            assignee_email = hpo_row['contact_email']
                            assignee = zenpy_client.search(type='user', email=assignee_email)
                            assignee_id = list(assignee)[0].id
                            requester = zenpy_client.search(type='user', email='noreply@researchallofus.org')
                            requester_id = list(requester)[0].id
                            hpo_name = list(site_contact_df[site_contact_df['hpo_id'] == src_hpo_id]['Site Name'])[0]
                            if table_name == '':
                                metric_value = scores[f'_{metric}']
                                table_phrase = table_name
                            else:
                                metric_value = scores[table_name]
                                table_name = table_name.title() + " Table"
                                table_phrase = " for the " + table_name + " table"
                            metric_upper = metric.upper()
                            ticket_descr = get_ticket_body(table_phrase, metric_upper, round(metric_value, 2), hpo_name)
                            print('Creating Zendesk ticket...')
                            # zenpy_client.tickets.create(Ticket(requester_id=requester_id, assignee_id=assignee_id, 
                            #                                     subject=f"{metric_upper} {table_name} Data Quality Issue Flagged", 
                            #                                     description=ticket_descr, tags=tag_list))
