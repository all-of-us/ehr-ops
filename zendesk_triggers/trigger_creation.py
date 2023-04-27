import os
import pandas as pd
from zenpy import Zenpy
from zenpy.lib.api_objects import Ticket, Comment, Trigger
from google.cloud import bigquery
from googleapiclient.discovery import build
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

# Zenpy credentials
CREDENTIALS = {
    "email": os.getenv('EMAIL'),
    "token": os.getenv('ZENPY_TOKEN'),
    "subdomain": os.getenv('SUBDOMAIN')
}


def get_sheets():
    # Create google sheets credentials
    key_file_location = "KEYFILE_LOC.txt"
    scopes = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets'
    ]
    creds = service_account.Credentials.from_service_account_file(
        key_file_location, scopes=scopes)

    # Authenticate and construct service.
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    # Get Submission Tracking data
    submission_tracking_result = sheet.values().get(
        spreadsheetId=os.getenv('SUBMISSION_TRACK_ID'),
        range='Mapping').execute()
    submission_tracking_values = submission_tracking_result.get('values')
    submission_tracking_df = pd.DataFrame(
        submission_tracking_values[1:], columns=submission_tracking_values[0])

    return submission_tracking_df

# CALCULATE EACH METRIC
def trigger_creation():

    zenpy_client = Zenpy(**CREDENTIALS)

    # Create bigquery client
    bigquery_client = bigquery.Client()

    # Get submission tracking data
    submission_tracking_df = get_sheets()

    # Create mapping for organization to assignee
    ticket_assignees = ['cl3777@cumc.columbia.edu', 'gage.rion@vumc.org', 'na2960@cumc.columbia.edu']
    assignee_map = {}
    # {email: {orgs:[], user_id = ''}}
    for assignee in ticket_assignees:
        org_list = list(submission_tracking_df[submission_tracking_df['contact_email'] == assignee]['organization'])
        assignee_list = list(zenpy_client.search(type='user', email=assignee))
        assignee_id = assignee_list[0].id
        user_dict = {'orgs': org_list, 'user_id': assignee_id}
        assignee_map[assignee] = user_dict


    for key in assignee_map:
        org_conditions = {'any': []}
        curr_org_list = assignee_map[key]['orgs']
        user_id = assignee_map[key]['user_id']

        for org in curr_org_list:
            org_conditions.append({"field": "Organization", "operator": "is", "value": org})

        # Create triggers for Chun Yee, Nripendra, and Gage
        trigger_audit = zenpy_client.triggers.create(
                        Trigger(actions=[{"field": "assignee_id", "value": user_id}],
                                conditions=org_conditions,
                                active=False
                            ))



    

if __name__ == '__main__':
    trigger_creation()