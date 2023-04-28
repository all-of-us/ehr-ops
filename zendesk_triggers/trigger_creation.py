import os
import pandas as pd
from zenpy import Zenpy
from zenpy.lib.api_objects import Ticket, Comment, Trigger
from google.cloud import bigquery
from googleapiclient.discovery import build
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials
import _utils
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
    key_file_location = os.path.join("zendesk_triggers","KEYFILE_LOC.json")
    scopes = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets'
    ]
    creds = creds = service_account.Credentials.from_service_account_file(
        key_file_location, scopes=scopes)

    # Authenticate and construct service.
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()

    # Get Submission Tracking data
    submission_tracking_result = sheet.values().get(
        spreadsheetId=('1jn2UHlttx80exkpLpMRIe8MQmHqPnDkIWyrFz8twxSo'),
        range='Mapping').execute()
    submission_tracking_values = submission_tracking_result.get('values')
    submission_tracking_df = pd.DataFrame(
        submission_tracking_values[1:], columns=submission_tracking_values[0])

    return submission_tracking_df

# CALCULATE EACH METRIC
def trigger_creation():

    zenpy_client = Zenpy(**CREDENTIALS)

    # Get submission tracking data
    submission_tracking_df = get_sheets()

    # Create mapping for organization to assignee
    ticket_assignees = ['cl3777@cumc.columbia.edu', 'gage.rion@vumc.org', 'na2960@cumc.columbia.edu', 'michael.tokarz@vumc.org']
    assignee_map = {}

    for assignee in ticket_assignees:
        user_rows = submission_tracking_df.loc[submission_tracking_df['contact_email'] == assignee]
        user_name = list(submission_tracking_df.loc[submission_tracking_df['contact_email'] == assignee]['contact'])[0]
        org_list = list(user_rows['organization'])
        org_ids = []

        for org in org_list:
            search = list(zenpy_client.search(f'name:{org}', type='organization'))
            if len(search) < 1:
                print(org)

            else:
                org_ids.append(search[0].id)


        assignee_list = list(zenpy_client.search(type='user', email=assignee))
        assignee_id = assignee_list[0].id
        user_dict = {'orgs': org_ids, 'user_id': assignee_id, 'user_name': user_name}
        assignee_map[assignee] = user_dict


    for key in assignee_map:
        org_conditions = {'any': []}
        curr_org_list = assignee_map[key]['orgs']
        curr_user_id = assignee_map[key]['user_id']
        curr_user_name = assignee_map[key]['user_name']

        for org in curr_org_list:
            org_conditions['any'].append({"field": "organization_id", "operator": "is", "value": org})

        

        # Create triggers for Chun Yee, Nripendra, and Gage
        trigger_audit = zenpy_client.triggers.create(
                        Trigger(actions=[{"field": "assignee_id", "value": curr_user_id}],
                                conditions=org_conditions,
                                active=False,
                                title=f'{curr_user_name} Trigger'
                            ))
        print(f"Creating Trigger for {key}")



    

if __name__ == '__main__':
    trigger_creation()