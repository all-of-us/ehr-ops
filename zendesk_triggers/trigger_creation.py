import os
import pandas as pd
from zenpy import Zenpy
from zenpy.lib.api_objects import Ticket, Comment
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
def ticket_automation():

    zenpy_client = Zenpy(**CREDENTIALS)

    # Create bigquery client
    bigquery_client = bigquery.Client()

    # Get submission tracking data
    submission_tracking_df = get_sheets()

    # Create mapping for organization to assignee

    # Map assignee to their zenpy user id

    # Create triggers for Chun Yee, Nripendra, and Gage



    

if __name__ == '__main__':
    ticket_automation()