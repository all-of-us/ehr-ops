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

# Google scopes
DEFAULT_SCOPES = [
    'https://www.googleapis.com/auth/devstorage.read_only',
    'https://www.googleapis.com/auth/bigquery.read_only',
    'https://www.googleapis.com/auth/drive'
]


def tag_intersection(zenpy_client, status_list, tag_list):
    # create set of ticket ids that will be intersected on each search
    sets = []
    for index, tag in enumerate(tag_list):

        ticket_ids = set()
        search = list(zenpy_client.search(status=status_list,
                                     type="ticket",
                                     tags=tag))

        for ticket in search:
            ticket_ids.add(ticket.id)

        sets.append(ticket_ids)

    ticket_intersection = set.intersection(*sets)
    return list(ticket_intersection)


def get_ticket_body(action, hpo_name, metric, metric_value=None, table_name=None, ids=None):
    # gc1, pysical measurement, covid mapping, measurement integration
    # only gc1 is by table
    ticket_body = ""
    comment_body = ""
    if metric == 'GC1' and action == 'ticket':
        ticket_body = f'''Hi {hpo_name}, 
        
In your latest submission, your GC-1 rate was {metric_value}{table_name}, which is below our acceptance threshold. GC-1 measures conformance to OMOP standard concepts and is a priority for data quality. 
        
There is additional information linked here, along with SQL queries to help you identify the issue: https://aou-ehr-ops.zendesk.com/hc/en-us/articles/1500012365822-NIH-Grant-Award-Metrics-#h_01F7AB48M32ZRGYAVQXWAX92W6

You can access our EHR Ops dashboard here: https://drc.aouanalytics.org/#/views/EHROpsGeneralDataQualityDashboard/Home

Please fix this data quality issue and resubmit. 

Thanks, 
EHR Ops Team
        '''

        return ticket_body
    
    elif metric == 'GC1' and action == 'comment':
        comment_body = f'''Hi {hpo_name},
        
In your latest submission, your GC-1 rate was {metric_value}{table_name}, which is still below our acceptance threshold. This ticket has not been resolved. Please make the necessary changes or reach out to our team if you have questions or concerns.

Thanks, 
EHR Ops Team
        '''

        return comment_body

    elif metric == 'PHYSICAL MEASUREMENT':
        ticket_body = f'''Hi {hpo_name},
        
In your latest submission, the physical measurements you submitted were either marked as missing or fell below our threshold. 
This is a high priority data quality metric: particularly height, weight, blood pressure, and heart rate. 

There is additional information linked here, along with SQL queries to help you identify the issue: https://aou-ehr-ops.zendesk.com/hc/en-us/articles/1500012365842-Physical-measurements

You can access our EHR Ops dashboard here: https://drc.aouanalytics.org/#/views/EHROpsGeneralDataQualityDashboard/Home

Please fix this data quality issue and resubmit. 

Thanks,
EHR Ops Team
        '''

    elif metric == 'COVID MAPPING':
        ticket_body = f'''Hi {hpo_name},

In your latest submission, the COVID-19 mappings you submitted were either missing or mapped incorrectly. This is a high-priority data quality metric. 

To find these errors, use the SQL query here: https://aou-ehr-ops.zendesk.com/hc/en-us/articles/1500012461581-COVID-Mapping. 

You can see the total counts of the mappings here: https://drc.aouanalytics.org/#/views/EHROpsGeneralDataQualityDashboard_16795059942430/COVIDMappingTab?:iid=1. 

Please fix your COVID-19 mappings in the measurement table and resubmit all tables.

Thanks, 

EHR Ops Team
'''
        return ticket_body
    
    elif metric == 'EHR CONSENT STATUS':
        joined_list = '\n'.join(str(id) for id in ids)
        joined_list += '\n'
        ticket_body = f'''Hi {hpo_name},

Please remove the following PMIDs from your next submission as these participants do not have EHR consent:

{joined_list}

Thanks,
EHR Ops Team
'''
        return ticket_body

    return


def ticket_update(ticket_action, zenpy_client, submission_tracking_df, src_hpo_id,
                  site_contact_df, metric, tag_list, scores=None, table_name=None, search=None, ids=None):
    
    # Get submission tracking information to internall assign the ticket
    hpo_row = submission_tracking_df.loc[submission_tracking_df['hpo_id'] ==
                                         src_hpo_id]
    hpo_row = submission_tracking_df.loc[submission_tracking_df['hpo_id'] ==
                                         src_hpo_id]
    

    site_contact_row = site_contact_df.loc[site_contact_df['hpo_id'] ==
                                         src_hpo_id]
    
    # Get site contact information to externally assign the ticket
    site_contact_email = str(site_contact_row['contact_email'].values[0]).split('; ')[0]
    assignee_email = hpo_row['contact_email'] 
    assignee = list(zenpy_client.search(type='user', email=assignee_email))
    assignee_id = -1
    if len(assignee) > 0:
       assignee_id = assignee[0].id
    requester = list(zenpy_client.search(type='user', email=site_contact_email))
    requester_id = -1
    if len(requester) > 0:
       requester_id = requester[0].id
    hpo_name = list(site_contact_df[site_contact_df['hpo_id'] == src_hpo_id]
                    ['site_name'])[0]

    if table_name is not None and table_name == '':
        metric_value = scores[f'_{metric}']
        table_phrase = table_name
    elif table_name is not None:
        metric_value = scores[table_name]
        table_name = table_name.title() + " Table"
        table_phrase = " for the " + table_name

    metric_upper = metric.replace('_', ' ').upper()

    #Define subject line text and ticket description
    subject_line = ''
    ticket_descr = ''

    
    if 'ehr_consent_status' in tag_list:
        subject_line = f"{metric_upper} Issue Flagged"
        ticket_descr = get_ticket_body(ticket_action, hpo_name, metric_upper, ids=ids)

    elif 'covid_mapping' in tag_list:
        subject_line = f"{metric_upper} Issue Flagged"
        ticket_descr = get_ticket_body(ticket_action, hpo_name, metric_upper)

    else:
        subject_line = f"{metric_upper} {table_name} Data Quality Issue Flagged"
        ticket_descr = get_ticket_body(ticket_action, hpo_name, metric_upper,
                                   metric_value=float(str(metric_value)[:4]), table_name=table_phrase)


    if ticket_action == 'comment':
        for ticket_id in search:
            ticket = zenpy_client.tickets(id=ticket_id)
            print(f'Commenting on Zendesk ticket with ID {ticket_id}...')
            ticket.comment = Comment(body=ticket_descr, public=True)
            zenpy_client.tickets.update(ticket)
        return search
    

    elif ticket_action == 'ticket':
        print('Creating Zendesk ticket...')
        ticket_audit = zenpy_client.tickets.create(
            Ticket(
                requester_id=requester_id,
                assignee_id=assignee_id,
                subject=subject_line,
                description=ticket_descr,
                tags=tag_list,
                status='pending'))
        return ticket_audit.ticket


def get_hpo_list(filled_scores_df, metric, threshold, obs_threshold):
    hpo_list = filled_scores_df[
        (filled_scores_df[f'condition_{metric}'] < threshold) |
        (filled_scores_df[f'drug_{metric}'] < threshold) |
        (filled_scores_df[f'measurement_{metric}'] < threshold) |
        (filled_scores_df[f'observation_{metric}'] < obs_threshold) |
        (filled_scores_df[f'procedure_{metric}'] < threshold) |
        (filled_scores_df[f'visit_{metric}'] < threshold) |
        (filled_scores_df[f'_{metric}'] < threshold)]
    return hpo_list


def get_table_metric(row, metric):
    metric_by_table = {
        f'_{metric}': row[f'_{metric}'],
        'condition': row[f'condition_{metric}'],
        'drug': row[f'drug_{metric}'],
        'measurement': row[f'measurement_{metric}'],
        'observation': row[f'observation_{metric}'],
        'procedure': row[f'procedure_{metric}'],
        'visit': row[f'visit_{metric}']
    }
    return metric_by_table


def get_table_name(name, metric):
    return name.split(f'_{metric}')[0]


def evaluate_metrics(zenpy_client, site_contact_df, metric, src_hpo_id,
                     submission_tracking_df=None, scores=None, ids=None):

    # Ticket status array
    ticket_status = ['open', 'pending']
    tag_list = [src_hpo_id, metric]
    audit = None

    if metric == 'ehr_consent_status' or metric == 'covid_mapping':
        # Check for existing ticket
        search = tag_intersection(zenpy_client, ticket_status, tag_list)

        # If a ticket exists comment
        if search is not None and len(search) > 0:
            if metric == 'ehr_consent_status':
                audit = ticket_update('comment', zenpy_client, submission_tracking_df, src_hpo_id,
                            site_contact_df, metric,tag_list, search=search, ids=ids)
                
            else:
                audit = ticket_update('comment', zenpy_client, submission_tracking_df, src_hpo_id,
                            site_contact_df, metric,tag_list, search=search)
                
        # If the search does not return a ticket then create a new ticket
        else:
            tag_list.append('auto')

            if metric == 'ehr_consent_status':
                audit = ticket_update('ticket', zenpy_client, submission_tracking_df, src_hpo_id,
                            site_contact_df, metric, tag_list, ids=ids)
            
            else:     
                audit = ticket_update('ticket', zenpy_client, submission_tracking_df, src_hpo_id,
                            site_contact_df, metric, tag_list)


    else:
        for name, score in scores.items():

            # If the score is low (whole row is returned for the hpo_id) begin search & comment or create ticket workflow
            if ('observation' not in name and score < 0.9) or ('observation' in name and score < 0.6):
                # search for pending and open tickets with GC-1 and hpo_id
                table_name = get_table_name(name, metric)
                if len(table_name) < 1 or metric != 'gc1':
                    tag_list.append('over-all') 
                else:
                    tag_list.append(table_name)

                
                search = tag_intersection(zenpy_client, ticket_status, tag_list)

                # TODO: Decide what to do if multiple tickets are returned by the search
                # If the search does return a ticket then add a comment to the existing ticket
                if search is not None and len(search) > 0:
                    audit = ticket_update('comment', zenpy_client, submission_tracking_df, src_hpo_id,
                                    site_contact_df, metric, tag_list, scores=scores,
                                    table_name=table_name, search=search)

                # If the search does not return a ticket then create a new ticket
                else:
                    tag_list.append('auto')
                    audit = ticket_update('ticket', zenpy_client, submission_tracking_df, src_hpo_id,
                                site_contact_df, metric, tag_list, scores=scores,
                                    table_name=table_name)
                    

    return audit


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

    # Get Site Contact data
    # sheet = service.spreadsheets()
    # site_contact_result = sheet.values().get(spreadsheetId='1aS9etPRp1pHpms-uL1mnwoqK7B6uuWfXgU3MabbCyFE',
    #                             range='Site Contacts').execute()
    # site_contact_values = site_contact_result.get('values')
    # site_contact_df = pd.DataFrame(site_contact_values[1:], columns=site_contact_values[0])

    # Get TEST Site Contact data
    sheet = service.spreadsheets()
    site_contact_result = sheet.values().get(
        spreadsheetId=os.getenv('SITE_CONTACT_ID'),
        range='Site Contacts').execute()
    site_contact_values = site_contact_result.get('values')
    site_contact_df = pd.DataFrame(site_contact_values[1:],
                                   columns=site_contact_values[0])

    # Get Submission Tracking data
    submission_tracking_result = sheet.values().get(
        spreadsheetId=os.getenv('SUBMISSION_TRACK_ID'),
        range='Mapping').execute()
    submission_tracking_values = submission_tracking_result.get('values')
    submission_tracking_df = pd.DataFrame(
        submission_tracking_values[1:], columns=submission_tracking_values[0])

    return [site_contact_df, submission_tracking_df]

# CALCULATE EACH METRIC
def ticket_automation():

    zenpy_client = Zenpy(**CREDENTIALS)

    # Create bigquery client
    bigquery_client = bigquery.Client()

    site_contact_df, submission_tracking_df = get_sheets()
    

    latest_submission_job_config = bigquery.QueryJobConfig()
    ls = open(f'latestSubmissions.sql')
    LS_QUERY = ls.read()
    # API request
    ls_query_job = bigquery_client.query(LS_QUERY, job_config=latest_submission_job_config)
    latest_submission_hpos_df = ls_query_job.result().to_dataframe()
    hpo_list = latest_submission_hpos_df['hpo_id'].str.lower().values.tolist()
    hpo_set = set(hpo_list)

    # get symmetric difference between hpo_list and list to exclude
    exclude = ['waianae_coast_comprehensive', 'saou_hhsys', 'unc_chapel_hill', 'UPR_COMPREHENSIVE_CANCER_CENTER', 'CAL_PMC_USC_ALTAMED', 'wash_u_stl', 'vcu']
    exclude = list(map(str.lower,exclude))

    included_hpos = list(set(hpo_list) - set(exclude))

    metrics = ['gc1', 'ehr_consent_status', 'covid_mapping']

    for metric in metrics:

        # GC1 query/calculation for included hpos
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ArrayQueryParameter("included_hpos", "STRING",
                                         included_hpos)
        ])
        f = open(f'{metric}.sql')
        QUERY = f.read()
        # API request
        query_job = bigquery_client.query(QUERY, job_config=job_config)
        scores_df = query_job.result().to_dataframe()

        print(scores_df)

        if metric == 'ehr_consent_status' and scores_df.shape[0] > 0:
            hpo_ehr_consent_issues = []

            # Group returned values by HPO
            for hpo, hpo_df in scores_df.groupby('HPO_ID'):
                hpo_ehr_consent_issues.append(hpo_df)


            # Handle each HPO with consent issues
            for hpo in hpo_ehr_consent_issues:
                # Get HPO id
                hpo_id = hpo['HPO_ID'][0]

                # Get PMID list
                ids = list(hpo['participant_id'])

                # Evaluate results of EHR consent query
                evaluate_metrics(zenpy_client, site_contact_df, metric, hpo_id,
                                 submission_tracking_df=submission_tracking_df, ids=ids)
                
        elif metric == 'covid_mapping' and scores_df.shape[0] > 0:
            # Handle each HPO with covid mapping issues
            for i, hpo in scores_df.iterrows():
                # Get HPO id
                hpo_id = hpo['HPO_ID']

                # Evaluate results of EHR consent query
                evaluate_metrics(zenpy_client, site_contact_df, metric, hpo_id,
                                 submission_tracking_df=submission_tracking_df)

        else:

            filled_scores_df = scores_df.fillna(0)

            # If metrics are returned
            if filled_scores_df.shape[0] > 0:
                # TODO: should I just remove the metric name from the column names so the variables can be generic for each metric?
                # TODO: do I need this anymore - I think this is included in the initial query
                # TODO ROLLOUT: need to increase threshold to test with columbia - set back to 0.9 for production
                # Find all hpos with scores below the threshold
                low_scores_df = get_hpo_list(filled_scores_df,
                                            metric,
                                            threshold=0.9,
                                            obs_threshold=0.6)

                # For each hpo with a low score evaluate which table has the low metric
                for _, row in low_scores_df.iterrows():
                    src_hpo_id = row['src_hpo_id']

                    # Dictionary of current hpo's tables and respective calculation
                    scores = get_table_metric(row, metric)

                    # 
                    evaluate_metrics(zenpy_client, site_contact_df, metric, src_hpo_id,
                                    submission_tracking_df=submission_tracking_df, scores=scores)


if __name__ == '__main__':
    ticket_automation()