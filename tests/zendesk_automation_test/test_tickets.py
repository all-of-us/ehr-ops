
import io
import os
import sys
import unittest
import shutil
from zendesk_automation.ticketAutomation import ticketAutomation as ta
import pandas as pd
from zenpy import Zenpy
from zenpy.lib.api_objects import Ticket, User, Comment, BaseObject
from google.cloud import bigquery
from googleapiclient.discovery import build
from google.oauth2 import service_account
import json


class TestBasicInput(unittest.TestCase):

    @classmethod
    def tearDownClass(cls) -> None:
        for ticket in cls.cleanup:
            try:
                cls.zenpy_client.tickets.delete(ticket)
            except Zenpy.ApiException:
                print()

        return super().tearDownClass(cls)

    @classmethod
    def setUp(cls) -> None:

        cls.cleanup = []

        CREDENTIALS = {
            "email": os.environ['EMAIL'],
            "token": os.environ['ZENPY_TOKEN'],
            "subdomain": os.environ['SUBDOMAIN']
        }
        cls.zenpy_client = Zenpy(**credentials)

        return super().setUp(cls)
    
    def test_create_ticket(cls):
        pd.set_option('display.max_columns', None)
        contact_data = [['Test', 'test_create_ticket', 'em3697@cumc.columbia.edu']]
        site_contact_df = pd.DataFrame(contact_data, columns=['Site Name', 'hpo_id', 'Point of Contact (mostly data stewards)\n*Use semi-colons to separate the email addresses*'])
        submission_data = [['Test', 'Elise', 'em3697@cumc.columbia.edu', 'test_create_ticket']]
        submission_tracking_df = pd.DataFrame(submission_data, columns=['Site Name', 'contact', 'contact_email', 'hpo_id'])

        src_hpo_id = 'test_create_ticket'
        hpo_name = 'Test'
        metric = 'gc1'
        scores = {f'_{metric}': 0.97510765, 
                        'condition': 0.940730566, 
                        'drug': 0.825231502, 
                        'measurement': 0, 
                        'observation': 1, 
                        'procedure': 0.89552811, 
                        'visit': 0.862934988}
        table_name = 'condition'
        tag_list = ['test_create_ticket', metric, table_name, 'auto-test-tickets']
        ticket_obj = ta.create_ticket(cls.zenpy_client, submission_tracking_df, src_hpo_id, site_contact_df, scores, metric, table_name, tag_list, hpo_name)
        ticket_body = list(cls.zenpy_client.tickets.comments(ticket=ticket_obj.id))[0].body
        ticket_subject = ticket_obj.subject
        expected_subject = f"GC1 Condition Table Data Quality Issue Flagged"
        expected_body = f'''Hi Test, 
        
        In your latest submission, your GC-1 rate was 0.94 for the Condition Table, which is below our acceptance threshold. GC-1 measures conformance to OMOP standard concepts and is a priority for data quality. 
        
        There is additional information linked here, along with SQL queries to help you identify the issue: https://aou-ehr-ops.zendesk.com/hc/en-us/articles/1500012365822-NIH-Grant-Award-Metrics-

        You can access our EHR Ops dashboard here: https://drc.aouanalytics.org/#/views/EHROpsGeneralDataQualityDashboard/Home

        Please fix this data quality issue and resubmit. 

        Thanks, 
        EHR Ops Team'''
        cls.cleanup.append(ticket_obj)
        cls.assertTrue(ticket_subject == expected_subject)
        cls.assertMultiLineEqual(ticket_body, expected_body) 

        
if __name__ == '__main__':
    unittest.main()
    

