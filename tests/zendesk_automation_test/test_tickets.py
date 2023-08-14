import os
import unittest
from zendesk_automation import ticketAutomation as ta
import pandas as pd
from zenpy import Zenpy


class TestBasicInput(unittest.TestCase):
    def tearDown(self) -> None:
        for ticket in self.cleanup:
            try:
                self.zenpy_client.tickets.delete(ticket)
            except Zenpy.ApiException:
                print()

    def setUp(self) -> None:

        self.cleanup = []

        CREDENTIALS = {
            "email": os.environ['EMAIL'],
            "token": os.environ['ZENPY_TOKEN'],
            "subdomain": os.environ['SUBDOMAIN']
        }
        self.zenpy_client = Zenpy(**CREDENTIALS)

    def test_create_ticket(self):
        pd.set_option('display.max_columns', None)
        contact_data = [['Test', 'test_create_ticket', os.environ['EMAIL']]]
        site_contact_df = pd.DataFrame(
            contact_data,
            columns=[
                'Site Name', 'hpo_id',
                'Point of Contact'
            ])
        submission_data = [[
            'Test', 'Elise', os.environ['EMAIL'], 'test_create_ticket'
        ]]
        submission_tracking_df = pd.DataFrame(
            submission_data,
            columns=['Site Name', 'contact', 'contact_email', 'hpo_id'])

        src_hpo_id = 'test_create_ticket'
        hpo_name = 'Test'
        metric = 'gc1'
        scores = {
            f'_{metric}': 0.97510765,
            'condition': 0.940730566,
            'drug': 0.825231502,
            'measurement': 0,
            'observation': 1,
            'procedure': 0.89552811,
            'visit': 0.862934988
        }
        table_name = 'condition'
        tag_list = [
            'test_create_ticket', metric, table_name, 'auto'
        ]
        ticket_obj = ta.ticket_update('ticket', self.zenpy_client,
                                      submission_tracking_df, src_hpo_id,
                                      site_contact_df, metric, 
                                      tag_list, scores, table_name)
        ticket_body = list(
            self.zenpy_client.tickets.comments(ticket=ticket_obj.id))[0].body
        ticket_subject = ticket_obj.subject
        expected_subject = f"GC1 Condition Table Data Quality Issue Flagged"
        expected_body = f'''Hi Test, 
        
In your latest submission, your GC-1 rate was 0.94 for the Condition Table, which is below our acceptance threshold. GC-1 measures conformance to OMOP standard concepts and is a priority for data quality. 
        
There is additional information linked here, along with SQL queries to help you identify the issue: https://aou-ehr-ops.zendesk.com/hc/en-us/articles/1500012365822-NIH-Grant-Award-Metrics-#h_01F7AB48M32ZRGYAVQXWAX92W6

You can access our EHR Ops dashboard here: https://drc.aouanalytics.org/#/views/EHROpsGeneralDataQualityDashboard/Home

Please fix this data quality issue and resubmit. 

Thanks, 
EHR Ops Team'''
        self.cleanup.append(ticket_obj)
        self.assertTrue(ticket_subject == expected_subject)
        self.assertMultiLineEqual(ticket_body, expected_body)


    def test_ehr_consent_ticket(self):
        # provide data that would generate an EHR consent issue
        zenpy_client = self.zenpy_client
        contact_data = [['Test', 'test_create_ticket', os.environ['EMAIL']]]
        site_contact_df = site_contact_df = pd.DataFrame(
            contact_data,
            columns=[
                'Site Name', 'hpo_id',
                'Point of Contact'
            ])
        submission_data = [[
            'Test', 'Elise', os.environ['EMAIL'], 'test_create_ticket'
        ]]
        submission_tracking_df = pd.DataFrame(
            submission_data,
            columns=['Site Name', 'contact', 'contact_email', 'hpo_id'])

        hpo_id = 'test_create_ticket'
        hpo_name = 'Test'
        metric = 'ehr_consent_status'
        ids = [123456, 234567, 345678, 456789]
        tag_list = [
            'test_create_ticket', metric, 'auto'
        ]
        ta.evaluate_metrics(zenpy_client, site_contact_df, metric, hpo_id,
                                 submission_tracking_df=submission_tracking_df, ids=ids)

        results = ta.tag_intersection(zenpy_client, status_list=['open', 'pending'], tag_list=tag_list)
        self.assertTrue(len(results) > 0)
        tag_result_id = results[0]
        ticket = zenpy_client.tickets(id=tag_result_id)             
        ticket_body = ticket.description
        ticket_subject = ticket.subject
        expected_subject = f"EHR CONSENT STATUS Issue Flagged"
        expected_body = '''Hi Test,

Please remove the following PMIDs from your next submission as these participants do not have EHR consent:

123456
234567
345678
456789


Thanks,
EHR Ops Team
'''
        self.cleanup.append(ticket)
        self.assertTrue(ticket_subject == expected_subject)
        self.assertMultiLineEqual(ticket_body, expected_body)


        

if __name__ == '__main__':
    unittest.main()
