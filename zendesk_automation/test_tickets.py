import io
import os
import utils
import unittest
import shutil
import ticketAutomation as ta
import pandas as pd
from zenpy import Zenpy
from zenpy.lib.api_objects import Ticket, User, Comment, BaseObject
from google.cloud import bigquery
from googleapiclient.discovery import build
from google.oauth2 import service_account
import json


class TestBasicInput(unittest.TestCase):
    @classmethod

    def initialize_class_vars(cls):
        cls.cleanup = []

    def tearDownClass(cls) -> None:
        for ticket in cls.cleanup_ids:
            try:
                cls.zenpy_client.tickets.delete(ticket)
            except Zenpy.ApiException:
                print()

        return super().tearDownClass()
    
    def setUp(cls) -> None:
        credentials = {
            "email": utils.EMAIL,
            "token": utils.ZENPY_TOKEN,
            "subdomain": utils.SUBDOMAIN
        }
        cls.zenpy_client = Zenpy(**credentials)

        return super().setUp()
    
    def test_create_ticket(cls):
        site_contact_df, submission_tracking_df = ta.get_sheets()
        src_hpo_id = 'test_create_ticket'
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
        ticket_obj = ta.create_ticket(cls.zenpy_client, submission_tracking_df, src_hpo_id, site_contact_df, scores, metric, table_name, tag_list)
        expected_subject = ''
        expected_body = ''
        

    # TODO: Create ticket scenario to test intersection of tags, compare list returned to expected list
    def test_tag_intersection(cls):
        
        status_list = ['open', 'pending']
        tag_list = ['auto-test-tickets']
        shared_ids = ta.tag_intersection(cls.zenpy_client, status_list, tag_list)
        cls.assertEqual(shared_ids)






        
if __name__ == '__main__':
    unittest.main()
    

