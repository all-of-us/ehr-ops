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
    
    # TODO: Test ticket creation function 
    def test_create_ticket(cls):
        cls.assertEqual()
    
    # TODO: Create ticket scenario to test intersection of tags, compare list returned to expected list
    def test_tag_intersection(cls):
        status_list = ['open', 'pending']
        tag_list = ['auto-test-tickets']
        shared_ids = ta.tag_intersection(cls.zenpy_client, status_list, tag_list)






        
if __name__ == '__main__':
    unittest.main()
    

