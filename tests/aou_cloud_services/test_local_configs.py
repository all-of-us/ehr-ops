#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from tests.helpers.unittest_base import BaseTestCase

from aou_cloud.services.gcp_cloud_app_config import get_config_provider, LocalFilesystemConfigProvider
from python_easy_json import JSONObject


class TestAppConfigs(BaseTestCase):

    def test_app_config(self):
        """ Basic test to see if the local app config can be loaded using 'get_config_provider'. """
        provider: LocalFilesystemConfigProvider = get_config_provider()
        self.assertIsNotNone(provider)
        self.assertTrue(isinstance(provider, LocalFilesystemConfigProvider))

        config = JSONObject(provider.read(name='app_config'))
        self.assertIsNotNone(config)

        users = config.users
        self.assertIsNotNone(users)

        self.assertEqual(users[0].email, 'example@example.com')

    def test_db_config(self):
        """ Basic test to see if the local app config can be loaded using 'get_config_provider'. """
        provider: LocalFilesystemConfigProvider = get_config_provider()
        self.assertIsNotNone(provider)
        self.assertTrue(isinstance(provider, LocalFilesystemConfigProvider))

        config = JSONObject(provider.read(name='db_config'))
        self.assertIsNotNone(config)

        self.assertEqual(["drc",], config.databases)

        self.assertEqual(config.users[0].name, 'ehr_ops')
