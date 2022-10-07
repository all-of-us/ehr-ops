#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging
import sys
import unittest


class BaseTestCase(unittest.TestCase):
    """ Base class for unittests. """

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)

    def setUp(self) -> None:
        super().setUp()

        logger = logging.getLogger()
        stream_handler = logging.StreamHandler(sys.stdout)
        logger.addHandler(stream_handler)
        # Change this to logging.ERROR when you want to see API server errors.
        logger.setLevel(logging.CRITICAL)

    def tearDown(self) -> None:
        super().tearDown()
