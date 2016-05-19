import os.path
import shutil
import tempfile
import unittest

import mock

from org.bccvl.movelib import move
from org.bccvl.movelib.utils import AuthTkt


class HTTPTest(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        if self.tmpdir and os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)

    # call to get
    @mock.patch('requests.Session')
    def test_http_to_file(self, mock_SessionClass=None):
        mock_session = mock_SessionClass.return_value  # get mock response
        mock_response = mock_session.get.return_value
        mock_response.iter_content.return_value = ['test content']
        mock_headers = mock_response.headers
        mock_headers.get.return_value = 'text/csv'

        ticket = AuthTkt('ibycgtpw', 'admin')
        cookies = {
            'name': '__ac',
            'value': ticket.ticket(),
            'domain': '',
            'path': '/',
            'secure': True
        }
        http_source = {
            'url': 'http://www.bccvl.org.au/datasets/test.csv',
            'cookies': cookies
        }
        file_dest = {
            'url': 'file://{}'.format(self.tmpdir)
        }

        move(http_source, file_dest)

        # verify destination file
        dest_file = os.path.join(self.tmpdir, 'test.csv')
        self.assertTrue(os.path.exists(dest_file))
        self.assertEqual(open(dest_file).read(), 'test content')
