import os.path
import shutil
import tempfile
import unittest

import mock

from org.bccvl.movelib import move


class SCPTest(unittest.TestCase):

    scp_source = {
        'url': 'scp://username:password@hostname/srcpath/test.csv'
    }

    scp_dest = {
        'url':
        'scp://username:password@hostname/destpath',
        'filename': 'test.csv'
    }

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        if self.tmpdir and os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)

    def _scp_get(self, src, dest, recursive):
        tmpfile = os.path.join(self.tmpdir, 'test.csv')
        with (open(tmpfile, 'w')) as f:
            f.write('test content')

    @mock.patch('org.bccvl.movelib.protocol.scp.SCPClient')
    @mock.patch('org.bccvl.movelib.protocol.scp.SSHClient')
    def test_scp_to_scp(self, mock_SSHClient=None, mock_SCPClient=None):
        mock_scp = mock_SCPClient.return_value
        mock_scp.get.side_effect = self._scp_get
        move(self.scp_source, self.scp_dest)

        # verify calls
        mock_SCPClient.assert_has_calls([
            # init with ssh transport
            mock.call(mock.ANY),
            # fetch to local tempfile
            mock.call().get('/srcpath/test.csv', mock.ANY, recursive=False),
            # init with ssh transport
            mock.call(mock.ANY),
            # push local temp to remote
            mock.call().put(mock.ANY, '/destpath/test.csv', recursive=True)
        ])

    @mock.patch('org.bccvl.movelib.protocol.scp.SCPClient')
    @mock.patch('org.bccvl.movelib.protocol.scp.SSHClient')
    def test_scp_to_file(self, mock_SSHClient=None, mock_SCPClient=None):
        mock_scp = mock_SCPClient.return_value
        mock_scp.get.side_effect = self._scp_get

        file_dest = {
            'url': 'file://{}'.format(self.tmpdir)
        }
        move(self.scp_source, file_dest)

        # verify scp calls
        mock_SCPClient.assert_has_calls([
            # init with ssh transport
            mock.call(mock.ANY),
            # fetch to local tempfile
            mock.call().get('/srcpath/test.csv', mock.ANY, recursive=False)
        ])
        # verify destination file
        dest_file = os.path.join(self.tmpdir, 'test.csv')
        self.assertTrue(os.path.exists(dest_file))
        self.assertEqual(open(dest_file).read(), 'test content')
