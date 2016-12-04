import os.path
import shutil
import tempfile
import unittest

import mock

from org.bccvl.movelib import move


class SwiftTest(unittest.TestCase):

    swift_source = {
        'url': 'swift+https://swift.example.com/v1/account/container2/test/test2.txt',
        'auth': 'https://keystone.example.com:5000/v2.0/',
        'user': 'username@example.com',
        'key': 'password',
        'os_tenant_name': 'pt-12345',
        'auth_version': '2'
    }

    swift_dest = {
        'url': 'swift+https://swift.example.com/v1/account/container2/testup.txt',
        'auth': 'https://keystone.example.com:5000/v2.0/',
        'user': 'username@example.com',
        'key': 'password',
        'os_tenant_name': 'pt-12345',
        'auth_version': '2'
    }

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        if self.tmpdir and os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)

    def _swift_download(self, container, objects, output):
        # container = 'test'
        # objects = ['test2.text']
        # output = {'out_file': '/tmp/movelib_Rut6v7/tmp_move_file'}
        open(output['out_file'], 'w').write('test content')
        return [{
            'success': True,
            'response_dict': {
                'headers': {
                    'content-type': 'application/octet-stream'
                }
            }
        }]

    @mock.patch('org.bccvl.movelib.protocol.swift.SwiftService')
    def test_swift_to_swift(self, mock_SwiftService=None):
        mock_swiftservice = mock_SwiftService.return_value
        mock_swiftservice.upload.return_value = [{'success': True}]  # simulate successful upload
        mock_swiftservice.download.side_effect = self._swift_download

        move(self.swift_source, self.swift_dest)

        mock_SwiftService.assert_has_calls([
            # init SwiftService
            mock.call(mock.ANY),
            mock.call().download('container2', ['test/test2.txt'], {'out_file': mock.ANY}),
            # init SwiftService
            mock.call(mock.ANY),
            # TODO: mock.ANY here is a SwiftUploadObject, can we verify that in more detail? like object name etc...
            mock.call().upload('container2', [mock.ANY]),
        ])

    @mock.patch('org.bccvl.movelib.protocol.swift.SwiftService')
    def test_swift_to_file(self, mock_SwiftService=None):
        mock_swiftservice = mock_SwiftService.return_value
        mock_swiftservice.download.side_effect = self._swift_download

        file_dest = {
            'url': 'file://{}'.format(self.tmpdir)
        }
        move(self.swift_source, file_dest)

        dest_file = os.path.join(self.tmpdir, 'test2.txt')
        mock_SwiftService.assert_has_calls([
            # init SwiftService
            mock.call(mock.ANY),
            mock.call().download('container2', ['test/test2.txt'], {'out_file': dest_file}),
        ])
        # assert dest file?
        self.assertTrue(os.path.exists(dest_file))
        self.assertEqual(open(dest_file).read(), 'test content')
