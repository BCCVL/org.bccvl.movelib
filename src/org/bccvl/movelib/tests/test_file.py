import unittest
import os.path
import pkg_resources
import shutil
import tempfile

from org.bccvl.movelib import move


class FileTest(unittest.TestCase):

    file_source = {
        'url': 'file://{}'.format(pkg_resources.resource_filename(__name__, 'data/test.csv'))
    }

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        if self.tmpdir and os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)

    def test_file_to_file(self, mock_copy=None):
        file_dest = {
            'url': 'file://{}'.format(self.tmpdir)
        }
        move(self.file_source, file_dest)

        # verify destination file
        dest_file = os.path.join(self.tmpdir, 'test.csv')
        self.assertTrue(os.path.exists(dest_file))
        self.assertEqual(open(dest_file, 'rb').read(), pkg_resources.resource_string(__name__, 'data/test.csv'))
