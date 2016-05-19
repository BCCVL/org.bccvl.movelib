import os.path
import pkg_resources
import shutil
import tempfile
import unittest

import mock

from org.bccvl.movelib import move


class GBIFTest(unittest.TestCase):

    gbif_source = {
        'url': 'gbif://gbif/?lsid=urn:lsid:biodiversity.org.au:afd.taxon:31a9b8b8-4e8f-4343-a15f-2ed24e0bf1ae'
    }

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        if self.tmpdir and os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)

    def _urlretrieve(self, url, dest=None):
        # 1. occurrence_url
        if url.startswith('http://api.gbif.org/v1/occurrence'):
            temp_file = os.path.join(self.tmpdir, 'gbif_occurrence.json')
            shutil.copy(pkg_resources.resource_filename(__name__, 'data/gbif_occurrence.json'),
                        temp_file)
            return (temp_file, None)  # fd is ignored
        # 2. metadata_url, destpath
        if url.startswith('http://api.gbif.org/v1/species/'):
            shutil.copy(pkg_resources.resource_filename(__name__, 'data/gbif_metadata.json'),
                        dest)
            return (dest, None)

    @mock.patch('urllib.urlretrieve')
    def test_gbif_to_file(self, mock_urlretrieve=None):
        mock_urlretrieve.side_effect = self._urlretrieve
        # mock urllib.urlretrieve ....
        #        return zip file with data.csv and citation.csv
        # mock urllib.urlretriev ...
        #        return gbif_metadata.json

        file_dest = {
            'url': 'file://{}'.format(self.tmpdir)
        }
        move(self.gbif_source, file_dest)

        # verify call scp.put
        # verify gbif calls?
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'gbif_dataset.json')))
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'gbif_occurrence.csv')))
