import os.path
import pkg_resources
import shutil
import tempfile
import unittest

import mock

from org.bccvl.movelib import move


class AekosTest(unittest.TestCase):

    occurrence_source = {
        'url': 'aekos://occurrence?speciesName=Abutilon%20fraseri'
    }

    traits_source = {
        'url': 'aekos://traits?speciesName=Abutilon%20fraseri&traitName=lifeForm%2ClifeStage&envVarName=aspect%2Cslope%2CelectricalConductivity%2CpH'
    }

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        if self.tmpdir and os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)

    def _urlretrieve(self, url, dest=None):
        # 1. occurrence_url
        if url.startswith('http://biocache.ala.org.au/ws/occurrences'):
            temp_file = os.path.join(self.tmpdir, 'ala_data.zip')
            shutil.copy(pkg_resources.resource_filename(__name__, 'data/ala_data.zip'),
                        temp_file)
            return (temp_file, None)  # fd is ignored
        # 2. metadata_url, destpath
        if url.startswith('http://bie.ala.org.au/ws/species'):
            shutil.copy(pkg_resources.resource_filename(__name__, 'data/ala_metadata.json'),
                        dest)
            return (dest, None)

    #@mock.patch('urllib.urlretrieve')
    def test_aekos_occurrence_to_file(self, mock_urlretrieve=None):
        #mock_urlretrieve.side_effect = self._urlretrieve

        file_dest = {
            'url': 'file://{0}'.format(self.tmpdir)
        }
        move(self.occurrence_source, file_dest)

        # Check for these files are created
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'aekos_metadata.json')))
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'aekos_dataset.json')))
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'aekos_occurrence.zip')))
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'data', 'aekos_occurrence.csv')))
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'data', 'aekos_citation.txt')))


    #@mock.patch('urllib.urlretrieve')
    def test_aekos_traits_to_file(self, mock_urlretrieve=None):
        #mock_urlretrieve.side_effect = self._urlretrieve

        file_dest = {
            'url': 'file://{}'.format(self.tmpdir)
        }
        move(self.traits_source, file_dest)

        # Check for these files are created
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'aekos_dataset.json')))
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'aekos_traits_env.zip')))
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'data', 'aekos_traits_env.csv')))
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'data', 'aekos_citation.txt')))
