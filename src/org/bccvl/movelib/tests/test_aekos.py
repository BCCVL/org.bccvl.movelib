import filecmp
import io
import os.path
from pkg_resources import resource_filename
import shutil
import tempfile
import json
import unittest
from urllib import urlencode

import mock

from org.bccvl.movelib import move


class AekosTest(unittest.TestCase):

    occurrence_source = {
        'url': 'aekos://occurrence?{}'.format(
            urlencode({
                'speciesNames': ['Abutilon halophilum']
            }, True)
        )
    }

    traits_source = {
        'url': 'aekos://traits?{}'.format(
            urlencode({
                'speciesNames': ['Abutilon halophilum'],
                'traitNames': ['heght', 'LifeForm'],
                'varNames': ['aspect', 'electricalConductivity']
            }, True)
        )
    }

    AEKOS_API_BASE = 'https://test.api.aekos.org.au/v2'

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        if self.tmpdir and os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)

    def _download_as_file(self, url, data, dest_file):
        # 1. occurrence_url
        if url.startswith('{}/speciesData.json'.format(self.AEKOS_API_BASE)):
            resp = [json.load(open(resource_filename(__name__, 'data/aekos_occurrence.json')))]
            with io.open(dest_file, 'wb') as f:
                json.dump(resp, f)            
        # 2. metadata_url, destpath
        elif url.startswith('{}/speciesSummary.json'.format(self.AEKOS_API_BASE)):
            resp = [json.load(open(resource_filename(__name__, 'data/aekos_metadata.json')))]
            with io.open(dest_file, 'wb') as f:
                json.dump(resp, f)            
        elif url.startswith('{}/traitData.json'.format(self.AEKOS_API_BASE)):
            resp = [json.load(open(resource_filename(__name__, 'data/aekos_trait_data.json')))]
            with io.open(dest_file, 'wb') as f:
                json.dump(resp, f)                        
        elif url.startswith('{}/environmentData.json'.format(self.AEKOS_API_BASE)):
            resp = [json.load(open(resource_filename(__name__, 'data/aekos_env_data.json')))]
            with io.open(dest_file, 'wb') as f:
                json.dump(resp, f)                                    

    def _download_multispecies(self, url, data, dest_file):
        # 1. occurrence_url
        if url.startswith('{}/speciesData.json'.format(self.AEKOS_API_BASE)):
            resp = [json.load(open(resource_filename(__name__, 'data/aekos_occurrence.json')))]
            with io.open(dest_file, 'wb') as f:
                json.dump(resp, f)                        
        # 2. metadata_url, destpath
        elif url.startswith('{}/speciesSummary.json'.format(self.AEKOS_API_BASE)):
            resp = [json.load(open(resource_filename(__name__, 'data/aekos_metadata.json')))]
            with io.open(dest_file, 'wb') as f:
                json.dump(resp, f)                        
        elif url.startswith('{}/traitData.json'.format(self.AEKOS_API_BASE)):
            resp = [json.load(open(resource_filename(__name__, 'data/aekos_trait_data_multispecies.json')))]
            with io.open(dest_file, 'wb') as f:
                json.dump(resp, f)                        
        elif url.startswith('{}/environmentData.json'.format(self.AEKOS_API_BASE)):
            resp = [json.load(open(resource_filename(__name__, 'data/aekos_env_data.json')))]
            with io.open(dest_file, 'wb') as f:
                json.dump(resp, f)                        

    @mock.patch('org.bccvl.movelib.protocol.aekos._download_as_file')
    def test_aekos_occurrence_to_file(self, mock_download_as_file=None):
        mock_download_as_file.side_effect = self._download_as_file

        file_dest = {
            'url': 'file://{0}'.format(self.tmpdir)
        }
        move(self.occurrence_source, file_dest)

        # Check for these files are created
        self.assertTrue(os.path.exists(
            os.path.join(self.tmpdir, 'aekos_metadata.json')))
        self.assertTrue(os.path.exists(
            os.path.join(self.tmpdir, 'aekos_dataset.json')))
        self.assertTrue(os.path.exists(os.path.join(
            self.tmpdir, 'aekos_occurrence.zip')))
        self.assertTrue(os.path.exists(os.path.join(
            self.tmpdir, 'data', 'aekos_occurrence.csv')))
        self.assertTrue(os.path.exists(os.path.join(
            self.tmpdir, 'data', 'aekos_citation.txt')))

        # Check file content
        md1 = json.load(open(os.path.join(self.tmpdir, 'aekos_metadata.json')))
        md2 = json.load(open(resource_filename(__name__, 'data/aekos_metadata.json')))
        self.assertTrue(len(md1) == len(md2))
        for i in md1:
            self.assertTrue(i in md2)

        self.assertTrue(filecmp.cmp(os.path.join(self.tmpdir, 'data', 'aekos_occurrence.csv'),
                                    resource_filename(__name__, 'data/aekos_occurrence.csv')))
        self.assertTrue(filecmp.cmp(os.path.join(self.tmpdir, 'data', 'aekos_citation.txt'),
                                    resource_filename(__name__, 'data/aekos_citation.txt')))

    @mock.patch('org.bccvl.movelib.protocol.aekos._download_as_file')
    def test_aekos_traits_to_file(self, mock_download_as_file=None):
        mock_download_as_file.side_effect = self._download_as_file

        file_dest = {
            'url': 'file://{}'.format(self.tmpdir)
        }
        move(self.traits_source, file_dest)

        # Check for these files are created
        self.assertTrue(os.path.exists(
            os.path.join(self.tmpdir, 'aekos_dataset.json')))
        self.assertTrue(os.path.exists(os.path.join(
            self.tmpdir, 'aekos_traits_env.zip')))
        self.assertTrue(os.path.exists(os.path.join(
            self.tmpdir, 'data', 'aekos_traits_env.csv')))
        self.assertTrue(os.path.exists(os.path.join(
            self.tmpdir, 'data', 'aekos_citation.csv')))

        # Check file content
        # traits env data is written from a dictionary ... order of data in dictionary is undefined so we have
        # to read the lines manually and compare the sets
        self.assertEqual(
            set(open(os.path.join(self.tmpdir, 'data', 'aekos_traits_env.csv')).readlines()),
            set(open(resource_filename(__name__, 'data/aekos_traits_env.csv')).readlines())
        )
        # self.assertTrue(filecmp.cmp(os.path.join(self.tmpdir, 'data', 'aekos_traits_env.csv'),
        #                             resource_filename(__name__, 'data/aekos_traits_env.csv')))
        self.assertEqual(
            set(open(os.path.join(self.tmpdir, 'data', 'aekos_citation.csv')).readlines()),
            set(open(resource_filename(__name__, 'data/aekos_citation.csv')).readlines())
        )
        # self.assertTrue(filecmp.cmp(os.path.join(self.tmpdir, 'data', 'aekos_citation.csv'),
        #                             resource_filename(__name__, 'data/aekos_citation.csv')))

    @mock.patch('org.bccvl.movelib.protocol.aekos._download_as_file')
    def test_aekos_traits_to_file_no_envvar(self, mock_download_as_file=None):
        mock_download_as_file.side_effect = self._download_as_file

        self.traits_source = {
            'url': 'aekos://traits?{}'.format(
                urlencode({
                    'speciesNames': ['Abutilon halophilum'],
                    'traitNames': ['height', 'lifeForm'],
                    'varNames': None
                }, True)
            )
        }

        file_dest = {
            'url': 'file://{}'.format(self.tmpdir)
        }
        move(self.traits_source, file_dest)

        # Check for these files are created
        self.assertTrue(os.path.exists(
            os.path.join(self.tmpdir, 'aekos_dataset.json')))
        self.assertTrue(os.path.exists(os.path.join(
            self.tmpdir, 'aekos_traits_env.zip')))
        self.assertTrue(os.path.exists(os.path.join(
            self.tmpdir, 'data', 'aekos_traits_env.csv')))
        self.assertTrue(os.path.exists(os.path.join(
            self.tmpdir, 'data', 'aekos_citation.csv')))

        # Check file content
        self.assertTrue(filecmp.cmp(os.path.join(self.tmpdir, 'data', 'aekos_traits_env.csv'),
                                    resource_filename(__name__, 'data/aekos_traits_env_no_env.csv')))
        self.assertTrue(filecmp.cmp(os.path.join(self.tmpdir, 'data', 'aekos_citation.csv'),
                                    resource_filename(__name__, 'data/aekos_citation_no_env.csv')))

    @mock.patch('org.bccvl.movelib.protocol.aekos._download_as_file')
    def test_aekos_traits_to_file_no_trait(self, mock_download_as_file=None):
        mock_download_as_file.side_effect = self._download_as_file

        self.traits_source = {
            'url': 'aekos://traits?{}'.format(
                urlencode({
                    'speciesNames': ['Abutilon halophilum'],
                    'traitNames': None,
                    'varNames': ['aspect', 'electricalConductivity'],
                })
            )
        }

        file_dest = {
            'url': 'file://{}'.format(self.tmpdir)
        }
        move(self.traits_source, file_dest)

        # Check for these files are created
        self.assertTrue(os.path.exists(
            os.path.join(self.tmpdir, 'aekos_dataset.json')))
        self.assertTrue(os.path.exists(os.path.join(
            self.tmpdir, 'aekos_traits_env.zip')))
        self.assertTrue(os.path.exists(os.path.join(
            self.tmpdir, 'data', 'aekos_traits_env.csv')))
        self.assertTrue(os.path.exists(os.path.join(
            self.tmpdir, 'data', 'aekos_citation.csv')))

        # Check file content
        self.assertEqual(
            set(open(os.path.join(self.tmpdir, 'data', 'aekos_traits_env.csv')).readlines()),
            set(open(resource_filename(__name__, 'data/aekos_traits_env_no_trait.csv')).readlines())
        )
        # self.assertTrue(filecmp.cmp(os.path.join(self.tmpdir, 'data', 'aekos_traits_env.csv'),
        #                             resource_filename(__name__, 'data/aekos_traits_env_no_trait.csv')))
        self.assertEqual(
            set(open(os.path.join(self.tmpdir, 'data', 'aekos_citation.csv')).readlines()),
            set(open(resource_filename(__name__, 'data/aekos_citation_no_trait.csv')).readlines())
        )
        # self.assertTrue(filecmp.cmp(os.path.join(self.tmpdir, 'data', 'aekos_citation.csv'),
        #                             resource_filename(__name__, 'data/aekos_citation_no_trait.csv')))

    @mock.patch('org.bccvl.movelib.protocol.aekos._download_as_file')
    def test_aekos_traits_to_file_multispecies(self, mock_download_as_file=None):
        mock_download_as_file.side_effect = self._download_multispecies

        traits_source = {
            'url': 'aekos://traits?{}'.format(
                urlencode({
                    'speciesNames': ['Abutilon fraseri', 'Abutilon halophilum'],
                    'traitNames': ['height', 'lifeForm'],
                    'varNames': ['aspect', 'electricalConductivity'],
                })
            )
        }

        file_dest = {
            'url': 'file://{}'.format(self.tmpdir)
        }
        move(self.traits_source, file_dest)

        # Check for these files are created
        self.assertTrue(os.path.exists(
            os.path.join(self.tmpdir, 'aekos_dataset.json')))
        self.assertTrue(os.path.exists(os.path.join(
            self.tmpdir, 'aekos_traits_env.zip')))
        self.assertTrue(os.path.exists(os.path.join(
            self.tmpdir, 'data', 'aekos_traits_env.csv')))
        self.assertTrue(os.path.exists(os.path.join(
            self.tmpdir, 'data', 'aekos_citation.csv')))

        # Check file content
        self.assertEqual(
            set(open(os.path.join(self.tmpdir, 'data', 'aekos_traits_env.csv')).readlines()),
            set(open(resource_filename(__name__, 'data/aekos_traits_env_multispecies.csv')).readlines())
        )
        # self.assertTrue(filecmp.cmp(os.path.join(self.tmpdir, 'data', 'aekos_traits_env.csv'),
        #                             resource_filename(__name__, 'data/aekos_traits_env_multispecies.csv')))
        self.assertEqual(
            set(open(os.path.join(self.tmpdir, 'data', 'aekos_citation.csv')).readlines()),
            set(open(resource_filename(__name__, 'data/aekos_citation_multispecies.csv')).readlines())
        )
        # self.assertTrue(filecmp.cmp(os.path.join(self.tmpdir, 'data', 'aekos_citation.csv'),
        #                             resource_filename(__name__, 'data/aekos_citation_multispecies.csv')))
