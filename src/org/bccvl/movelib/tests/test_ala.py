import os.path
import pkg_resources
import shutil
import tempfile
import zipfile
import unittest
from six.moves.urllib_parse import urlparse, parse_qs

import mock

from org.bccvl.movelib import move
from org.bccvl.movelib.utils import UnicodeCSVReader


class ALATest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        if self.tmpdir and os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)

    def _urlretrieve(self, url, dest=None):
        # 1. occurrence_url
        params = parse_qs(url)
        if url.startswith('https://biocache-ws.ala.org.au/ws/occurrences/index/download'):
            query = params['q']
            temp_file = os.path.join(self.tmpdir, 'ala_data.zip')
            shutil.copy(pkg_resources.resource_filename(__name__, 'data/ala_data.zip'),
                            temp_file)

            return (temp_file, None)  # fd is ignored
        # 2. metadata_url, destpath
        if url.startswith('https://bie-ws.ala.org.au/ws/species'):
            shutil.copy(pkg_resources.resource_filename(__name__, 'data/ala_metadata.json'),
                        dest)
            return (dest, None)

    def _urlretrieve_trait(self, url, dest=None):
        # 1. occurrence_url
        params = parse_qs(url)
        if url.startswith('https://biocache-ws.ala.org.au/ws/occurrences/index/download'):
            query = params['q']
            temp_file = os.path.join(self.tmpdir, 'ala_trait_data.zip')
            shutil.copy(pkg_resources.resource_filename(__name__, 'data/ala_trait_data.zip'),
                            temp_file)

            return (temp_file, None)  # fd is ignored
        # 2. metadata_url, destpath
        if url.startswith('https://bie-ws.ala.org.au/ws/species'):
            shutil.copy(pkg_resources.resource_filename(__name__, 'data/ala_metadata.json'),
                        dest)
            return (dest, None)

    @mock.patch('org.bccvl.movelib.protocol.ala.urlretrieve')
    def test_ala_to_file(self, mock_urlretrieve=None):
        mock_urlretrieve.side_effect = self._urlretrieve
        # mock urllib.urlretrieve ....
        #        return zip file with data.csv and citation.csv
        # mock urllib.urlretriev ...
        #        return ala_metadata.json
        occurrence_url = "https://biocache-ws.ala.org.au/ws/occurrences/index/download"
        query = "lsid:urn:lsid:biodiversity.org.au:afd.taxon:0e03431f-775a-4873-ac88-e003f15b359b"
        qfilter = "zeroCoordinates,badlyFormedBasisOfRecord,detectedOutlier,decimalLatLongCalculationFromEastingNorthingFailed,missingBasisOfRecord,decimalLatLongCalculationFromVerbatimFailed,coordinatesCentreOfCountry,geospatialIssue,coordinatesOutOfRange,speciesOutsideExpertRange,userVerified,processingError,decimalLatLongConverionFailed,coordinatesCentreOfStateProvince,habitatMismatch"
        email = "testuser@gmail.com"
        src_url = 'ala://ala?url={}&query={}&filter={}&email={}'.format(occurrence_url, query, qfilter, email)

        file_dest = {
            'url': 'file://{}'.format(self.tmpdir)
        }
        move({'url': src_url}, file_dest)

        # verify call scp.put
        # verify ala calls?
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'ala_dataset.json')))
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'ala_occurrence.zip')))

    @mock.patch('org.bccvl.movelib.protocol.ala.urlretrieve')
    def test_ala_qid_to_file(self, mock_urlretrieve=None):
        # FIXME: we do live metadata lookup here
        mock_urlretrieve.side_effect = self._urlretrieve
        # mock urllib.urlretrieve ....
        #        return zip file with data.csv and citation.csv
        # mock urllib.urlretriev ...
        #        return ala_metadata.json
        occurrence_url = "https://biocache-ws.ala.org.au/ws/occurrences/index/download"
        query = "qid:urn:lsid:biodiversity.org.au:afd.taxon:31a9b8b8-4e8f-4343-a15f-2ed24e0bf1ae"
        qfilter = "zeroCoordinates,badlyFormedBasisOfRecord,detectedOutlier,decimalLatLongCalculationFromEastingNorthingFailed,missingBasisOfRecord,decimalLatLongCalculationFromVerbatimFailed,coordinatesCentreOfCountry,geospatialIssue,coordinatesOutOfRange,speciesOutsideExpertRange,userVerified,processingError,decimalLatLongConverionFailed,coordinatesCentreOfStateProvince,habitatMismatch"
        email = "testuser@gmail.com"
        src_url = 'ala://ala?url={}&query={}&filter={}&email={}'.format(occurrence_url, query, qfilter, email)

        file_dest = {
            'url': 'file://{}'.format(self.tmpdir)
        }
        move({'url': src_url}, file_dest)

        # verify ala calls?
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'ala_dataset.json')))
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'ala_occurrence.zip')))

    @mock.patch('org.bccvl.movelib.protocol.ala.urlretrieve')
    def test_ala_trait_qid_to_file(self, mock_urlretrieve=None):
        # FIXME: we do live metadata lookup here
        mock_urlretrieve.side_effect = self._urlretrieve_trait
        # mock urllib.urlretrieve ....
        #        return zip file with data.csv and citation.csv
        # mock urllib.urlretriev ...
        #        return ala_metadata.json
        occurrence_url = "https://biocache-ws.ala.org.au/ws/occurrences/index/download"
        query = "qid:urn:lsid:biodiversity.org.au:afd.taxon:31a9b8b8-4e8f-4343-a15f-2ed24e0bf1ae"
        qfilter = "zeroCoordinates,badlyFormedBasisOfRecord,detectedOutlier,decimalLatLongCalculationFromEastingNorthingFailed,missingBasisOfRecord,decimalLatLongCalculationFromVerbatimFailed,coordinatesCentreOfCountry,geospatialIssue,coordinatesOutOfRange,speciesOutsideExpertRange,userVerified,processingError,decimalLatLongConverionFailed,coordinatesCentreOfStateProvince,habitatMismatch"
        email = "testuser@gmail.com"
        src_url = 'ala://ala?url={}&query={}&filter={}&email={}'.format(occurrence_url, query, qfilter, email)

        file_dest = {
            'url': 'file://{}'.format(self.tmpdir)
        }
        move({'url': src_url}, file_dest)

        # verify ala calls?
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'ala_dataset.json')))
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'ala_occurrence.zip')))

        # Verify that it contains trait data
        headers = []
        with zipfile.ZipFile(os.path.join(self.tmpdir, 'ala_occurrence.zip')) as z:
            z.extract('data/ala_occurrence.csv', self.tmpdir)
            csv_file = open(os.path.join(self.tmpdir, 'data/ala_occurrence.csv'))
            csv_reader = UnicodeCSVReader(csv_file)
            headers = next(csv_reader)
        self.assertTrue(headers[-2:] == ['trait1', 'trait2'])
