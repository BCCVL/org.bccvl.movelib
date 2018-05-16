"""
ObisService used to interface with Ocean Biogeographic Information System (OBIS)
"""
import codecs
import datetime
import io
import json
import logging
import os
import tempfile
import zipfile
import shutil

from six.moves.urllib_parse import urlparse, parse_qs
from six.moves.urllib.request import urlretrieve, urlopen

from org.bccvl.movelib.utils import UnicodeCSVWriter


PROTOCOLS = ('obis',)

SPECIES = 'species'
LONGITUDE = 'lon'
LATITUDE = 'lat'
UNCERTAINTY = 'uncertainty'
EVENT_DATE = 'date'
YEAR = 'year'
MONTH = 'month'

# for OBIS, obisid is the speciesKey
settings = {
    "metadata_url": "https://api.iobis.org/taxon/{obisid}",
    "occurrence_url": "https://api.iobis.org/occurrence?&obisid={obisid}&offset={offset}&limit={limit}",
    "dataset_url": "https://api.iobis.org/resource?obisid={obisid}&offset={offset}&limit={limit}"
}


def validate(url):
    return url.scheme == 'obis' and url.query and parse_qs(url.query).get('lsid')


def download(source, dest=None):
    """
    Download files from a remote SWIFT source
    @param source_info: Source information such as the source to download from.
    @type source_info: a dictionary
    @param local_dest_dir: The local directory to store file
    @type local_dest_dir: str
    @return: True and a list of file downloaded if successful. Otherwise False.
    """
    log = logging.getLogger(__name__)
    url = urlparse(source['url'])
    obisid = parse_qs(url.query)['lsid'][0]

    if dest is None:
        dest = tempfile.mkdtemp()

    try:
        csvfile = _download_occurrence_by_obisid(obisid, dest)
        mdfile = _download_metadata_for_obisid(obisid, dest)
        dsfile = _obis_postprocess(csvfile['url'], mdfile['url'],
                                   obisid, dest, csvfile['count'])
        return [dsfile, csvfile, mdfile]
    except Exception as e:
        log.error(
            "Failed to download occurrence data with obisid '{0}': {1}".format(obisid, e), exc_info=True)
        raise


def _zip_occurrence_data(occzipfile, data_folder_path):
    with zipfile.ZipFile(occzipfile, 'w') as zf:
        zf.write(os.path.join(data_folder_path, 'obis_occurrence.csv'),
                 'data/obis_occurrence.csv')
        zf.write(os.path.join(data_folder_path, 'obis_citation.txt'),
                 'data/obis_citation.txt')


def _download_occurrence_by_obisid(obisid, dest):
    """
    Downloads Species Occurrence data from OBIS based on an obis ID  (i.e. species taxonKey)
    @param obisid: the obisid of the species to download occurrence data for
    @type obisid: str
    @param remote_destination_directory: the destination directory that the OBIS files are going to end up inside of on the remote machine. Used to form the metadata .json file.
    @type remote_destination_directory: str
    @param local_dest_dir: The local directory to temporarily store the OBIS files in.
    @type local_dest_dir: str
    @return True if the dataset was obtained. False otherwise
    """
    # TODO: validate dest is a dir?

    # Get occurrence data
    log = logging.getLogger(__name__)
    offset = 0
    limit = 400
    count = 20
    lastpage = False
    data = [[SPECIES, LONGITUDE, LATITUDE, UNCERTAINTY, EVENT_DATE, YEAR, MONTH]]
    keys = ['species', 'decimalLongitude',
            'decimalLatitude', 'eventDate', 'year', 'month']
    data_dest = os.path.join(dest, 'data')

    try:
        while offset < count or not lastpage:
            occurrence_url = settings['occurrence_url'].format(
                obisid=obisid, offset=offset, limit=limit)
            f = urlopen(occurrence_url)
            t1 = json.load(f)
            count = t1['count']
            offset += t1['limit']
            lastpage = t1.get('lastpage', False)
            for row in t1['results']:
                # TODO: isn't there a builtin for this?
                if 'decimalLongitude' not in row or 'decimalLatitude' not in row or \
                   not _is_number(row['decimalLongitude']) or not _is_number(row['decimalLatitude']):
                    continue

                # Check that the coordinates are in the range
                if (row['decimalLongitude'] > 180.0 or row['decimalLongitude'] < -180.0 or \
                   row['decimalLatitude'] > 90.0 or row['decimalLatitude'] < -90.0):
                    raise Exception('Dataset contains out-of-range longitude/latitude value. Please download manually and fix the issue.')

                data.append([row['scientificName'], row['decimalLongitude'], row['decimalLatitude'], '',
                             row.get('eventDate', ''), row.get('yearcollected', ''), row.get('month', '')])
            f.close()
            f = None

        rowCount = len(data)
        if rowCount == 1:
            # Everything was filtered out!
            raise Exception('No valid occurrences left.')

        # Write data as a CSV file
        os.mkdir(data_dest)
        with io.open(os.path.join(data_dest, 'obis_occurrence.csv'), mode='wb') as csv_file:
            csv_writer = UnicodeCSVWriter(csv_file)
            csv_writer.writerows(data)

        # Get citation for each dataset from the dataset details
        _get_dataset_citation(obisid,
                              os.path.join(data_dest, 'obis_citation.txt'))
        _zip_occurrence_data(os.path.join(dest, 'obis_occurrence.zip'),
                             data_dest)

    except Exception as e:
        log.error("Fail to download occurrence records from OBIS, %s", e, exc_info=True)
        raise
    finally:
        if os.path.exists(data_dest):
            shutil.rmtree(data_dest)

    return {'url': os.path.join(dest, 'obis_occurrence.zip'),
            'name': 'obis_occurrence.zip',
            'content_type': 'application/zip',
            'count': rowCount - 1}


def _get_dataset_citation(obisid, destfilepath):
    """Download dataset details to extract the citation record for each dataset.
    """
    log = logging.getLogger(__name__)
    offset = 0
    limit = 400
    count = 20
    lastpage = False

    try:
        # save as utf-8 file
        with codecs.open(destfilepath, 'w', 'utf-8') as citfile:
            # download citation records
            while offset < count or not lastpage:    
                dataset_url = settings['dataset_url'].format(obisid=obisid, offset=offset, limit=limit)
                f = urlopen(dataset_url)
                data = json.load(f)
                count = data['count']
                offset += data['limit']
                lastpage = data.get('lastpage', False)
                for row in data['results']:
                    citation = row.get('citation', None)
                    if citation:
                        citfile.write(citation.replace('\n', ' ') + '\n')
                f.close()
                f = None
    except Exception as e:
        log.error("Fail to download dataset citations from OBIS: %s", e, exc_info=True)
        raise
    finally:
        f = None


def _download_metadata_for_obisid(obisid, dest):
    """Download metadata for obisid from OBIS
    """
    # Get occurrence metadata
    log = logging.getLogger(__name__)
    metadata_url = settings['metadata_url'].format(obisid=obisid)
    try:
        metadata_file, _ = urlretrieve(metadata_url,
                                       os.path.join(dest, 'obis_metadata.json'))
    except Exception as e:
        log.error("Could not download occurrence metadata from OBIS for obisid %s : %s",
                  obisid, e, exc_info=True)
        raise

    return {'url': metadata_file,
            'name': 'obis_metadata.json',
            'content_type': 'application/json'}


def _obis_postprocess(csvfile, mdfile, obisid, dest, num_occurrences):
    # Generate dataset metadata. csvfile is a zip file of occurrence csv file
    # and citation file.

    # Generate dataset .json
    # 1. read mdfile and find interesting bits:
    metadata = json.load(open(mdfile))

    taxon_name = metadata.get('tname', None)
    common_name = metadata.get('tname', None)

    # 3. generate obis_dataset.json
    imported_date = datetime.datetime.now().strftime('%d/%m/%Y')
    if common_name:
        title = "%s (%s) occurrences" % (common_name, taxon_name)
        description = "Observed occurrences for %s (%s), imported from OBIS on %s" % (
            common_name, taxon_name, imported_date)
    else:
        title = "%s occurrences" % (taxon_name)
        description = "Observed occurrences for %s, imported from OBIS on %s" % (
            taxon_name, imported_date)

    obis_dataset = {
        'title': title,
        'description': description,
        'num_occurrences': num_occurrences,
        'files': [
            {
                'url': csvfile,                 # This is a zip file
                'dataset_type': 'occurrence',
                'size': os.path.getsize(csvfile)
            },
            {
                'url': mdfile,
                'dataset_type': 'attribution',
                'size': os.path.getsize(mdfile)
            }

        ],
        'provenance': {
            'source': 'OBIS',
            'url': settings['occurrence_url'].format(obisid=obisid, offset=0, limit=400),
            'source_date': imported_date
        }
    }

    # Write the dataset to a file
    dataset_path = os.path.join(dest, 'obis_dataset.json')
    f = io.open(dataset_path, mode='wb')
    json.dump(obis_dataset, codecs.getwriter('utf-8')(f), indent=2)
    f.close()
    dsfile = {'url': dataset_path,
              'name': 'obis_dataset.json',
              'content_type': 'application/json'}
    return dsfile


def _is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
