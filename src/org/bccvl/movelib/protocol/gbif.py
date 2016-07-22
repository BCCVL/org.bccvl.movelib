import csv
import datetime
import io
import json
import logging
import os
import tempfile
import zipfile
import urllib
import shutil
from urlparse import urlparse, parse_qs


PROTOCOLS = ('gbif',)

SPECIES = 'species'
LONGITUDE = 'lon'
LATITUDE = 'lat'
UNCERTAINTY = 'uncertainty'
EVENT_DATE = 'date'
YEAR = 'year'
MONTH = 'month'

# for GBIF, lsid is the speciesKey
settings = {
    "metadata_url" : "http://api.gbif.org/v1/species/{lsid}",
    "occurrence_url" : "http://api.gbif.org/v1/occurrence/search?taxonKey={lsid}&offset={offset}&limit={limit}",
    "dataset_url": "http://api.gbif.org/v1/dataset/{datasetkey}"
}

"""
GBIFService used to interface with Global Biodiversity Information Facility (GBIF)
"""

LOG = logging.getLogger(__name__)


def validate(url):
    return url.scheme == 'gbif' and url.query and parse_qs(url.query).get('lsid')


def download(source, dest=None):
    """
    Download files from a remote SWIFT source
    @param source_info: Source information such as the source to download from.
    @type source_info: a dictionary
    @param local_dest_dir: The local directory to store file
    @type local_dest_dir: str
    @return: True and a list of file downloaded if successful. Otherwise False.
    """

    url = urlparse(source['url'])
    lsid = parse_qs(url.query)['lsid'][0]

    if dest is None:
        dest = tempfile.mkdtemp()

    try:
        csvfile = _download_occurrence_by_lsid(lsid, dest)
        mdfile = _download_metadata_for_lsid(lsid, dest)
        dsfile = _gbif_postprocess(csvfile['url'], mdfile['url'], lsid, dest, csvfile['count'])
        return [dsfile, csvfile, mdfile]
    except Exception as e:
        LOG.error("Failed to download occurrence data with lsid '{0}': {1}".format(lsid, e))
        raise

def _zip_occurrence_data(occzipfile, data_folder_path):
    with zipfile.ZipFile(occzipfile, 'w') as zf:
        zf.write(os.path.join(data_folder_path, 'gbif_occurrence.csv'), 'data/gbif_occurrence.csv')
        zf.write(os.path.join(data_folder_path, 'gbif_citation.txt'), 'data/gbif_citation.txt')

def _download_occurrence_by_lsid(lsid, dest):
    """
    Downloads Species Occurrence data from GBIF (Global Biodiversity Information Facility) based on an LSID (i.e. species taxonKey)
    @param lsid: the lsid of the species to download occurrence data for
    @type lsid: str
    @param remote_destination_directory: the destination directory that the GBIF files are going to end up inside of on the remote machine. Used to form the metadata .json file.
    @type remote_destination_directory: str
    @param local_dest_dir: The local directory to temporarily store the GBIF files in.
    @type local_dest_dir: str
    @return True if the dataset was obtained. False otherwise
    """
    # TODO: validate dest is a dir?

    # Get occurrence data
    temp_file = None
    offset = 0
    limit = 300
    count = 20
    data = [[SPECIES, LONGITUDE, LATITUDE, UNCERTAINTY, EVENT_DATE, YEAR, MONTH]]
    keys = ['species', 'decimalLongitude', 'decimalLatitude', 'eventDate', 'year', 'month']
    datasetkeys = []
    data_dest = os.path.join(dest, 'data')  

    try:
        while offset < count:
            occurrence_url = settings['occurrence_url'].format(lsid=lsid, offset=offset, limit=limit)
            temp_file, _ = urllib.urlretrieve(occurrence_url)
            with open(temp_file) as f:
                t1 = json.load(f)
                count = t1['count']
                offset += t1['limit']
                for row in t1['results']:
                    # TODO: isn't there a builtin for this? 
                    if not row.has_key('decimalLongitude') or not row.has_key('decimalLatitude') or \
                       not _is_number(row['decimalLongitude']) or not _is_number(row['decimalLatitude']):
                        continue
                    # save the dataset key
                    if row['datasetKey'] not in datasetkeys:
                        datasetkeys.append(row['datasetKey'])
                    data.append([row['species'], row['decimalLongitude'], row['decimalLatitude'], '', row.get('eventDate', ''), row.get('year', ''), row.get('month', '')]) 
            os.remove(temp_file)

        rowCount = len(data)
        if rowCount == 1:
            # Everything was filtered out!
            raise Exception('No valid occurrences left.')

        # Write data as a CSV file
        os.mkdir(data_dest)
        with io.open(os.path.join(data_dest, 'gbif_occurrence.csv'), mode='wb') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerows(data)

        # Get citation for each dataset from the dataset details
        _get_dataset_citation(datasetkeys, os.path.join(data_dest, 'gbif_citation.txt'))
        _zip_occurrence_data(os.path.join(dest, 'gbif_occurrence.zip'), data_dest) 

    except Exception as e:
        LOG.error("Fail to download occurrence records from GBIF, %s", e)
        raise
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)
        if os.path.exists(data_dest):
            shutil.rmtree(data_dest)

    return { 'url' : os.path.join(dest, 'gbif_occurrence.zip'),
             'name': 'gbif_occurrence.zip',
             'content_type': 'application/zip',
             'count': rowCount - 1 }

def _get_dataset_citation(dskeylist, destfilepath):
    """Download dataset details to extract the citation record for each dataset.
    """
    try:
        with open(destfilepath, 'w') as citfile:
            for key in dskeylist:
                dataset_url = settings['dataset_url'].format(datasetkey=key)
                f = urllib.urlopen(dataset_url)
                data = json.load(f)
                citation = data.get('citation', {}).get('text')
                if citation:
                    citfile.write(citation + '\n')
                f.close()
                f = None 
    except Exception as e:
        LOG.error("Fail to download dataset citations from GBIF: %s", e)
        raise
    finally:
        f = None

def _download_metadata_for_lsid(lsid, dest):
    """Download metadata for lsid from GBIF
    """
    # TODO: verify dest is a dir?

    # Get occurrence metadata
    metadata_url = settings['metadata_url'].format(lsid=lsid)
    try:
        metadata_file, _ = urllib.urlretrieve(metadata_url,
                                              os.path.join(dest, 'gbif_metadata.json'))
    except Exception as e:
        LOG.error("Could not download occurrence metadata from GBIF for LSID %s : %s",
                  lsid, e)
        raise

    return { 'url' : metadata_file,
             'name': 'gbif_metadata.json',
             'content_type': 'application/json'}


def _gbif_postprocess(csvfile, mdfile, lsid, dest, csvRowCount):
    # Generate dataset metadata. csvfile is a zip file of occurrence csv file and citation file.

    # Generate dataset .json
    # 1. read mdfile and find interesting bits:
    metadata = json.load(open(mdfile))

    taxon_name = metadata.get('scientificName', None)
    common_name = metadata.get('vernacularName', None)
    num_occurrences = csvRowCount

    # 3. generate gbif_dataset.json
    imported_date = datetime.datetime.now().strftime('%d/%m/%Y')
    if common_name:
        title = "%s (%s) occurrences" % (common_name, taxon_name)
        description = "Observed occurrences for %s (%s), imported from GBIF on %s" % (common_name, taxon_name, imported_date)
    else:
        title = "%s occurrences" % (taxon_name)
        description = "Observed occurrences for %s, imported from GBIF on %s" % (taxon_name, imported_date)

    gbif_dataset = {
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
            'source': 'GBIF',
            'url': settings['occurrence_url'].format(lsid=lsid, offset=0, limit=300),
            'source_date': imported_date
        }
    }

    # Write the dataset to a file
    dataset_path = os.path.join(dest, 'gbif_dataset.json')
    f = io.open(dataset_path, mode='wb')
    json.dump(gbif_dataset, f, indent=2)
    f.close()
    dsfile = { 'url' : dataset_path,
               'name': 'gbif_dataset.json',
               'content_type': 'application/json'}
    return dsfile



def _is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
