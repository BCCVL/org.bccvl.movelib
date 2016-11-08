import csv
import datetime
import io
import json
import logging
import os
import tempfile
import urllib
import requests
from urlparse import urlparse, parse_qs
import zipfile
from org.bccvl.movelib.utils import zip_occurrence_data

PROTOCOLS = ('ala',)

SPECIES = 'species'
LONGITUDE = 'lon'
LATITUDE = 'lat'
UNCERTAINTY = 'uncertainty'
EVENT_DATE = 'date'
YEAR = 'year'
MONTH = 'month'

settings = {
    "metadata_url" : "http://bie.ala.org.au/ws/species/guids/bulklookup",
    "occurrence_url" : "{biocache_url}?qa={filter}&q={query}&fields=decimalLongitude,decimalLatitude,coordinateUncertaintyInMeters.p,eventDate.p,year.p,month.p,speciesID.p&reasonTypeId=4"
    }

"""
ALAService used to interface with Atlas of Living Australia (ALA)
"""

LOG = logging.getLogger(__name__)


def validate(url):
    return url.scheme == 'ala' and url.query


def download(source, dest=None):
    """
    Download files from a remote SWIFT source
    @param source_info: Source information such as the source to download from.
    @type source_info: a dictionary
    @param local_dest_dir: The local directory to store file
    @type local_dest_dir: str
    @return: True and a list of file downloaded if successful. Otherwise False.
    """

    # Parameters for the query
    url = urlparse(source['url'])
    params = parse_qs(url.query)
    qparam = params['query'][0].split(':', 1)
    lsid = None
    if (qparam[0] == 'lsid'):
        lsid = qparam[1]

    if dest is None:
        dest = tempfile.mkdtemp()

    try:
        occurrence_url = settings['occurrence_url'].format(
                                    biocache_url=params['url'][0], 
                                    filter=params['filter'][0], 
                                    query=params['query'][0])        
        csvfile = _download_occurrence(occurrence_url, dest)

        #TODO: Need to handle user loaded dataset where there is no lsid
        if lsid is None and not csvfile['lsids']:
            raise Exception("No lsid for species in query with {}".format(params['query'][0]))

        lsid_list = csvfile['lsids'] if lsid is None else [lsid]

        if lsid_list:
            mdfile = _download_metadata_for_lsid(lsid_list, dest)
            dsfile = _ala_postprocess(csvfile['url'], mdfile['url'], occurrence_url, dest)
            return [dsfile, csvfile, mdfile]
        else: 
            dsfile = _ala_postprocess(csvfile['url'], None, occurrence_url, dest)
            return [dsfile, csvfile]            
    except Exception as e:
        LOG.error("Failed to download occurrence data with lsid '{0}': {1}".format(', '.join(lsid_list), e))
        raise

def _get_species_guid_from_csv(csvfile):
    lsids = set()
    with io.open(csvfile, mode='br+') as csv_file:
        csv_reader = csv.reader(csv_file)

        # skip the header
        next(csv_reader)

        # get the 7th column as the lsid
        for row in csv_reader:
            if row[6]:
                lsids.add(row[6])
    return list(lsids)

def _download_occurrence(occurrence_url, dest):
    """
    Downloads Species Occurrence data from ALA (Atlas of Living Australia)
    @param download_url: the url to download species occurrence data
    @type download_url: str
    @param dest: the destination directory that the ALA files are going to end up inside of on the remote machine. Used to form the metadata .json file.
    @type dest: str
    @return True if the dataset was obtained. False otherwise
    """
    # TODO: validate dest is a dir?

    # Get occurrence data
    temp_file = None
    lsid_list = []
    try:
        temp_file, _ = urllib.urlretrieve(occurrence_url)
        # extract data.csv file into dest
        with zipfile.ZipFile(temp_file) as z:
            z.extract('data.csv', dest)
            # rename to ala_occurrence.csv
            data_dest = os.path.join(dest, 'data')
            os.mkdir(data_dest)
            os.rename(os.path.join(dest, 'data.csv'),
                      os.path.join(data_dest, 'ala_occurrence.csv'))
            z.extract('citation.csv', dest)
            os.rename(os.path.join(dest, 'citation.csv'),
                      os.path.join(data_dest, 'ala_citation.csv'))
        lsid_list = _get_species_guid_from_csv(os.path.join(data_dest, 'ala_occurrence.csv'))

        # Zip it out
        zip_occurrence_data(os.path.join(dest, 'ala_occurrence.zip'), 
                            os.path.join(dest, 'data'),
                            'ala_occurrence.csv',
                            'ala_citation.csv')

    except KeyError:
        LOG.error("Cannot find file %s in downloaded zip file", 'data.csv')
        raise
    except Exception:
        # TODO: Not a zip file error.... does it have to raise?
        LOG.error("The file %s is not a zip file", 'data.csv')
        raise
    finally:
        if temp_file:
            os.remove(temp_file)

    return { 'url' : os.path.join(dest, 'ala_occurrence.zip'),
             'name': 'ala_occurrence.zip',
             'content_type': 'application/zip',
             'lsids': lsid_list }

def _download_metadata_for_lsid(lsid_list, dest):
    """Download metadata from ALA for the list of lsids specified.
    """

    # Get occurrence metadata
    metadata_url = settings['metadata_url']
    try:
        response = requests.post(metadata_url, json=lsid_list)
        metadata_file = os.path.join(dest, 'ala_metadata.json')
        results = json.loads(response.text)['searchDTOList']

        with io.open(metadata_file, mode='wb') as f:
            json.dump(results, f, indent=2)

    except Exception as e:
        LOG.error("Could not download occurrence metadata from ALA for LSID %s : %s",
                  ', '.join(lsid_list), e)
        raise

    return { 'url' : metadata_file,
             'name': 'ala_metadata.json',
             'content_type': 'application/json'}


def _ala_postprocess(csvzipfile, mdfile, occurrence_url, dest):
    # cleanup occurrence csv file and generate dataset metadata
    # occurrence dataset can be multiple species, i.e. user upload data
    taxon_names = {}
    common_names = []

    if mdfile:
        # Generate dataset .json
        # 1. read mdfile and find interesting bits:
        sp_metadata = json.load(open(mdfile))

        for md in sp_metadata:
            # TODO: is this the correct bit? (see plone dataset import )
            guid = md.get('guid')
            if guid:                
                taxon_names[guid] = md.get('scientificName') or \
                                    md.get('name') or \
                                    md.get('nameComplete')
                common_names.append(md['commonNameSingle'])

    # 2. clean up occurrence csv file and count occurrence points
    csvfile = os.path.join(dest, 'data/ala_occurrence.csv')
    num_occurrences = _normalize_occurrence(csvfile, taxon_names)

    # Rebuild the zip archive file with updated occurrence csv file.
    os.remove(csvzipfile)
    zip_occurrence_data(csvzipfile, 
                        os.path.join(os.path.dirname(csvzipfile), 'data'),
                        'ala_occurrence.csv',
                        'ala_citation.csv')
    
    # 3. generate ala_dataset.json
    imported_date = datetime.datetime.now().strftime('%d/%m/%Y')
    common = ', '.join(common_names)
    taxon = ', '.join(taxon_names.values())
    if common_names:
        title = "%s (%s) occurrences" % (common, taxon)
        description = "Observed occurrences for %s (%s), imported from ALA on %s" % (common, taxon, imported_date)
    elif taxon:
        title = "%s occurrences" % (taxon)
        description = "Observed occurrences for %s, imported from ALA on %s" % (taxon, imported_date)
    else:
        # This would be the case where the user dataset does not match to any species in ALA
        # TODO: Use the user supplied name
        title = "Occurrence for user defined dataset"
        description = "User defined occurrence dataset, imported on %s" % (imported_date)

    files = [{
                'url': csvzipfile,
                'dataset_type': 'occurrence',
                'size': os.path.getsize(csvzipfile)
             }]
    if mdfile:
        files.append({
                        'url': mdfile,
                        'dataset_type': 'attribution',
                        'size': os.path.getsize(mdfile)
                     })

    ala_dataset = {
        'title': title,
        'description': description,
        'num_occurrences': num_occurrences,
        'files': files,
        'provenance': {
            'source': 'ALA',
            'url': occurrence_url,
            'source_date': imported_date
        }
    }

    # Write the dataset to a file
    dataset_path = os.path.join(dest, 'ala_dataset.json')
    f = io.open(dataset_path, mode='wb')
    json.dump(ala_dataset, f, indent=2)
    f.close()
    dsfile = { 'url' : dataset_path,
               'name': 'ala_dataset.json',
               'content_type': 'application/json'}
    return dsfile


def _normalize_occurrence(file_path, taxon_names):
    """
    Normalizes an occurrence CSV file by replacing the first line of content from:
    Scientific Name,Longitude - original,Latitude - original,Coordinate Uncertainty in Metres - parsed,Event Date - parsed,Year - parsed,Month - parsed
    to:
    species,lon,lat,uncertainty,date,year,month
    Also ensures the first column contains the same taxon name for each row.
    Sometimes ALA sends occurrences with empty lon/lat values. These are removed.
    Also filters any occurrences which are tagged as erroneous by ALA.
    @param file_path: the path to the occurrence CSV file to normalize
    @type file_path: str
    @param taxon_names: The actual list of taxon names to use for each occurrence row. Sometimes ALA mixes these up.
    @type taxon_name: str
    """

    if not os.path.isfile(file_path):
        raise Exception("ALA occurrence file not found or does not exist")

    if os.path.getsize(file_path) == 0:
        raise Exception("ALA occurrence file downloaded is empty (zero bytes)")

    # Build the normalized CSV in memory, order needs to match whatever ala returns
    new_csv = [[SPECIES, LONGITUDE, LATITUDE, UNCERTAINTY, EVENT_DATE, YEAR, MONTH]]

    with io.open(file_path, mode='br+') as csv_file:
        csv_reader = csv.reader(csv_file)

        # skip the header
        next(csv_reader)
        for row in csv_reader:
            lon = row[0]
            lat = row[1]
            uncertainty = row[2]
            date = row[3]
            year = row[4]
            month = row[5]
            guid = row[6]

            # TODO: isn't there a builtin for this?
            if not _is_number(lon) or not _is_number(lat):
                continue
            # one of our filters returned true (shouldn't happen?)
            if 'true' in row[7:]:
                continue

            new_csv.append([taxon_names.get(guid, ''), lon, lat, uncertainty, date, year, month])

    if len(new_csv) == 1:
        # Everything was filtered out!
        raise Exception('No valid occurrences left.')

    # Overwrite the CSV file
    with io.open(file_path, mode='wb') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(new_csv)

    # return number of rows
    return len(new_csv) - 1


def _is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
