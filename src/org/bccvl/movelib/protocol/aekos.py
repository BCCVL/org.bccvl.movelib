import csv
import datetime
import io
import json
import logging
import os
import requests
import shutil
import tempfile
import urllib
from urlparse import urlparse, parse_qs
import zipfile

SPECIES = 'species'
LONGITUDE = 'lon'
LATITUDE = 'lat'
UNCERTAINTY = 'uncertainty'
EVENT_DATE = 'date'
YEAR = 'year'
MONTH = 'month'
CITATION = 'citation'

PROTOCOLS = ('aekos',)

SETTINGS = {
    "occurrence_url" : "https://api.aekos.org.au/v1/speciesData.json?{}&row=0",
    "traitdata_url": "http://api.aekos.org.au:8099/v1/traitData.json?{}",
    "environmentdata_url": "http://api.aekos.org.au:8099/v1/environmentData.json?{}",
}

"""
ALAService used to interface with Atlas of Living Australia (ALA)
"""

LOG = logging.getLogger(__name__)


def validate(url):
    return url.scheme == 'aekos'


def download(source, dest=None):
    """
    Download files from AEKOS
    @param source_info: Source information such as the source to download from.
    @type source_info: a dictionary
    @param local_dest_dir: The local directory to store file
    @type local_dest_dir: str
    @return: True and a list of file downloaded if successful. Otherwise False.
    """

    url = urlparse(source['url'])
    service = url.netloc
    params = parse_qs(url.query)

    # TODO: cleanup temp_file, and other urlretrieve downloads (and in other protocols as well)
    # TODO: assumes that dest is None, and dest is a directory
    if dest is None:
        dest = tempfile.mkdtemp()

    try:
        if service == 'occurrence':
            # build url with params and fetch file
            occurrence_url = SETTINGS['occurrence_url'].format(urllib.urlencode(params, True))
            # create dataset and push to destination
            # TODO: still need to do things like creating zip file, citation file, bccvl dataset info, support NA's in columns
            # TODO: change to requests so that we can actually process http error codes
            temp_file, _ = urllib.urlretrieve(occurrence_url)
            csv_file = _process_occurrence_data(temp_file, dest)
            ds_file = _aekos_postprocess(csv_file['url'], dest, csv_file['count'], csv_file['scientificName'], occurrence_url)
            return csv_file
        elif service == 'traits':
            # build urls for species, traits and envvar download with params and fetch files
            import pdb; pdb.set_trace()

            trait_data = os.path.join(dest, 'trait_data.json')
            r = requests.get(SETTINGS['traitdata_url'].format(urllib.urlencode(params, True), stream=True))
            r.raise_for_status()
            if r.status_code == 200:
                with open(trait_data, 'wb') as f:
                    r.raw.decode_content = True
                    shutil.copyfileobj(r.raw, f)

            env_data = os.path.join(dest, 'env_data.json')
            r = requests.get(SETTINGS['environmentdata_url'].format(urllib.urlencode(params, True)))
            r.raise_for_status()
            if r.status_code == 200:
                with open(env_data, 'wb') as f:
                    r.raw.decode_content = True
                    shutil.copyfileobj(r.raw, f)


            # convert json to csv file for traits modelling
            # TODO: merge traits and environment data (may need to add NAs)
            # TODO: generate dataset, zip file, citation info, bccvl dataset metadata

            # create dataset and push to destination
            pass
    except Exception as e:
        LOG.error("Failed to download {0} data with params '{1}': {2}".format(service, params, e))
        raise


def _process_occurrence_data(occurrencefile, destdir):
    # Get the occurrence data
    datadir = os.path.join(destdir, 'data')
    os.mkdir(datadir)
    occurrdata = json.load(io.open(occurrencefile))
    data = occurrdata['response']

    # Extract valid occurrence records
    headers = [SPECIES, LONGITUDE, LATITUDE, UNCERTAINTY, EVENT_DATE, YEAR, MONTH, CITATION]
    count = 0
    scientificName = ''
    citationList = []
    with io.open(os.path.join(datadir, 'aekos_occurrence.csv'), mode='wb') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(headers)
        for row in data:
            # Skip record if location data is not valid.
            if not row.has_key('decimalLongitude') or not row.has_key('decimalLatitude') or \
               not _is_number(row['decimalLongitude']) or not _is_number(row['decimalLatitude']):
                continue

            # Add citation if not already included
            citation = row.get('bibliographicCitation', '').strip()
            scientificName = row.get('scientificName', '').strip()
            if citation and citation not in citationList:
                citationList.append(row['bibliographicCitation'])
            csv_writer.writerow([scientificName, row['decimalLongitude'], row['decimalLatitude'], '', row.get('eventDate', ''), row.get('year', ''), row.get('month', ''), citation])
            count += 1

    if count == 0:
        # Everything was filtered out!
        raise Exception('No valid occurrences left.')

    # Save citations as file
    with io.open(os.path.join(datadir, 'aekos_citation.txt'), mode='wb') as cit_file:
        for citation in citationList:
            cit_file.write(citation + '\n');

    # zip occurrence data file and citation file
    _zip_occurrence_data(os.path.join(destdir, 'aekos_occurrence.zip'), datadir)

    return { 'url' : os.path.join(destdir, 'aekos_occurrence.zip'),
             'name': 'aekos_occurrence.zip',
             'content_type': 'application/zip',
             'count': count,
             'scientificName': scientificName
            }


def _aekos_postprocess(csvfile, dest, csvRowCount, scientificName, url):
    # cleanup occurrence csv file and generate dataset metadata

    # Generate dataset .json

    taxon_name = scientificName
    num_occurrences = csvRowCount

    # 3. generate arkos_dataset.json
    imported_date = datetime.datetime.now().strftime('%d/%m/%Y')
    title = "%s occurrences" % (taxon_name)
    description = "Observed occurrences for %s, imported from AEKOS on %s" % (taxon_name, imported_date)

    aekos_dataset = {
        'title': title,
        'description': description,
        'num_occurrences': num_occurrences,
        'files': [
            {
                'url': csvfile,
                'dataset_type': 'occurrence',
                'size': os.path.getsize(csvfile)
            },
        ],
        'provenance': {
            'source': 'AEKOS',
            'url': url,
            'source_date': imported_date
        }
    }

    # Write the dataset to a file
    dataset_path = os.path.join(dest, 'aekos_dataset.json')
    f = io.open(dataset_path, mode='wb')
    json.dump(aekos_dataset, f, indent=2)
    f.close()
    dsfile = { 'url' : dataset_path,
               'name': 'aekos_dataset.json',
               'content_type': 'application/json'}
    return dsfile

def _zip_occurrence_data(occzipfile, data_folder_path):
    with zipfile.ZipFile(occzipfile, 'w') as zf:
        zf.write(os.path.join(data_folder_path, 'aekos_occurrence.csv'), 'data/aekos_occurrence.csv')
        zf.write(os.path.join(data_folder_path, 'aekos_citation.txt'), 'data/aekos_citation.txt')

def _is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
