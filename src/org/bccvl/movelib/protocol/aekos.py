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
    "metadata_url": "https://api.aekos.org.au/v1/speciesSummary.json?{0}",
    "occurrence_url": "https://api.aekos.org.au/v1/speciesData.json?{0}&row=0",
    "traitdata_url": "https://api.aekos.org.au/v1/traitData.json?{0}&row=0",
    "environmentdata_url": "https://api.aekos.org.au/v1/environmentData.json?{0}&row=0",
}

"""
AEKOS-service used to interface with Advanced Ecological Knowledge and Conservation System.
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

    # TODO: cleanup occur_file, and other urlretrieve downloads (and in other protocols as well)
    # TODO: assumes that dest is None, and dest is a directory
    if dest is None:
        dest = tempfile.mkdtemp()

    try:
        occur_file = None
        trait_file = None
        env_file = None
        if service == 'occurrence':
            # build url with params and fetch file
            # create dataset and push to destination
            # TODO: still need to support NA's in columns
            #import pdb; pdb.set_trace()
            occur_file = os.path.join(dest, 'occurrence_data.json')
            occurrence_url = SETTINGS['occurrence_url'].format(urllib.urlencode(params, True))
            _download_as_file(occurrence_url, occur_file)
            csv_file = _process_occurrence_data(occur_file, dest)
            md_file = _download_metadata(params, dest)
            ds_file = _aekos_postprocess(csv_file['url'], md_file['url'], dest, csv_file['count'], csv_file['scientificName'], occurrence_url)
            return [ds_file, csv_file, md_file]
        elif service == 'traits':
            # build urls for species, traits and envvar download with params and fetch files
            trait_file = os.path.join(dest, 'trait_data.json')
            trait_url = SETTINGS['traitdata_url'].format(urllib.urlencode(params, True))
            _download_as_file(trait_url, trait_file)

            env_file = os.path.join(dest, 'env_data.json')
            env_url = SETTINGS['environmentdata_url'].format(urllib.urlencode(params, True))
            _download_as_file(env_url, env_file)

            # Merge traits and environment data to a csv file for traits modelling (may need to add NAs).
            # Generate the merged dataset, zip file, citation info, bccvl dataset metadata.
            csv_file = _process_trait_env_data(trait_file, env_file, dest)

            # create dataset and push to destination
            src_urls = [trait_url, env_url]
            ds_file = _aekos_postprocess(csv_file['url'], None, dest, csv_file['count'], csv_file['scientificName'], src_urls)
            return [ds_file, csv_file]
    except Exception as e:
        LOG.error("Failed to download {0} data with params '{1}': {2}".format(service, params, e))
        raise
    finally:
        # remove temp files
        for tempfile in [occur_file, trait_file, env_file]:
            if tempfile and os.path.exists(tempfile):
                os.remove(tempfile)


def _download_as_file(dataurl, dest_file):
    r = requests.get(dataurl, stream=True)
    r.raise_for_status()
    if r.status_code == 200:
        with open(dest_file, 'wb') as f:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, f)
    else:
        raise Exception('Fail to download from Aekos: status= {}'.format(r.status_code))

def _process_trait_env_data(traitfile, envfile, destdir):
    # Return a dictionary (longitude, latitude) as key
    datadir = os.path.join(destdir, 'data')
    os.mkdir(datadir)

    # Extract the trait data and the env variable data
    traitenvRecords = {}
    citationList = []
    traitNames, scientificName = _add_trait_env_data(traitfile, 'traits', citationList, traitenvRecords)
    envNames, scientificName = _add_trait_env_data(envfile, 'variables', citationList, traitenvRecords)

    if not traitNames or not envNames:
        raise Exception("No traits or environment variables are found")

    # Save data as csv file
    headers = traitNames + envNames
    count = _save_as_csv(citationList, traitenvRecords, headers, datadir)

    if count == 0:
        raise Exception("No trait/environment data is found")

    # zip traits/env data file and citation file
    zipfilename = 'aekos_traits_env.zip'
    _zip_data_dir(os.path.join(destdir, zipfilename), datadir, ['aekos_traits_env.csv', 'aekos_citation.txt'])

    return { 'url' : os.path.join(destdir, zipfilename),
             'name': zipfilename,
             'content_type': 'application/zip',
             'count': count,
             'scientificName': scientificName
            }


def _save_as_csv(citationList, trait_env_data, headers, datadir):
    # Save citations as file
    with io.open(os.path.join(datadir, 'aekos_citation.txt'), mode='wb') as cit_file:
        for citation in citationList:
            cit_file.write(citation + '\n');

    # save data as csv file
    count = 0
    with io.open(os.path.join(datadir, 'aekos_traits_env.csv'), mode='wb') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([LONGITUDE, LATITUDE, EVENT_DATE] + headers)
        for key, item in trait_env_data.iteritems():
            row = [key[0], key[1], key[2]] + ([''] * len(headers))
            for record in item['traits'] + item['variables']:
                try:    
                    index = headers.index(record['name']) + 3
                    row[index] = record['value']
                except ValueError as e:
                    LOG.info('Skip {} ...'.format(record['name']))
                    continue
                csv_writer.writerow(row)
                count += 1
    return count


def _add_trait_env_data(resultfile, fieldname, citationList, trait_env_data):
    scientificName = ''
    results = json.load(io.open(resultfile))
    resphdrfield = 'traitNames' if fieldname == 'traits' else 'envVarNames'
    nameList = results['responseHeader'].get('params', {}).get(resphdrfield, [])
    for row in results['response']:
        # Skip record if location data is not valid.
        if not row.has_key('decimalLongitude') or not row.has_key('decimalLatitude') or \
           not _is_number(row['decimalLongitude']) or not _is_number(row['decimalLatitude']):
            continue

        # Save the data with date, as it can have multiple records collected at different dates
        valueList = row.get(fieldname, [])      # shall be either traits or variables
        if valueList: 
            location = (row['decimalLongitude'], row['decimalLatitude'], row.get('eventDate', ''))
            trait_env_data.setdefault(location, {'traits': [], 'variables': []})[fieldname] = valueList
            _addName(valueList, nameList)

            # Add citation if not already included
            citation = row.get('bibliographicCitation', '').strip()
            if citation and citation not in citationList:
                citationList.append(row['bibliographicCitation'])
            scientificName = row.get('scientificName', '')
    return nameList, scientificName


def _addName(recordList, nameList):
    # Add the name of the record if it is not included in the name list yet.
    for record in recordList:
        name = record.get('name', None)
        if name and name not in nameList:
            nameList.append(name)

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
    zipfilename = 'aekos_occurrence.zip'
    _zip_data_dir(os.path.join(destdir, zipfilename), datadir, ['aekos_occurrence.csv', 'aekos_citation.txt'])

    return { 'url' : os.path.join(destdir, zipfilename),
             'name': zipfilename,
             'content_type': 'application/zip',
             'count': count,
             'scientificName': scientificName
            }

def _download_metadata(params, dest):
    """Download metadata for species from AEKOS
    """
    # Get species metadata
    md_file = os.path.join(dest, 'aekos_metadata.json')
    metadata_url = SETTINGS['metadata_url'].format(urllib.urlencode(params, True))
    try:
        _download_as_file(metadata_url, md_file)
    except Exception as e:
        LOG.error("Could not download occurrence metadata from AEKOS for %s : %s", params, e)
        raise

    return { 'url' : md_file,
             'name': 'aekos_metadata.json',
             'content_type': 'application/json'}

def _aekos_postprocess(csvfile, mdfile, dest, csvRowCount, scientificName, source_url):
    # cleanup occurrence csv file and generate dataset metadata
    # Generate dataset .json

    taxon_name = scientificName
    if mdfile:
        md = json.load(open(mdfile, 'r'))
        taxon_name = md[0]['scientificName'] or scientificName

    num_occurrences = csvRowCount

    # 3. generate arkos_dataset.json
    imported_date = datetime.datetime.now().strftime('%d/%m/%Y')
    title = "%s occurrences" % (taxon_name)
    description = "Observed occurrences for %s, imported from AEKOS on %s" % (taxon_name, imported_date)

    # Construct file items
    filelist = [
                {
                    'url': csvfile,
                    'dataset_type': 'occurrence',
                    'size': os.path.getsize(csvfile)
                }
            ]

    if mdfile:
        filelist.append({
                            'url': mdfile,
                            'dataset_type': 'attribution',
                            'size': os.path.getsize(mdfile)
                        })

    aekos_dataset = {
        'title': title,
        'description': description,
        'num_occurrences': num_occurrences,
        'files': filelist,
        'provenance': {
            'source': 'AEKOS',
            'url': source_url,
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

def _zip_data_dir(occzipfile, data_folder_path, filelist):
    with zipfile.ZipFile(occzipfile, 'w') as zf:
        for filename in filelist:
            zf.write(os.path.join(data_folder_path, filename), 'data/{}'.format(filename))

def _is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
