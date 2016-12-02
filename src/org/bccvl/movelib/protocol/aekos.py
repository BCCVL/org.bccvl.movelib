import csv
import io
import json
import logging
import os
import requests
import shutil
import tempfile
import zipfile
import itertools
import urllib
from urlparse import urlparse, parse_qs
from datetime import datetime


SPECIES = 'species'
LONGITUDE = 'lon'
LATITUDE = 'lat'
UNCERTAINTY = 'uncertainty'
EVENT_DATE = 'date'
YEAR = 'year'
MONTH = 'month'
CITATION = 'citation'
METADATA = 'metadata'
LOCATION_ID = 'locationID'

PROTOCOLS = ('aekos',)

SETTINGS = {
    "metadata_url": "https://api.aekos.org.au/v1/speciesSummary.json?{0}",
    "occurrence_url": "https://api.aekos.org.au/v1/speciesData.json?{0}&rows=0",
    "traitdata_url": "https://api.aekos.org.au/v1/traitData.json?{0}&rows=0",
    "environmentdata_url": "https://api.aekos.org.au/v1/environmentData.json?{0}&rows=0",
}

"""
AEKOS-service used to interface with Advanced Ecological Knowledge
and Conservation System.

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

    # TODO: cleanup occur_file, and other urlretrieve downloads (and
    #       in other protocols as well)
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
            occur_file = os.path.join(dest, 'occurrence_data.json')
            occurrence_url = SETTINGS['occurrence_url'].format(
                urllib.urlencode(params, True))
            _download_as_file(occurrence_url, occur_file)
            csv_file = _process_occurrence_data(occur_file, dest)
            md_file = _download_metadata(params, dest)
            ds_file = _aekos_postprocess(csv_file['url'], md_file['url'], dest,
                                         csv_file['count'],
                                         csv_file['scientificName'],
                                         'occurrence', occurrence_url)
            return [ds_file, csv_file, md_file]
        elif service == 'traits':
            # build urls for species, traits and envvar download with params
            # and fetch files
            src_urls =[]
            trait_file = None
            if params.get('traitName', None) and params.get('traitName')[0] != 'None':
                trait_file = os.path.join(dest, 'trait_data.json')
                trait_url = SETTINGS['traitdata_url'].format(
                    urllib.urlencode(params, True))
                _download_as_file(trait_url, trait_file)
                src_urls.append(trait_url)

            env_file = None
            if params.get('envVarName', None) and params.get('envVarName')[0] != 'None':
                env_file = os.path.join(dest, 'env_data.json')
                env_url = SETTINGS['environmentdata_url'].format(
                    urllib.urlencode(params, True))
                _download_as_file(env_url, env_file)
                src_urls.append(env_url)

            # Merge traits and environment data to a csv file for
            # traits modelling (may need to add NAs).
            # Generate the merged dataset, zip file, citation info, bccvl
            # dataset metadata.
            csv_file = _process_trait_env_data(trait_file, env_file, dest)

            # create dataset and push to destination
            ds_file = _aekos_postprocess(csv_file['url'], None, dest,
                                         csv_file['count'],
                                         csv_file['speciesName'],
                                         'traits', src_urls)
            return [ds_file, csv_file]
    except Exception as e:
        LOG.error("Failed to download {0} data with params '{1}': {2}".format(
            service, params, e), exc_info=True)
        raise
    finally:
        # remove temp files
        for tmpfile in [occur_file, trait_file, env_file]:
            if tmpfile and os.path.exists(tmpfile):
                os.remove(tmpfile)

def _download_as_file(dataurl, dest_file):
    r = requests.get(dataurl, stream=True)
    r.raise_for_status()
    if r.status_code == 200:
        with open(dest_file, 'wb') as f:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, f)
    else:
        raise Exception(
            'Fail to download from Aekos: status= {}'.format(r.status_code))


def _process_trait_env_data(traitfile, envfile, destdir):
    # Return a dictionary (longitude, latitude) as key
    datadir = os.path.join(destdir, 'data')
    os.mkdir(datadir)

    # Extract the trait data and the env variable data.
    # Possible that no trait data or env variable data.
    traitenvRecords = {}
    traitNames, speciesNames1 = _add_trait_env_data(
        traitfile, 'traits', traitenvRecords)
    envNames, speciesNames2 = _add_trait_env_data(
        envfile, 'variables', traitenvRecords)

    if not traitNames and not envNames:
        raise Exception("No traits and environment variables are found")

    # Save data as csv file
    headers = traitNames + envNames
    count = _save_as_csv(traitenvRecords, headers, datadir)

    if count == 0:
        raise Exception("No trait/environment data is found")

    # zip traits/env data file and citation file
    zipfilename = 'aekos_traits_env.zip'
    _zip_data_dir(os.path.join(destdir, zipfilename), datadir, [
                  'aekos_traits_env.csv', 'aekos_citation.csv'])

    return {'url': os.path.join(destdir, zipfilename),
            'name': zipfilename,
            'content_type': 'application/zip',
            'count': count,
            'speciesName':  ','.join(speciesNames1 or speciesNames2)
            }


def _save_as_csv(trait_env_data, headers, datadir):
    # Save citations and metadata as csv file
    colhders = [LONGITUDE, LATITUDE, SPECIES, LOCATION_ID] 
    otrhders = ['trait_date', 'trait_citation', 'trait_metadata', \
                'env_date', 'env_citation', 'env_metadata']
    with io.open(os.path.join(datadir, 'aekos_citation.csv'),
                 mode='wb') as cit_file:
        cit_writer = csv.writer(cit_file)
        cit_writer.writerow(colhders + otrhders)

        for item in trait_env_data.itervalues():
            for traits, envvars in _product(item['traits'], item['variables']):
                # Add the event date, citation and metadata for trait/env data
                row = [item.get(i, '') for i in colhders] + \
                      [traits.get('metadata', {}).get(col, '') 
                          for col in [EVENT_DATE, CITATION, METADATA]] + \
                      [envvars.get('metadata', {}).get(col, '') 
                          for col in [EVENT_DATE, CITATION, METADATA]]
                cit_writer.writerow(row)

    # save trait/env data as csv file
    count = 0
    colhders = [LONGITUDE, LATITUDE, EVENT_DATE, SPECIES, LOCATION_ID, MONTH, YEAR]
    with io.open(os.path.join(datadir, 'aekos_traits_env.csv'),
                 mode='wb') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(colhders + headers)
        for key, item in trait_env_data.iteritems():
            for traits, envvars in _product(item['traits'], item['variables']):
                row = [item.get(i, '') for i in colhders] + \
                    ([''] * len(headers))

                # Add in the list of traits/env variables.
                for record in (traits.get('value', []) + envvars.get('value', [])):
                    try:
                        index = headers.index(record['name']) + len(colhders)
                        row[index] = record['value']
                    except ValueError as e:
                        LOG.info('Skip {} ...'.format(record['name']))
                        continue
                csv_writer.writerow(row)
                count += 1
    return count


def _product(traitcol, envcol):
    # return all possible combinations of traits and env variables.
    if not traitcol:
        return itertools.product([{}], envcol)
    if not envcol:
        return itertools.product(traitcol, [{}])
    return itertools.product(traitcol, envcol)


def _add_trait_env_data(resultfile, fieldname, trait_env_data):
    nameList = []

    # return if there is no result file
    if not resultfile:
        return nameList, []

    results = json.load(io.open(resultfile))
    resphdrfield = 'traitNames' if fieldname == 'traits' else 'envVarNames'
    speciesList = results['responseHeader'].get(
        'params', {}).get('speciesNames', [])
    nameList = results['responseHeader'].get(
        'params', {}).get(resphdrfield, [])
    for row in results['response']:
        # Skip record if location data is not valid.
        if 'decimalLongitude' not in row or 'decimalLatitude' not in row:
            continue

        # Save the data with date, as it can have multiple records
        # collected at different dates.
        # Potentially can have multiple records of traits/env variables for a
        # given date, with different values.
        # shall be either traits or variables
        valueList = row.get(fieldname, [])
 
        if valueList:
            data = {
                'value': valueList,
                'metadata': {
                    CITATION : row.get('bibliographicCitation', ''),
                    METADATA : row.get('samplingProtocol', ''),
                    EVENT_DATE : row.get('eventDate', '')
                }
            }
 
            # For trait, merged by location ID, eventdate and species name. 
            # For env data, add to records where trait data is within x 
            # (i.e. 30) days from collection, and same location.
            found = False
            if fieldname != 'traits':
                for item in trait_env_data.itervalues():
                    if item[LOCATION_ID] == row.get('locationID') and \
                       _days(item.get(EVENT_DATE), row.get('eventDate')) <= 30:
                        item[fieldname].append(data)
                        found = True

            if not found or fieldname == 'traits':
                location = (row.get('locationID'), row.get('eventDate'), row.get('scientificName', ''))
                trait_env_data.setdefault(location, {
                    LONGITUDE : row.get('decimalLongitude'),
                    LATITUDE : row.get('decimalLatitude'),
                    LOCATION_ID : row.get('locationID', ''),
                    EVENT_DATE : row.get('eventDate', ''),
                    MONTH : row.get('month'),
                    YEAR : row.get('year'),
                    SPECIES : row.get('scientificName'),
                    'traits': [],
                    'variables': []})[fieldname].append(data)

            _addName(valueList, nameList)

    return nameList, speciesList

def _days(date1, date2):
    return abs((datetime.strptime(date1, '%Y-%m-%d') - datetime.strptime(date2, '%Y-%m-%d')).days)

def _addName(recordList, nameList):
    # Add the name of the record if it is not included in the name list yet.
    for record in recordList:
        name = record.get('name', '').strip()
        if name and name not in nameList:
            nameList.append(name)

def _process_occurrence_data(occurrencefile, destdir):
    # Get the occurrence data
    datadir = os.path.join(destdir, 'data')
    os.mkdir(datadir)
    occurrdata = json.load(io.open(occurrencefile))
    data = occurrdata['response']

    # Extract valid occurrence records
    headers = [SPECIES, LONGITUDE, LATITUDE,
               UNCERTAINTY, EVENT_DATE, YEAR, MONTH, CITATION]
    count = 0
    scientificName = ''
    citationList = []
    with io.open(os.path.join(datadir, 'aekos_occurrence.csv'),
                 mode='wb') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(headers)
        for row in data:
            # Skip record if location data is not valid.
            if 'decimalLongitude' not in row or 'decimalLatitude' not in row:
                continue

            # Add citation if not already included
            citation = row.get('bibliographicCitation', '').strip()
            scientificName = row.get('scientificName', '').strip()
            if citation and citation not in citationList:
                citationList.append(row['bibliographicCitation'])
            csv_writer.writerow([scientificName, row['decimalLongitude'],
                                 row['decimalLatitude'], '',
                                 row.get('eventDate', ''), row.get('year', ''),
                                 row.get('month', ''), citation])
            count += 1

    if count == 0:
        # Everything was filtered out!
        raise Exception('No valid occurrences left.')

    # Save citations as file
    with io.open(os.path.join(datadir, 'aekos_citation.txt'),
                 mode='wb') as cit_file:
        for citation in citationList:
            cit_file.write(citation + '\n')

    # zip occurrence data file and citation file
    zipfilename = 'aekos_occurrence.zip'
    _zip_data_dir(os.path.join(destdir, zipfilename), datadir, [
                  'aekos_occurrence.csv', 'aekos_citation.txt'])

    return {'url': os.path.join(destdir, zipfilename),
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
    metadata_url = SETTINGS['metadata_url'].format(
        urllib.urlencode(params, True))
    try:
        _download_as_file(metadata_url, md_file)
    except Exception as e:
        LOG.error(
            "Could not download occurrence metadata from AEKOS for %s : %s",
            params, e, exc_info=True)
        raise

    return {'url': md_file,
            'name': 'aekos_metadata.json',
            'content_type': 'application/json'}


def _aekos_postprocess(csvfile, mdfile, dest, csvRowCount,
                       scientificName, dsType, source_url):
    # cleanup occurrence csv file and generate dataset metadata
    # Generate dataset .json

    taxon_name = scientificName
    if mdfile:
        md = json.load(open(mdfile, 'r'))
        taxon_name = md[0]['scientificName'] or scientificName

    # Generate aekos_dataset.json
    imported_date = datetime.now().strftime('%d/%m/%Y')
    if dsType == 'occurrence':
        title = "%s occurrences" % (taxon_name)
        description = "Observed occurrences for %s, imported from AEKOS on %s" % (
            taxon_name, imported_date)
    else:
        title = "%s trait and environment variable data" % (taxon_name)
        description = "Observed trait and environment varaible data for %s, imported from AEKOS on %s" % (
            taxon_name, imported_date)


    # Construct file items
    filelist = [
        {
            'url': csvfile,
            'dataset_type': dsType,
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
        'num_occurrences': csvRowCount,
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
    dsfile = {'url': dataset_path,
              'name': 'aekos_dataset.json',
              'content_type': 'application/json'}
    return dsfile


def _zip_data_dir(occzipfile, data_folder_path, filelist):
    with zipfile.ZipFile(occzipfile, 'w') as zf:
        for filename in filelist:
            zf.write(os.path.join(data_folder_path, filename),
                     'data/{}'.format(filename))
