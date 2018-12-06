"""
ALAService used to interface with Atlas of Living Australia (ALA)
"""
import codecs
import datetime
import io
import json
import logging
import os
import tempfile
import zipfile

import requests
from six.moves.urllib_parse import urlparse, parse_qs
from six.moves.urllib.request import urlretrieve

from org.bccvl.movelib.utils import zip_occurrence_data, UnicodeCSVReader, UnicodeCSVWriter

PROTOCOLS = ('ala',)

SPECIES = 'species'
LONGITUDE = 'lon'
LATITUDE = 'lat'
UNCERTAINTY = 'uncertainty'
EVENT_DATE = 'date'
YEAR = 'year'
MONTH = 'month'

# To do: Shall replace species_guid with taxon_concept_lsid.
fields = "decimalLongitude.p,decimalLatitude.p,coordinateUncertaintyInMeters.p,eventDate.p,year.p,month.p,species_guid,taxon_name"
settings = {
    "metadata_url": "http://bie.ala.org.au/ws/species/guids/bulklookup",
    "occurrence_url": "{biocache_url}?qa={filter}&q={query}&fields={fields}&email={email}&reasonTypeId=4&sourceTypeId=2002"
}


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
    log = logging.getLogger(__name__)
    url = urlparse(source['url'])
    params = parse_qs(url.query)
    qparam = params['query'][0].split(':', 1)
    lsid = None
    lsid_list = []
    if (qparam[0] == 'lsid'):
        lsid = qparam[1]

    if dest is None:
        dest = tempfile.mkdtemp()

    try:
        occurrence_url = settings['occurrence_url'].format(
            biocache_url=params['url'][0],
            filter=params['filter'][0],
            query=params['query'][0],
            fields=fields,
            email=params.get('email', [''])[0])
        csvfile = _download_occurrence(occurrence_url, dest)

        # Possible that there is no lsid for user loaded dataset
        lsid_list = csvfile['lsids'] if lsid is None else [lsid]

        if lsid_list:
            mdfile = _download_metadata_for_lsid(lsid_list, dest)
            dsfile = _ala_postprocess(csvfile['url'], mdfile['url'], occurrence_url, dest)
            return [dsfile, csvfile, mdfile]
        else:
            dsfile = _ala_postprocess(csvfile['url'], None, occurrence_url, dest)
            return [dsfile, csvfile]
    except Exception as e:
        log.error("Failed to download occurrence data with lsid '{0}': {1}".format(
            ', '.join(lsid_list), e), exc_info=True)
        raise


# Return a list of index for the specified headers
def _get_header_index(header, csv_header):
    index = {}
    for col in header:
        index[col] = csv_header.index(col) if col in csv_header else -1
    return index


def _get_species_guid_from_csv(csvfile):
    lsids = set()
    speciesColName = 'species _ guid'

    with io.open(csvfile, mode='br+') as csv_file:
        csv_reader = UnicodeCSVReader(csv_file)

        # Check if csv file header has species ID column
        csv_header = next(csv_reader)
        speciesIndex = _get_header_index([speciesColName], csv_header)[speciesColName]
        if speciesIndex >= 0:
            # get the lsid
            for row in csv_reader:
                if row[speciesIndex]:
                    lsids.add(row[speciesIndex])
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
    log = logging.getLogger(__name__)
    temp_file = None
    lsid_list = []
    try:
        temp_file, _ = urlretrieve(occurrence_url)
        # extract data.csv file into dest
        with zipfile.ZipFile(temp_file) as z:
            data_dest = os.path.join(dest, 'data')
            os.mkdir(data_dest)

            # rename to ala_occurrence.csv
            z.extract('data.csv', dest)
            os.rename(os.path.join(dest, 'data.csv'),
                      os.path.join(data_dest, 'ala_occurrence.csv'))

            # citation file is optional
            try:
                z.extract('citation.csv', dest)
                os.rename(os.path.join(dest, 'citation.csv'),
                          os.path.join(data_dest, 'ala_citation.csv'))
            except Exception:
                pass
        lsid_list = _get_species_guid_from_csv(os.path.join(data_dest, 'ala_occurrence.csv'))

        # Zip out files if available
        zip_occurrence_data(os.path.join(dest, 'ala_occurrence.zip'),
                            os.path.join(dest, 'data'),
                            ['ala_occurrence.csv', 'ala_citation.csv'])

    except KeyError:
        log.error("Cannot find file %s in downloaded zip file", 'data.csv',
                  exc_info=True)
        raise
    except Exception:
        # TODO: Not a zip file error.... does it have to raise?
        log.error("The file %s is not a zip file", 'data.csv', exc_info=True)
        raise
    finally:
        if temp_file:
            os.remove(temp_file)

    return {'url': os.path.join(dest, 'ala_occurrence.zip'),
            'name': 'ala_occurrence.zip',
            'content_type': 'application/zip',
            'lsids': lsid_list}


def _download_metadata_for_lsid(lsid_list, dest):
    """Download metadata from ALA for the list of lsids specified.
    """

    # Get occurrence metadata
    log = logging.getLogger(__name__)
    metadata_url = settings['metadata_url']
    results = []
    try:
        # bulklookup API can only take 175 lsids, so do a loop to get metadata.
        for i in range(0, len(lsid_list), 100):
            response = requests.post(metadata_url, json=lsid_list[i:min(i+100, len(lsid_list))])
            results += json.loads(response.text)['searchDTOList']
        # TODO: bulk lookp may return null/None for unknown or outdated lsid
        #       should we try to walk lsid change history here?
        metadata_file = os.path.join(dest, 'ala_metadata.json')
        with io.open(metadata_file, mode='wb') as f:
            json.dump(results, codecs.getwriter('utf-8')(f), indent=2)

    except Exception as e:
        log.error("Could not download occurrence metadata from ALA for LSID %s : %s",
                  ', '.join(lsid_list), e, exc_info=True)
        raise

    return {'url': metadata_file,
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
            if not md:
                # we were note able to get species metadata for some of our lsids...
                # may happen because lsids change over time
                # TODO: ... should we try to walk the lsid change history to find most
                #           recent lsid? (links may be missing though)
                #           see _download_metadata_for_lsid
                continue
            # TODO: is this the correct bit? (see plone dataset import )
            guid = md.get('guid')
            if guid:
                taxon_names[guid] = md.get('scientificName') or \
                    md.get('name') or \
                    md.get('nameComplete')
                common_names.append(md.get('commonNameSingle') or md.get('scientificName'))

    # 2. clean up occurrence csv file and count occurrence points
    csvfile = os.path.join(dest, 'data/ala_occurrence.csv')
    num_occurrences = _normalize_occurrence(csvfile, taxon_names)

    # Rebuild the zip archive file with updated occurrence csv file.
    os.remove(csvzipfile)
    zip_occurrence_data(csvzipfile,
                        os.path.join(os.path.dirname(csvzipfile), 'data'),
                        ['ala_occurrence.csv', 'ala_citation.csv'])

    # 3. generate ala_dataset.json
    imported_date = datetime.datetime.now().strftime('%d/%m/%Y')
    common = u', '.join(common_names)
    taxon = u', '.join(taxon_names.values())
    if common_names:
        title = u"%s (%s) occurrences" % (common, taxon)
        description = u"Observed occurrences for %s (%s), imported from ALA on %s" % (common, taxon, imported_date)
    elif taxon:
        title = u"%s occurrences" % (taxon)
        description = u"Observed occurrences for %s, imported from ALA on %s" % (taxon, imported_date)
    else:
        # This would be the case where the user dataset does not match to any species in ALA
        # TODO: Use the user supplied name
        title = u"Occurrence for user defined dataset"
        description = u"User defined occurrence dataset, imported on %s" % (imported_date)

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
    json.dump(ala_dataset, codecs.getwriter('utf-8')(f), indent=2)
    f.close()
    dsfile = {'url': dataset_path,
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

    with io.open(file_path, mode='rb') as csv_file:
        csv_reader = UnicodeCSVReader(csv_file)

        # header of csv file
        csv_header = next(csv_reader)

        # column headers in ALA csv file
        colHeaders = [u'Longitude',
                      u'Latitude',
                      u'Coordinate Uncertainty in Metres',
                      u'Event Date - parsed',
                      u'Year',
                      u'Month',
                      u'species _ guid',
                      u'Scientific Name',
                      u'Supplied coordinates are zero']
        indexes = _get_header_index(colHeaders, csv_header)
        # Skip if any of the fields requested above is missing
        if -1 in indexes.values():
            raise Exception("Missing some columns in ALA data")

        index2 = indexes[u'Supplied coordinates are zero'] # start of filter column

        # Check for trait data; any columns between "Scientific Name" and "Supplied coordinates are zero"
        new_headers = [SPECIES, LONGITUDE, LATITUDE, UNCERTAINTY, EVENT_DATE, YEAR, MONTH]
        index1 = -1
        if indexe2 > (indexes[u'Scientific Name'] + 1):
            index1 = indexes[u'Scientific Name'] + 1
            new_headers += csv_header[index1:index2]

        new_csv = [new_headers]
        for row in csv_reader:
            # Skip if one of our fileters returned true
            if 'true' in row[index2:]:
                continue

            lon = _get_value(row, indexes[u'Longitude'])
            lat = _get_value(row, indexes[u'Latitude'])
            uncertainty = _get_value(row, indexes[u'Coordinate Uncertainty in Metres'])
            date = _get_value(row, indexes[u'Event Date - parsed'])
            year = _get_value(row, indexes[u'Year'])
            month = _get_value(row, indexes[u'Month'])
            guid = _get_value(row, indexes[u'species _ guid'])
            species = _get_value(row, indexes[u'Scientific Name'])

            # Validate lat/lon
            try:
                lon = float(lon)
                lat = float(lat)
            except (ValueError, TypeError):
                # ignore rows, where lat/lon are not numbers
                continue
            # Either species ID or species name must present
            if not guid and not species:
                continue

            # Check that the coordinates are in the range
            if (lon > 180.0 or lon < -180.0 or lat > 90.0 or lat < -90.0):
                raise Exception('Dataset contains out-of-range longitude/latitude value. Please download manually and fix the issue.')

            # For species name, use taxon name 1st, then the species name supplied in the occurrence file.
            new_row = [taxon_names.get(guid, species), lon, lat, uncertainty, date, year, month]
            # Add trait values if any
            if index1 > 0:
                new_row += row[index1:index2]
            new_csv.append(new_row)

    if len(new_csv) == 1:
        # Everything was filtered out!
        raise Exception('No valid occurrences left.')

    # Overwrite the CSV file
    with io.open(file_path, mode='bw+') as csv_file:
        csv_writer = UnicodeCSVWriter(csv_file)
        csv_writer.writerows(new_csv)

    # return number of rows
    return len(new_csv) - 1

def _get_value(row, index):
    return(row[index] if index >= 0 else u'')
