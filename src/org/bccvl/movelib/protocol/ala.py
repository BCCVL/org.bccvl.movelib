import csv
import datetime
import io
import json
import logging
import os
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

settings = {
    "metadata_url" : "http://bie.ala.org.au/ws/species/{lsid}.json",
    "occurrence_url" : "http://biocache.ala.org.au/ws/occurrences/index/download?qa=zeroCoordinates,badlyFormedBasisOfRecord,detectedOutlier,decimalLatLongCalculationFromEastingNorthingFailed,missingBasisOfRecord,decimalLatLongCalculationFromVerbatimFailed,coordinatesCentreOfCountry,geospatialIssue,coordinatesOutOfRange,speciesOutsideExpertRange,userVerified,processingError,decimalLatLongConverionFailed,coordinatesCentreOfStateProvince,habitatMismatch&q=lsid:{lsid}&fields=decimalLongitude,decimalLatitude,coordinateUncertaintyInMeters.p,eventDate.p,year.p,month.p&reasonTypeId=4"
}

"""
ALAService used to interface with Atlas of Living Australia (ALA)
"""

LOG = logging.getLogger(__name__)


def validate(url):
    return url.scheme == 'ala' and url.query and parse_qs(url.query).get('lsid')


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
        dsfile, taxa = _ala_postprocess(csvfile, mdfile, lsid, dest)
        _normalize_occurrence(csvfile, taxa)
        return [dsfile, csvfile, mdfile]
    except Exception as e:
        LOG.error("Failed to download occurrence data with lsid '{0}': {1}".format(lsid, e))
        raise


def _download_occurrence_by_lsid(lsid, dest):
    """
    Downloads Species Occurrence data from ALA (Atlas of Living Australia) based on an LSID (Life Science Identifier)
    @param lsid: the lsid of the species to download occurrence data for
    @type lsid: str
    @param remote_destination_directory: the destination directory that the ALA files are going to end up inside of on the remote machine. Used to form the metadata .json file.
    @type remote_destination_directory: str
    @param local_dest_dir: The local directory to temporarily store the ALA files in.
    @type local_dest_dir: str
    @return True if the dataset was obtained. False otherwise
    """
    # TODO: validate dest is a dir?

    # Get occurrence data
    occurrence_url = settings['occurrence_url'].format(lsid=lsid)
    temp_file = None
    try:
        temp_file, _ = urllib.urlretrieve(occurrence_url)
        # extract data.csv file into dest
        with zipfile.ZipFile(temp_file) as z:
            z.extract('data.csv', dest)
            # rename to ala_occurrence.csv
            os.rename(os.path.join(dest, 'data.csv'),
                      os.path.join(dest, 'ala_occurrence.csv'))
    except KeyError:
        LOG.error("Cannot find file %s in downloaded zip file", 'data.csv')
        raise
    except Exception:
        LOG.error("The file %s is not a zip file", 'data.csv')
        raise
    finally:
        if temp_file:
            os.remove(temp_file)
    return os.path.join(dest, 'ala_occurrence.csv')


def _download_metadata_for_lsid(lsid, dest):
    """Download metadata for lsid from ALA
    """
    # TODO: verify dest is a dir?

    # Get occurrence metadata
    metadata_url = settings['metadata_url'].format(lsid=lsid)
    try:
        metadata_file, _ = urllib.urlretrieve(metadata_url,
                                              os.path.join(dest, 'ala_metadata.json'))
        return metadata_file
    except Exception as e:
        LOG.error("Could not download occurrence metadata from ALA for LSID %s : %s",
                  lsid, e)
        raise


def _ala_postprocess(csvfile, mdfile, lsid, dest):
    # cleanup occurrence csv file and generate dataset metadata

    # Generate dataset .json
    # 1. read mdfile and find interesting bits:
    metadata = json.load(open(mdfile))

    taxon_name = None
    common_name = None
    # TODO: is this the correct bit? (see plone dataset import )
    if metadata['taxonName']['nameComplete'] is not None:
        taxon_name = metadata['taxonName']['nameComplete']

    for record in metadata['commonNames']:
        if record['nameString'] is not None:
            common_name = record['nameString']
            break

    # 2. clean up occurrence csv file and count occurrence points
    num_occurrences = _normalize_occurrence(csvfile, taxon_name)

    # 3. generate ala_dataset.json
    imported_date = datetime.datetime.now().strftime('%d/%m/%Y')
    if common_name:
        title = "%s (%s) occurrences" % (common_name, taxon_name)
        description = "Observed occurrences for %s (%s), imported from ALA on %s" % (common_name, taxon_name, imported_date)
    else:
        title = "%s occurrences" % (taxon_name)
        description = "Observed occurrences for %s, imported from ALA on %s" % (taxon_name, imported_date)

    ala_dataset = {
        'title': title,
        'description': description,
        'num_occurrences': num_occurrences,
        'files': [
            {
                'url': csvfile,
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
            'source': 'ALA',
            'url': settings['occurrence_url'].format(lsid=lsid),
            'source_date': imported_date
        }
    }

    # Write the dataset to a file
    dataset_path = os.path.join(dest, 'ala_dataset.json')
    f = io.open(dataset_path, mode='wb')
    json.dump(ala_dataset, f, indent=2)
    f.close()
    return dataset_path, taxon_name


def _normalize_occurrence(file_path, taxon_name):
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
    @param taxon_name: The actual taxon name to use for each occurrence row. Sometimes ALA mixes these up.
    @type taxon_name: str
    """

    if not os.path.isfile(file_path):
        raise Exception("ALA occurrence file not found or does not exist")

    if os.path.getsize(file_path) == 0:
        raise Exception("ALA occurrence file downloaded is empty (zero bytes)")

    # Build the normalized CSV in memory, order needs to match whatever ala returns
    new_csv = [[SPECIES, LONGITUDE, LATITUDE, UNCERTAINTY, EVENT_DATE, YEAR, MONTH]]

    with io.open(file_path, mode='r+') as csv_file:
        csv_reader = csv.reader(csv_file)

        # skip the header
        next(csv_reader)
        for row in csv_reader:
            lon = row[1]
            lat = row[2]
            uncertainty = row[3]
            date = row[4]
            year = row[5]
            month = row[6]

            # TODO: isn't there a builtin for this?
            if not _is_number(lon) or not _is_number(lat):
                continue

            if 'true' in row[7:]:
                continue

            new_csv.append([taxon_name, lon, lat, uncertainty, date, year, month])

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
