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

PROTOCOLS = ('aekos',)

SETTINGS = {
    "occurrence_url" : "http://api.aekos.org.au:8099/v1/speciesData.json?{}",
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

    import pdb; pdb.set_trace()

    try:
        if service == 'occurrence':
            # build url with params and fetch file
            occurrence_url = SETTINGS['occurrence_url'].format(urllib.urlencode(params, True))
            # create dataset and push to destination
            # TODO: still need to do things like creating zip file, citation file, bccvl dataset info, support NA's in columns
            # TODO: change to requests so that we can actually process http error codes
            temp_file, _ = urllib.urlretrieve(occurrence_url)
            import pdb; pdb.set_trace()
            csv_file = _process_occurrence_data(temp_file, dest)
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

    occurrdata = json.load(io.open(occurrencefile))
    csvfile = os.path.join(destdir, 'aekos_occurrence.csv')
    # TODO: assumes that all objects have the same attributes which will be used as column names
    # get headers from first object
    headers = occurrdata[0].keys()
    # rename latCoord, longCoord
    headers.remove('latCoord')
    headers.remove('longCoord')
    headers.insert(0, 'lon')
    headers.insert(0, 'lat')
    with io.open(csvfile, mode='wb') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=headers)
        csv_writer.writeheader()
        for item in occurrdata:
            item['lat'] = item.pop('latCoord')
            item['lon'] = item.pop('longCoord')
            csv_writer.writerow(item)
    return csvfile
