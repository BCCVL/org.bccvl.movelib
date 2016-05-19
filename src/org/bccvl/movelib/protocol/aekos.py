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

PROTOCOLS = ('aekos',)

SETTINGS = {
    "occurrence_url" : "http://api.aekos.org.au:8099/v1/speciesData.csv?{}",
    "traitdata_url": "http://api.aekos.org.au:8099/v1/traitData.json?{}",
    "speciesdata_url": "http://api.aekos.org.au:8099/v1/speciesData.json?{}",
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

    if dest is None:
        dest = tempfile.mkdtemp()

    try:
        if service == 'occurrence':
            # build url with params and fetch file
            occurrence_url = SETTINGS['occurrence_url'].format(params)
            # create dataset and push to destination
            temp_file, _ = urllib.urlretrieve(occurrence_url)
            import pdb; pdb.set_trace()

        elif service == 'traits':
            # build urls for species, traits and envvar download with params and fetch files
            trait_data, _ = urllib.urlretrieve(SETTINGS['traitdata_url'].format(params))
            species_data, _ = urllib.urlretrieve(SETTINGS['speciesdata_url'].format(params))
            env_data, _ = urllib.urlretrieve(SETTINGS['environmentdata_url'].format(params))
            # convert json to csv file for traits modelling

            import pdb; pdb.set_trace()

            # create dataset and push to destination
            pass
    except Exception as e:
        LOG.error("Failed to download {0} data with params '{1}': {2}".format(service, params, e))
        raise
