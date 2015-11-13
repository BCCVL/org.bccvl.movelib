import os
import logging
import urllib
from urlparse import urlparse

LOG = logging.getLogger(__name__)

# TODO: check headers return of urlretrieve
# TODO: consider using urllib2 or requests to support additional features


def validate(url):
    return url.scheme in ['http', 'https']


def download(source, dest=None):
    """
    Download files from a remote HTTP source to local disc
    @param source_info: Source information such as the source to download from.
    @type source_info: dict
    @param dest: The local filename. If None a temp file will be generated.
    @type dest: str
    @return: True and a list of files downloaded if successful. Otherwise False.
    """

    try:
        filename = os.path.basename(source['url'])
        dest_path = os.path.join(dest, filename)
        temp_file, headers = urllib.urlretrieve(source['url'], dest_path)
	htmlfile = {'url' : dest_path,
                    'name': filename,
                    'content_type': headers.get('Content-Type') 
		   }
        return [htmlfile]
    except Exception as e:
        LOG.error("Could not download file: %s: %s", source['url'], e)
        raise
