from contextlib import closing
import logging
import os
import requests
from urlparse import urlparse


PROTOCOLS = ('http', 'https')

LOG = logging.getLogger(__name__)


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
    response = None
    try:
        srcurl = urlparse(source['url'])
        if os.path.exists(dest) and os.path.isdir(dest):
            filename = os.path.basename(srcurl.path)
            dest_path = os.path.join(dest, filename)
        else:
            filename = os.path.basename(dest)
            dest_path = dest

        # Download from the source URL using cookies and then write content to file
        cookie = source.get('cookies', {})
        verify = source.get('verify', None)

        s = requests.Session()
        s.cookies.set(**cookie)

        response = s.get(source['url'], stream=True, verify=verify)
        # raise exception case of error
        response.raise_for_status()

        # TODO: could check response.headers['content-length'] to decide streaming or not
        with open(dest_path, 'w') as f:
            for chunk in response.iter_content(chunk_size=4096):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)

        htmlfile = {'url' : dest_path,
                    'name': filename,
                    'content_type': response.headers.get('Content-Type')
                   }
        return [htmlfile]
    except Exception as e:
        LOG.error("Could not download file: %s: %s", source['url'], e)
        raise
    finally:
        # We need to close response in case we did not consume all data
        if response:
            response.close()
