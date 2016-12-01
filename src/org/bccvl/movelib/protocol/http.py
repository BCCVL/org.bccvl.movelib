import logging
import os
import requests
import tempfile
from urlparse import urlsplit


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
        srcurl = urlsplit(source['url'])

        # Download from the source URL using cookies and then write content to file
        cookie = source.get('cookies', {})
        verify = source.get('verify', None)

        s = requests.Session()
        if cookie:
            s.cookies.set(**cookie)

        response = s.get(source['url'], stream=True, verify=verify)
        # raise exception case of error
        response.raise_for_status()

        # set destination filename
        if os.path.exists(dest) and os.path.isdir(dest):
            if response.headers.get('content-type', '') == 'application/zip':
                fd, dest_path = tempfile.mkstemp(suffix='.zip', dir=dest)
            else:
                fd, dest_path = tempfile.mkstemp(dir=dest)
            filename = os.path.basename(dest_path)
        else:
            filename = os.path.basename(dest)
            dest_path = dest

        # TODO: could check response.headers['content-length'] to decide streaming or not
        with open(dest_path, 'w') as f:
            for chunk in response.iter_content(chunk_size=4096):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)

        # TODO: check content-disposition header for filename?
        htmlfile = {
            'url': dest_path,
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
