import os
import logging
import requests

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

    try:
        filename = os.path.basename(source['url'])
        dest_path = os.path.join(dest, filename)

        # Download from the source URL using cookies and then write content to file
        response = requests.get(source['url'], cookies = source.get('cookies', {}), timeout = 7200.0)
        if response.reason != 'OK':
            raise Exception('reson: {0}'.format(response.reason))
    
        open(dest_path, 'w').write(response.content);
        htmlfile = {'url' : dest_path,
                    'name': filename,
                    'content_type': response.headers.get('Content-Type')
                   }
        return [htmlfile]
    except Exception as e:
        LOG.error("Could not download file: %s: %s", source['url'], e)
        raise
    finally:
        if response:
            response.close()