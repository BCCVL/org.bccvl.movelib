import logging
import os
import pwd
import shutil
from urlparse import urlparse

LOG = logging.getLogger(__name__)

def validate(url):
    return url.scheme == 'file' and url.path.strip() != ''

def download(source, dest=None):
    """
    Download files from a local source to local disc
    @param source_info: Source information such as the source to download from.
    @type source_info: dict
    @param dest: The local filename. If None a temp file will be generated.
    @type dest: str
    @return: True and a list of files downloaded if successful. Otherwise False.
    """

    try:
    	srcurl = urlparse(source['url'])
        if os.path.exists(dest) and os.path.isdir(dest):
            filename = os.path.basename(srcurl.path)
            dest_path = os.path.join(dest, filename)
        else:
            filename = os.path.basename(dest)
            dest_path = dest

        shutil.copy(srcurl.path, dest_path)
        localfile = {'url' : dest_path,
                    'name': filename,
                    'content_type': 'application/octet-stream'
               }
        return [localfile]
    except Exception as e:
        LOG.error("Could not download file: %s: %s", source['url'], e)
        raise

def upload(source, dest):
    """
    Upload local files to a local store
    @param source: local source file
    @type local_src_list: str
    @param dest_info: The destination information such as destination url to upload the file.
    @type dest_path: Dictionary
    @return: True if upload is successful. Otherwise False.
    """

    try:
    	url = urlparse(dest['url'])
        dest_filename = dest.get('filename', source['name'])
        dest_path = os.path.join(url.path, dest_filename)
        shutil.copy(source['url'], dest_path)
    except:
        LOG.error("Could not copy file %s to destination %s", source['url'], dest_path)
        raise
