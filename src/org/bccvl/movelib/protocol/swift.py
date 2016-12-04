import logging
import os
import re
import tempfile
import time
from urlparse import urlsplit

from swiftclient.service import SwiftService, SwiftUploadObject

"""
Swift Service used to interface with Nectar Object data store

Swift URL Scheme:
swift://host:port/account/container/object

http://host:port/v1/account/container/object
"""

PROTOCOLS = ('swift+http', 'swift+https')

LOG = logging.getLogger(__name__)

# TODO: add support for temp_url_key ....
#       e.g. if temp_url_key is in source/dest, use normal http transfer?


def validate(url):
    # check that container and file are specified in swift url.
    # i.e. swift://host:port/v1/account/container/path/to/file
    path_tokens = url.path.split('/', 4)
    # verify existence of version, account, container and object
    return (url.scheme in PROTOCOLS and len(path_tokens) >= 4 and len(path_tokens[3]) >= 0)


def download(source, dest=None):
    """
    Download files from a SWIFT object store
    @param source_info: Source information such as the source to download from.
    @type source_info: a dictionary
    @param local_dest_dir: The local directory to store file
    @type local_dest_dir: str
    @return: True and a list of file downloaded if successful. Otherwise False.
    """

    if not dest:
        dest = tempfile.mkstemp()

    url = urlsplit(source['url'])
    _, ver, account, container, object_name = url.path.split('/', 4)

    swift_opts = {
        'os_storage_url': '{scheme}://{netloc}/{ver}/{account}'.format(
            scheme=re.sub(r'^swift\+', '', url.scheme),
            netloc=url.netloc,
            ver=ver,
            account=account)
    }
    # SwiftService knows about environment variables
    for opt in ('os_auth_url', 'os_username', 'os_password', 'os_tenant_name', 'os_storage_url'):
        if opt in source:
            swift_opts[opt] = source[opt]
    try:
        swift = SwiftService(swift_opts)
        filelist = []

        if os.path.exists(dest) and os.path.isdir(dest):
            outfilename = os.path.join(dest, os.path.basename(object_name))
        else:
            outfilename = dest

        retries = 5  # number of retries left
        backoff = 30  # wait time between retries
        backoff_inc = 30  # increase in wait time per retry

        while retries:
            filelist = []
            retries -= 1

            try:
                for result in swift.download(container, [object_name], {'out_file': outfilename}):
                    # result dict:  success
                    #    action: 'download_object'
                    #    success: True
                    #    container: ...
                    #    object: ...
                    #    path: ....
                    #    pseudodir: ...
                    #    start_time, finish_time, headers_receipt, auth_end_time,
                    #    read_length, attempts, response_dict
                    # result dict: error
                    #    action: 'download_object'
                    #    success: False
                    #    error: ...
                    #    traceback: ...
                    #    container, object, error_timestamp, response_dict, path
                    #    psudodir, attempts
                    if not result['success']:
                        raise Exception(
                            'Download from selfelfwift {container}/{object} to {out_file} failed with {error}'.format(out_file=outfilename, **result))
                    outfile = {'url': outfilename,
                               'name': os.path.basename(object_name),
                               'content_type': result['response_dict']['headers'].get('content-type', 'application/octet-stream')}
                    filelist.append(outfile)
                # no exception we can continue
                retries = 0
            except Exception as e:
                if not retries:
                    # reraise if no retries left
                    raise
                LOG.warn("Download from Swift failed: %s - %d retries left", e, retries)
                time.sleep(backoff)
                backoff += backoff_inc
                backoff_inc += 30
        return filelist
    except Exception as e:
        LOG.error("Download from Swift failed: %s", e, exc_info=True)
        raise


def upload(source, dest):
    """
    Upload file to a remote SWIFT store
    @param source: List of local source path to upload from.
    @type source : Dicrionary
    @param dest: The destination information such as destination url to upload the file.
    @type dest: Dictionary
    @return: True if upload is successful. Otherwise False.
    """

    url = urlsplit(dest['url'])

    _, ver, account, container, object_name = url.path.split('/', 4)

    swift_opts = {
        'os_storage_url': '{scheme}://{netloc}/{ver}/{account}'.format(
            scheme=re.sub(r'^swift\+', '', url.scheme),
            netloc=url.netloc,
            ver=ver,
            account=account)
    }
    # SwiftService knows about environment variables
    for opt in ('os_auth_url', 'os_username', 'os_password', 'os_tenant_name', 'os_storage_url'):
        if opt in dest:
            swift_opts[opt] = dest[opt]
    try:
        swift = SwiftService(swift_opts)
        headers = []
        if 'content_type' in source:
            headers.append('Content-Type: {}'.format(source['content_type']))

        retries = 5  # number of retries left
        backoff = 30  # wait time between retries
        backoff_inc = 30  # increase in wait time per retry

        while retries:
            retries -= 1
            try:
                for result in swift.upload(container, [SwiftUploadObject(source['url'], object_name=object_name, options={'header': headers})]):
                    # TODO: we may get  result['action'] = 'create_container'
                    # self.assertNotIn(member, container)d result['action'] = 'upload_object';  result['path'] =
                    # source['url']
                    if not result['success']:
                        raise Exception(
                            'Upload to Swift {container}/{object_name} failed with {error}'.format(object_name=object_name, **result))
                # no exception we can continue
                retries = 0
            except Exception as e:
                if not retries:
                    # reraise if no retries left
                    raise
                LOG.warn('Upload to Swift failed: %s - %d retries left', e, retries)
                time.sleep(backoff)
                backoff += backoff_inc
                backoff_inc += 30
    except Exception as e:
        LOG.error("Upload to swift failed: %s", e, exc_info=True)
        raise
