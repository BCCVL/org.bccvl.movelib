import os
import logging
import tempfile
from urlparse import urlparse
from swiftclient.service import SwiftService, SwiftUploadObject

"""
Swift Service used to interface with Nectar Object data store

Swift URL Scheme:
swift://host:port/account/container/object

http://host:port/v1/account/container/object
"""

LOG = logging.getLogger(__name__)


def validate(url):
    # check that container and file are specified in swift url.
    # i.e. swift://nectar/my-container/path/to/file
    path_tokens = url.path.split('/', 2)
    return (url.scheme == 'swift' and len(path_tokens) >= 3
            and len(path_tokens[1]) > 0 and len(path_tokens[2]) > 0)


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
        dest = tempfile.mkstepm()

    url = urlparse(source['url'])
    path_tokens = url.path.split('/', 2)
    # TODO: do we need account name? or do we always use default account?
    #account = path_tokens[0] -> need to translate this into storage_url
    #                            parameter if we ever need it
    container = path_tokens[1]
    src_path = path_tokens[2]

    swift_opts = {}
    # SwiftService knows about environment variables
    for opt in ('auth', 'user', 'key', 'os_tenant_name', 'auth_version'):
        if opt in source:
            swift_opts[opt] = source[opt]
    try:
        swift = SwiftService(swift_opts)
        filelist = []
        outfilename = os.path.join(dest, 'tmp_move_file')
        for result in swift.download(container, [src_path], {'out_file': outfilename}):
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
                raise Exception('Download from swift {container}/{object} to {out_file} failed with {error}'.format(out_file=outfilename, **result))
            filelist.append(outfilename)
        return filelist
    except Exception as e:
        LOG.error("Download from Swift failed: %s", e)
        raise


def upload(source, dest):
    """
    Upload file to a remote SWIFT store
    @param local_src_list: List of local source path to upload from.
    @type local_src_list: str
    @param dest_info: The destination information such as destination url to upload the file.
    @type dest_path: Dictionary
    @return: True if upload is successful. Otherwise False.
    """

    url = urlparse(dest['url'])
    path_tokens = url.path.split('/', 2)
    # TODO: do we need account name? or do we always use default account?
    #account = path_tokens[0] -> need to translate this into storage_url
    #                            parameter if we ever need it
    # TODO: check if we have a dst_path at all?
    container = path_tokens[1]
    dest_path = path_tokens[2]

    swift_opts = {}
    # SwiftService knows about environment variables
    for opt in ('auth', 'user', 'key', 'os_tenant_name', 'auth_version'):
        if opt in dest:
            swift_opts[opt] = dest[opt]
    try:
        swift = SwiftService(swift_opts)
        for result in swift.upload(container, [SwiftUploadObject(source, object_name=dest_path)]):
            if not result['success']:
                raise Exception('Upload to Swift {container}/{dest_file} failed with {error}'.format(dest_file=dest_path, **result))
    except Exception as e:
	LOG.error("Upload to swift failed: %s", e)
        raise
