import importlib
import os
import shutil
import tempfile
from urlparse import urlsplit
import warnings


SERVICES = {}

for service in ('ala', 'http', 'scp', 'swift', 'file'):
    try:
        module = importlib.import_module(
            '{0}.{1}.{2}'.format(__name__, 'protocol', service))
        for proto in module.PROTOCOLS:
            if proto in SERVICES:
                warnings.warn('Duplicate protocol handler found: {}'.format(proto))
            SERVICES[proto] = module
    except ImportError as e:
        # TODO: should we output some warning here?
        pass


def move(source, dest):
    """
    Performs a "move" of a file from a source to a destination
    @param source_info: Source information such as the source URL to move from, and other optional informations such as password.
    @type source_info: Dictionary
    @param dest_info: Destination information such as the destination URL to move to, and other optional informations such as password.
    @type dest_info: dictionary
    @return: The status of the move
    @rtype: tuplet(bool, string)
    """

    if (source is None or dest is None
        or source.get('url') is None or dest.get('url') is None):
        raise Exception('Missing source/destination url')

    surl = urlsplit(source['url'])
    if surl.scheme not in SERVICES:
        raise Exception("Unknown source URL scheme '{0}'".format(source['url']))

    src_service = SERVICES[surl.scheme]
    if not src_service.validate(surl):
        raise Exception('Invalid source url')

    # TODO: Do I need to check SERVICES if dest_url is file:// ? -> we are not using upload .... otherwise if we stream data, upload would be suitable
    durl = urlsplit(dest['url'])
    if durl.scheme not in SERVICES:
        raise Exception("Unknown destination URL scheme '{0}'".format(dest['url']))

    dest_service = SERVICES[durl.scheme]
    # Check if upload function is supported i.e. ALA does not support upload
    if not hasattr(dest_service, 'upload'):
        raise Exception("Upload not supported for destination '{0}'".format(dest['url']))

    if not dest_service.validate(durl):
        raise Exception('Invalid destination url')

    temp_dir = None
    try:
        # TODO: another option would be to let download return a stream
        #       which could be used by upload directly (no tmp storage)
        if durl.scheme == 'file':
            # Shortcut: Download file directly to local destination
            files = src_service.download(source, durl.path)
        elif surl.scheme == 'file':
            # Shortcut: Upload local file
            # remove file:// from url
            local_source = dict(source)
            local_source['url'] = surl.path
            dest_service.upload(local_source, dest)
        else:
            # Download source files to a temporary local directory before transfer files to destination
            # TODO: maybe add infos from source to temp prefix?
            temp_dir = tempfile.mkdtemp(prefix='movelib_')
            files = src_service.download(source, temp_dir)

            for file in files:
                dest_service.upload(file, dest)

    finally:
        # Remove temporary directory
        if temp_dir is not None and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
