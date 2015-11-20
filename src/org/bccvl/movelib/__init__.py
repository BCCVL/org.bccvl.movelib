import importlib
import os
import shutil
import tempfile
from urlparse import urlparse
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

    surl = urlparse(source['url'])
    if surl.scheme not in SERVICES:
        raise Exception("Unknown source URL scheme '{0}'".format(source['url']))

    src_service = SERVICES[surl.scheme]
    if not src_service.validate(surl):
        raise Exception('Invalid source url')

    durl = urlparse(dest['url'])
    if durl.scheme not in SERVICES:
        raise Exception("Unknown destination URL scheme '{0}'".format(dest['url']))

    dest_service = SERVICES[durl.scheme]
    # Check if upload function is supported i.e. ALA does not support upload
    if not hasattr(dest_service, 'upload'):
        raise Exception("Upload not supported for destination '{0}'".format(dest['url']))

    if not dest_service.validate(durl):
        raise Exception('Invalid destination url')

    try:
        temp_dir = None
        if durl.scheme == 'file':
            # Download file directly to local destination
            dest_filename = dest.get('filename', os.path.basename(surl.path))
            destpath = os.path.join(durl.path, dest_filename)
            files = src_service.download(source, destpath)
        else:
            # Download source files to a temporary local directory before transfer files to destination
            temp_dir = tempfile.mkdtemp(prefix='move_job_')
            files = src_service.download(source, temp_dir)

            for file in files:
                dest_service.upload(file, dest)

    finally:
        # Remove temporary directory
        if temp_dir is not None and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
