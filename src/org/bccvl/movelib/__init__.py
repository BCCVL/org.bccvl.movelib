import os
import tempfile
import shutil
from urlparse import urlparse

from org.bccvl.movelib.protocol import ala
from org.bccvl.movelib.protocol import http
from org.bccvl.movelib.protocol import scp
from org.bccvl.movelib.protocol import swift


SERVICES = {
    'ala': ala,
    'http': http,
    'https': http,
    'scp': scp,
    'swift': swift
}


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

    url = urlparse(source['url'])
    if url.scheme not in SERVICES:
        raise Exception("Unknown source URL scheme '{0}'".format(source['url']))

    src_service = SERVICES[url.scheme]
    if not src_service.validate(url):
        raise Exception('Invalid source url')

    url = urlparse(dest['url'])
    if url.scheme not in SERVICES:
        raise Exception("Unknown destination URL scheme '{0}'".format(dest['url']))

    dest_service = SERVICES[url.scheme]
    # Check if upload function is supported i.e. ALA does not support upload
    if not hasattr(dest_service, 'upload'):
        raise Exception("Upload not supported for destination '{0}'".format(dest['url']))

    if not dest_service.validate(url):
        raise Exception('Invalid destination url')

    try:
        # Store all the source files for this job in a temporary local directory
        temp_dir = tempfile.mkdtemp(prefix='move_job_')
        files = src_service.download(source, temp_dir)

        for file in files:
            dest_service.upload(file, dest)

    finally:
        # Remove temporary directory
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
