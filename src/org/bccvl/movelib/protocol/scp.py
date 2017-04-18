from __future__ import absolute_import
import logging
import os
import pwd
import tempfile
from six.moves.urllib_parse import urlsplit, urlunsplit

from paramiko import SSHClient, AutoAddPolicy
from scp import SCPClient, SCPException

PROTOCOLS = ('scp',)

LOG = logging.getLogger(__name__)


def validate(url):
    return (url.scheme == 'scp' and url.hostname.strip() != ''
            and url.path.strip() != '')


def download(source, dest=None):
    """
    Download files from a remote SCP source
    @param source_info: Source information such as the source to download from.
    @type source_info: a dictionary
    @param local_dest_dir: The local filename
    @type local_dest_dir: str
    @return: True and a list of file downloaded if successful. Otherwise False.
    """
    try:
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(AutoAddPolicy())

        url = urlsplit(source['url'])
        username = url.username
        # Use the current user if one was not specified
        if not username:
            username = pwd.getpwuid(os.getuid())[0]

        ssh.connect(url.hostname, port=url.port or 22, username=username, password=url.password)

        scp = SCPClient(ssh.get_transport())

        # Download file to a local temporary file if a local file path is not specified.
        if not dest:
            dest = tempfile.mkdtemp()

        if os.path.exists(dest) and os.path.isdir(dest):
            # TODO: we should somehow support scp://dir to scp://dir
            # get filename from source
            filename = os.path.basename(url.path)
            if not filename:
                fd, dest = tempfile.mkstemp(dir=dest)
            else:
                dest = os.path.join(dest, filename)

        scp.get(url.path, dest, recursive=False)
        ssh.close()

        outputfile = {'url': dest,
                      'name': os.path.basename(url.path),
                      'content_type': 'application/octet-stream'
                      }
        return [outputfile]
    except SCPException:
        LOG.error("Could not SCP file %s from %s to local destination\
                  %s as user %s", url.path, url.hostname, dest, username, exc_info=True)
        raise


def upload(source, dest):
    """
    Upload files to a remote SWIFT store
    @param source: local source file
    @type local_src_list: str
    @param dest_info: The destination information such as destination url to upload the file.
    @type dest_path: Dictionary
    @return: True if upload is successful. Otherwise False.
    """

    url = urlsplit(dest['url'])

    try:
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(AutoAddPolicy())

        # Use the current user if one was not specified
        username = url.username
        if not username:
            username = pwd.getpwuid(os.getuid())[0]

        ssh.connect(url.hostname, port=url.port or 22, username=username, password=url.password)

        if 'filename' in dest:
            url = urlsplit(urlunsplit(
                (url.scheme,
                 url.netloc,
                 os.path.join(url.path, dest['filename']),
                 url.query,
                 url.fragment
                 )
            ))
        scp = SCPClient(ssh.get_transport())
        scp.put(source['url'], url.path, recursive=True)  # recursive should be an option in dest dict?
        ssh.close()
    except Exception as e:
        LOG.error("Could not SCP file %s to destination %s on %s as user %s: %s",
                  source['url'], url.path, url.hostname, username, e, exc_info=True)
        raise
