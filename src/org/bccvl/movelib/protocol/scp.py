import logging
import os
import pwd
import tempfile
from urlparse import urlparse

from paramiko import SSHClient, AutoAddPolicy
from scp import SCPClient, SCPException

LOG = logging.getLogger(__name__)


def validate_url(source):
    url = urlparse(source['url'])
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

        url = urlparse(source['url'])
        username = url.username
        # Use the current user if one was not specified
        if not username:
            username = pwd.getpwuid(os.getuid())[0]

        ssh.connect(url.hostname, username=username, password=url.password)

        scp = SCPClient(ssh.get_transport())
        if not dest:
            dest = tempfile.mkstemp()
        scp.get(url.path, dest, recursive=True)
        ssh.close()
        return [dest]
    except SCPException:
        LOG.error("Could not SCP file %s from %s to local destination\
                  %s as user %s", url.path, url.hostname, dest, username)
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

    url = urlparse(dest['url'])

    try:
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(AutoAddPolicy())

        # Use the current user if one was not specified
        username = url.username
        if not username:
            username = pwd.getpwuid(os.getuid())[0]

        ssh.connect(url.hostname, username=username, password=url.password)

        scp = SCPClient(ssh.get_transport())
        scp.put(source, url.path, recursive=False)  # recursive should be an option in dest dict?
        ssh.close()
        return dest
    except:
        LOG.error("Could not SCP file %s to destination %s on %s as user %s",
                  source, dest.path, dest.hostname, username)
        raise
