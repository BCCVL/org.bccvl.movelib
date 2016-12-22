import Cookie
import csv
import codecs
import hashlib
import os
import socket
import struct
from time import time
from urllib import quote
from urlparse import urlsplit
import zipfile


class AuthTkt(object):

    def __init__(self, secret, uid, data='', ip='0.0.0.0', tokens=(),
                 base64=True, ts=None):
        self.secret = str(secret)
        self.uid = str(uid)
        self.data = data
        self.ip = ip
        self.tokens = ','.join(tok.strip() for tok in tokens)
        self.base64 = base64
        self.ts = int(time() if ts is None else ts)
        # TODO: make configurable
        self.hashalg = 'md5'

    def ticket(self):
        v = self.cookie_value()
        if self.base64:
            return v.encode('base64').strip().replace('\n', '')
        return v

    def cookie(self, name, **kwargs):
        name = str(name)
        c = Cookie.SimpleCookie()
        c[name] = self.ticket()

        kwargs.setdefault('path', '/')
        c[name].update(kwargs)

        return c

    def cookie_value(self):
        parts = ['%s%08x%s' % (self._digest(), self.ts, quote(self.uid))]
        if self.tokens:
            parts.append(self.tokens)
        parts.append(self.data)
        return '!'.join(parts)

    def _digest(self):
        return hashlib.md5(self._digest0() + self.secret).hexdigest()

    def _digest0(self):
        parts = (self._encode_ip(self.ip), self._encode_ts(self.ts),
                 self.secret, self.uid, '\0', self.tokens, '\0', self.data)
        return hashlib.md5(''.join(parts)).hexdigest()

    def _encode_ip(self, ip):
        return socket.inet_aton(ip)

    def _encode_ts(self, ts):
        return struct.pack('!I', ts)


def get_cookies(settings, userid):
    if not settings.get('secret'):
        return {}
    tokens = [tok.strip() for tok in settings.get('tokens', '').split()]
    ticket = AuthTkt(settings['secret'], userid, tokens=tokens)
    return {
        'name': settings['name'],
        'value': ticket.ticket(),
        'domain': settings['domain'],
        'path': settings.get('path', '/'),
        'secure': settings.get('secure', True),
    }


def build_source(src, userid=None, settings=None):
    source = {'url': src}
    # Create a cookies for http download from the plone server
    url = urlsplit(src)
    if settings is None:
        settings = {}
    if url.scheme in ('http', 'https'):
        cookie_settings = settings.get('cookie', {})
        if url.hostname == cookie_settings.get('domain'):
            source['cookies'] = get_cookies(cookie_settings,
                                            userid)
        source['verify'] = settings.get('ssl', {}).get('verify', True)
    elif url.scheme in ('swift+http', 'swift+https'):
        # TODO: should check swift host name as well
        swift_settings = settings.get('swift', {})
        for key in ('os_auth_url', 'os_username', 'os_password', 'os_tenant_name', 'os_storage_url'):
            if key in swift_settings:
                source[key] = swift_settings[key]
    return source


def build_destination(dest, settings=None):
    destination = {'url': dest}

    # Create a cookies for http download from the plone server
    url = urlsplit(dest)
    if url.scheme in ('swift+http', 'swift+https'):
        # TODO: should check swift host name as well
        # FIXME: assumes settings is not None
        swift_settings = settings and settings.get('swift', {}) or {}
        for key in ('os_auth_url', 'os_username', 'os_password', 'os_tenant_name', 'os_storage_url'):
            if key not in swift_settings:
                continue
            destination[key] = swift_settings[key]
    return destination


def zip_occurrence_data(occzipfile, data_folder_path, filelist):
    with zipfile.ZipFile(occzipfile, 'w') as zf:
        for filename in filelist:
            if os.path.isfile(os.path.join(data_folder_path, filename)):
                zf.write(os.path.join(data_folder_path, filename), 'data/' + filename)


class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """

    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")


class UnicodeCSVReader(object):
    """
    Expect an iterator that returns unicode strings which will be
    parsed by csv module

    """

    def __init__(self, f, encoding="utf-8", **kwds):
        """
        Assumes utf-8 encoding and ignores all decoding errors.

        f ... an iterator (maybe file object opened in text mode)
        """
        # build a standard csv reader, that works on utf-8 strings
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, **kwds)

    def __iter__(self):
        """
        return an iterator over f
        """
        return self

    def next(self):
        """
        return next row frow csv.reader, each cell as unicode again
        """
        return [cell.decode('utf-8') for cell in self.reader.next()]


class UnicodeCSVWriter(object):
    """
    Writes unicode csv rows as utf-8 into file.
    """

    def __init__(self, f):
        """
        f ... an open file object that expects byte strings as input.
        """
        self.writer = csv.writer(f)

    def writerow(self, row):
        """
        encode each cell value as utf-8 and write to writer as usual
        """
        self.writer.writerow([cell.encode('utf-8') for cell in row])

    def writerows(self, rows):
        """
        write a list of rows using self.writerow
        """
        for row in rows:
            self.writerow(row)
