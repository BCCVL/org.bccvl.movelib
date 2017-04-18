import base64
import csv
import codecs
import hashlib
import io
import os
import socket
import struct
from time import time
import zipfile

import six
from six.moves import http_cookies as cookies
from six.moves.urllib_parse import quote, urlsplit


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
            enc = base64.b64encode(v.encode('utf-8'))
            enc = enc.decode('utf-8')
            return enc.strip().replace('\n', '')
        return v

    def cookie(self, name, **kwargs):
        name = str(name)
        c = cookies.SimpleCookie()
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
        return hashlib.md5(self._digest0().encode('utf-8') + self.secret.encode('utf-8')).hexdigest()

    def _digest0(self):
        parts = (self._encode_ip(self.ip), self._encode_ts(self.ts),
                 self.secret.encode('utf-8'), self.uid.encode('utf-8'), b'\0',
                 self.tokens.encode('utf-8'), b'\0', self.data.encode('utf-8'))
        return hashlib.md5(b''.join(parts)).hexdigest()

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

    def __init__(self, f):
        self.reader = f

    def __iter__(self):
        return self

    def next(self):
        """
        Try some popular encodings to convert input to unicode
        """
        line = next(self.reader)
        for codec in ('utf-8', 'cp1252', 'mac_roman', 'latin_1', 'ascii'):
            try:
                line = line.decode(codec)
                break
            except UnicodeDecodeError as e:
                pass
        if six.PY2:
            # in py2 we have to re-encode in defined encoding
            return line.encode('utf-8')
        # in py3 we can just return unicode
        return line

    __next__ = next


class UnicodeCSVReader(object):
    """
    Expect an iterator that returns unicode strings which will be
    parsed by csv module

    """

    def __init__(self, f, **kwds):
        """
        Assumes utf-8 encoding and ignores all decoding errors.

        f ... an iterator (maybe file object opened in binary mode)
        """
        # build a standard csv reader, that works on utf-8 strings
        f = UTF8Recoder(f)
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
        if six.PY2:
            # in py2 we have to decode csv parse result from utf-8 to unicode
            return [cell.decode('utf-8') for cell in next(self.reader)]
        # in py3 we already get unicode
        return next(self.reader)

    __next__ = next


class UnicodeCSVWriter(object):
    """
    Writes unicode csv rows as utf-8 into file.
    """

    def __init__(self, f):
        """
        f ... an open file object that expects byte strings as input.
        """
        if six.PY3:
            # wrap the bytesio into TextIOWrapper
            f = codecs.getwriter('utf-8')(f) # io.TextIOWrapper(f, encoding='utf-8')
        self.writer = csv.writer(f)

    def writerow(self, row):
        """
        encode each cell value as utf-8 and write to writer as usual
        """
        if six.PY3:
            self.writer.writerow(row)
        else:
            self.writer.writerow([cell.encode('utf-8') if isinstance(cell, unicode) else cell for cell in row])

    def writerows(self, rows):
        """
        write a list of rows using self.writerow
        """
        for row in rows:
            self.writerow(row)
