"""
fs.contrib.dropboxfs
========

A FS object that integrates with Dropbox.

"""

import time
import stat
import shutil
import optparse
import datetime
import tempfile
import os.path

from fs.base import *
from fs.path import *
from fs.errors import *
from fs.filelike import StringIO

from dropbox import rest
from dropbox import client
from dropbox import session


class ContextManagerStream(object):
    def __init__(self, temp):
        self.temp = temp

    def __iter__(self):
        while True:
            data = self.read(16384)
            if not data:
                break
            yield data

    def __getattr__(self, name):
        return getattr(self.temp, name)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class SpooledWriter(ContextManagerStream):
    """Spools bytes to a StringIO buffer until it reaches max_buffer. At that
    point it switches to a temporary file."""
    def __init__(self, client, path, max_buffer=1024**2):
        self.client = client
        self.path = path
        self.max_buffer = max_buffer
        super(SpooledWriter, self).__init__(StringIO())

    def __len__(self):
        return self.bytes

    def write(self, data):
        if self.temp.tell() + len(data) >= self.max_buffer:
            temp = tempfile.TemporaryFile()
            temp.write(self.temp.getvalue())
            self.temp = temp
        self.temp.write(data)

    def close(self):
        # Need to flush temporary file (but not StringIO).
        if hasattr(self.temp, 'flush'):
            self.temp.flush()
        self.bytes = self.temp.tell()
        self.temp.seek(0)
        self.client.put_file(self.path, self, overwrite=True)
        self.temp.close()


class DropboxCache(client.DropboxClient):
    "Performs caching to reduce round-trips."
    def __init__(self, *args, **kwargs):
        super(DropboxCache, self).__init__(*args, **kwargs)
        self.cache = PathMap()
        self.dcache = {}

    def info(self, path):
        "Perform metadata caching."
        metadata = self.cache.get(path)
        if not metadata:
            metadata = super(DropboxCache, self).metadata(path, list=False)
            self.cache[path] = metadata
        # Copy the info so the caller cannot affect our cache.
        return dict(metadata.items())

    def list(self, path):
        "Perform metadata caching."
        if not path in self.dcache:
            metadata = super(DropboxCache, self).metadata(path, list=True)
            if path not in self.cache:
                slimdata = dict(metadata.items())
                slimdata.pop('contents', None)
                self.cache[path] = slimdata
            for child in metadata['contents']:
                self.cache[child['path']] = child
            self.dcache[path] = True
        return self.cache.names(path)

    def file_create_folder(self, path):
        "Add newly created directory to cache."
        try:
            self.cache[path] = super(DropboxCache, self).file_create_folder(path)
        except rest.ErrorResponse, e:
            if e.status == 404:
                raise ResourceNotFoundError(path)
            raise

    def file_copy(self, src, dst):
        try:
            self.cache[dst] = super(DropboxCache, self).file_copy(src, dst)
        except rest.ErrorResponse, e:
            if e.status == 404:
                raise ResourceNotFoundError(src)
            raise

    def file_move(self, src, dst):
        try:
            self.cache[dst] = super(DropboxCache, self).file_move(src, dst)
        except rest.ErrorResponse, e:
            if e.status == 404:
                raise ResourceNotFoundError(src)
            raise
        self.cache.pop(src, None)
        self.dcache.pop(src, None)

    def file_delete(self, path):
        try:
            super(DropboxCache, self).file_delete(path)
        except rest.ErrorResponse, e:
            if e.status == 404:
                raise ResourceNotFoundError(path)
            raise
        self.cache.pop(path, None)
        self.dcache.pop(path, None)

    def put_file(self, path, f, overwrite=False):
        self.cache[path] = super(DropboxCache, self).put_file(path, f, overwrite=overwrite)


def create_token(app_key, app_secret, access_type):
    """Handles the oAuth workflow to enable access to a user's account.

    This only needs to be done initially, the token this function returns
    should then be stored and used in the future with create_client()."""
    s = session.DropboxSession(app_key, app_secret, access_type)
    # Get a temporary token, so we can make oAuth calls.
    t = s.obtain_request_token()
    print "Please visit the following URL and authorize this application.\n"
    print s.build_authorize_url(t)
    print "\nWhen you are done, please press <enter>."
    raw_input()
    # Trade up to permanent access token.
    a = s.obtain_access_token(t)
    print 'Your access token will be printed below, store it for later use.\n'
    print 'Access token:', a.key
    print 'Access token secret:', a.secret
    print "\nWhen you are done, please press <enter>."
    raw_input()
    return a.key, a.secret


def create_client(app_key, app_secret, access_type, token_key, token_secret):
    """Uses token from create_token() to gain access to the API."""
    s = session.DropboxSession(app_key, app_secret, access_type)
    s.set_token(token_key, token_secret)
    c = DropboxCache(s)
    return c


class DropboxFS(FS):
    """A FileSystem that stores data in Dropbox."""

    _meta = { 'thread_safe' : True,
              'virtual' : False,
              'read_only' : False,
              'unicode_paths' : True,
              'case_insensitive_paths' : False,
              'network' : False,
              'atomic.setcontents' : False
             }

    def __init__(self, client, thread_synchronize=True):
        """Create an fs that interacts with Dropbox.

        :param client: the Dropbox API client (from the SDK).
        :param thread_synchronize: set to True (default) to enable thread-safety
        """
        super(DropboxFS, self).__init__(thread_synchronize=thread_synchronize)
        self.client = client

    def __str__(self):
        return "<DropboxFS: >"

    def __unicode__(self):
        return u"<DropboxFS: >"

    def getmeta(self, meta_name, default=NoDefaultMeta):
        if meta_name == 'read_only':
            return self.read_only
        return super(ZipFS, self).getmeta(meta_name, default)

    @synchronize
    def open(self, path, mode="rb", **kwargs):
        if 'r' in mode:
            return ContextManagerStream(self.client.get_file(path))
        else:
            return SpooledWriter(self.client, path)

    @synchronize
    def getcontents(self, path, mode="rb"):
        path = abspath(normpath(path))
        return self.open(self, path, mode).read()

    def setcontents(self, path, data, *args, **kwargs):
        path = abspath(normpath(path))
        self.client.put_file(path, data, overwrite=True)

    def desc(self, path):
        return "%s in Dropbox" % path

    def getsyspath(self, path, allow_none=False):
        "Returns a path as the Dropbox API specifies."
        path = abspath(normpath(path))
        return client.format_path(path)

    def isdir(self, path):
        try:
            info = self.getinfo(path)
            return info.get('isdir', False)
        except ResourceNotFoundError:
            return False

    def isfile(self, path):
        try:
            info = self.getinfo(path)
            return not info.get('isdir', False)
        except ResourceNotFoundError:
            return False

    def exists(self, path):
        try:
            self.getinfo(path)
            return True
        except ResourceNotFoundError:
            return False

    def listdir(self, path="/", wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
        path = abspath(normpath(path))
        try:
            listing = self.client.list(path)
        except rest.ErrorResponse, e:
            if e.status == 404:
                raise ResourceNotFoundError(path)
            raise
        return self._listdir_helper(path, listing, wildcard, full, absolute, dirs_only, files_only)

    @synchronize
    def getinfo(self, path):
        path = abspath(normpath(path))
        try:
            info = self.client.info(path)
        except rest.ErrorResponse, e:
            if e.status == 404:
                raise ResourceNotFoundError(path)
            raise
        info['size'] = info.pop('bytes', 0)
        info['isdir'] = info.pop('is_dir', False)
        info['isfile'] = not info['isdir']
        mtime = time.strptime(info.pop('modified'), '%a, %d %b %Y %H:%M:%S %z')
        info['mtime'] = mtime
        info['modified_time'] = datetime.datetime.fromtimestamp(mtime)
        if path == '/':
            info['mime'] = 'virtual/dropbox'
        return info

    def copy(self, src, dst, *args, **kwargs):
        src = abspath(normpath(src))
        dst = abspath(normpath(dst))
        self.client.file_copy(src, dst)

    def copydir(self, src, dst, *args, **kwargs):
        src = abspath(normpath(src))
        dst = abspath(normpath(dst))
        self.client.file_copy(src, dst)

    def move(self, src, dst, *args, **kwargs):
        src = abspath(normpath(src))
        dst = abspath(normpath(dst))
        self.client.file_move(src, dst)

    def movedir(self, src, dst, *args, **kwargs):
        src = abspath(normpath(src))
        dst = abspath(normpath(dst))
        self.client.file_move(src, dst)

    def rename(self, src, dst, *args, **kwargs):
        src = abspath(normpath(src))
        dst = abspath(normpath(dst))
        try:
            self.client.file_move(src, dst)
        except rest.ErrorResponse, e:
            if e.status == 404:
                raise ResourceNotFoundError(src)
            raise

    def makedir(self, path, recursive=False, allow_recreate=False):
        path = abspath(normpath(path))
        self.client.file_create_folder(path)

    # This does not work, httplib refuses to send a Content-Length: 0 header
    # even though the header is required. We can't make a 0-length file.
    #def createfile(self, path, wipe=False):
    #    self.client.put_file(path, '', overwrite=False)

    def remove(self, path):
        path = abspath(normpath(path))
        self.client.file_delete(path)

    def removedir(self, path, *args, **kwargs):
        path = abspath(normpath(path))
        self.client.file_delete(path)


def main():
    parser = optparse.OptionParser(prog="dropboxfs", description="CLI harness for DropboxFS.")
    parser.add_option("-k", "--app-key", help="Your Dropbox app key.")
    parser.add_option("-s", "--app-secret", help="Your Dropbox app secret.")
    parser.add_option("-t", "--type", default='dropbox', choices=('dropbox', 'app_folder'), help="Your Dropbox app access type.")
    parser.add_option("-a", "--token-key", help="Your access token key (if you previously obtained one.")
    parser.add_option("-b", "--token-secret", help="Your access token secret (if you previously obtained one.")

    (options, args) = parser.parse_args()

    # Can't operate without these parameters.
    if not options.app_key or not options.app_secret:
        parser.error('You must obtain an app key and secret from Dropbox at the following URL.\n\nhttps://www.dropbox.com/developers/apps')

    # Instantiate a client one way or another.
    if not options.token_key and not options.token_secret:
        k, s = create_token(options.app_key, options.app_secret, options.type)
        c = create_client(options.app_key, options.app_secret, options.type, k, s)
    elif not options.token_key or not options.token_secret:
        parser.error('You must provide both the access token and the access token secret.')
    else:
        c = create_client(options.app_key, options.app_secret, options.type, options.token_key, options.token_secret)

    fs = DropboxFS(c)
    fs.rename('/Foobarbaz', '/Blah')

if __name__ == '__main__':
    main()

