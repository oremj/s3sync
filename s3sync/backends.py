import os
from os.path import join, relpath
from urlparse import urlparse

from boto.s3.connection import S3Connection

from .files import LocalFile, S3File
from .log_settings import log


backends = {}


def register_backend(k, b):
    backends[k] = b


def get_backend(uri, c):
    p = urlparse(uri)
    try:
        b = backends[p.scheme]
    except KeyError:
        log.critical("Backend %s does not exist" % p.scheme)
        raise
    else:
        return b(root=p.path, loc=p.netloc, config=c)


class Backend(object):
    def __init__(self, root, loc, config):
        self.root = root
        self.loc = loc
        self.config = config
        self._files = None

    @property
    def files(self):
        if self._files is None:
            self._files = self._filelist()
        return self._files


class LocalBackend(Backend):
    
    def _filelist(self):
        log.info("Fetching Local Files")
        files = [join(root, f).decode('utf-8')
                    for root, dirs, files in os.walk(self.root)
                        for f in files]
        return dict((relpath(f, self.root), LocalFile(self, f)) for f in files)


class S3Backend(Backend):
    
    def __init__(self, *args, **kwargs):
        super(S3Backend, self).__init__(*args, **kwargs)
        self.root = self.root.strip('/')
        s3_config = self.config['s3']
        c = S3Connection(s3_config['access_key_id'], s3_config['secret_access_key'])
        self.bucket = c.get_bucket(self.loc)

    
    def _filelist(self):
        log.info("Fetching S3 Files")
        return dict((relpath(k.name, self.root), S3File(self, k.name, k.etag)) for k in self.bucket.list())


register_backend('s3', S3Backend)
register_backend('local', LocalBackend)
