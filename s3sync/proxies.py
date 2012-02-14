import os
import os.path
from boto.exception import S3ResponseError

from .backends import LocalBackend, S3Backend
from .log_settings import log

class Proxy(object):
    def __init__(self, sync):
        self.sync = sync


class S3toLocalProxy(Proxy):
    def add(self, src, dest):
        dir = os.path.dirname(dest)
        if not os.path.isdir(dir):
            log.debug("Creating directory: %s" % dir)
            os.makedirs(dir)
        k = self.sync.src.bucket.get_key(src)
        k.get_contents_to_filename(dest)
    
    def delete(self, files):
        pass


class LocaltoS3Proxy(Proxy):
    def add(self, src, dest):
        k = self.sync.dest.bucket.new_key(dest)
        try:
            k.set_contents_from_filename(src)
        except S3ResponseError:
            log.error("Error Uploading: %s" % dest)

    
    def delete(self, files):
        res = self.sync.dest.bucket.delete_keys(files)
        for d in res.deleted:
            log.debug("Removed: %s" % d.key)
        for e in res.errors:
            log.error("Error removing %s (%s)" % (e.key, e.message))




PROXY_MAP = {(LocalBackend, S3Backend): LocaltoS3Proxy,
             (S3Backend, LocalBackend): S3toLocalProxy,
            }
