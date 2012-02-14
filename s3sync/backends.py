import os
from os.path import join, relpath

from .files import LocalFile, S3File
from .log_settings import log


class Backend(object):
    def __init__(self, root, **kwargs):
        self.root = root
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
        self.bucket = kwargs['bucket']
    
    def _filelist(self):
        log.info("Fetching S3 Files")
        return dict((relpath(k.name, self.root), S3File(self, k.name, k.etag)) for k in self.bucket.list())
