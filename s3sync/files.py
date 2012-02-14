import hashlib
from os.path import join, relpath


class File(object):
    def __init__(self, backend, path, md5=None):
        self.backend = backend
        self.path = path
        self._md5 = md5
        if self._md5:
            self._md5 = self._md5.strip('"')

    @property
    def md5(self):
        raise NotImplementedError("md5 is not defined")


class LocalFile(File):
    @property
    def md5(self):
        if not self._md5:
            m = hashlib.md5()
            m.update(open(self.path).read())
            self._md5 = m.hexdigest()
        return self._md5

    def to_remote_name(self, local_root, remote_root):
        return join(remote_root, relpath(self.path, local_root))


class S3File(File):
    @property
    def md5(self):
        return self._md5
