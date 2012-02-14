from os.path import join

from .log_settings import log
from .proxies import PROXY_MAP


class Sync(object):
    """
        Example: 
                 excludes = [re.compile("^temp/.*")]
                 conn = S3Connection('pub', 'priv')
                 bucket = conn.get_bucket('my_bucket')
                 s = Sync(LocalBackend("/tmp/foo"), S3Backend("files/", bucket=bucket), excludes=excludes)
                 s.print_status()
                 s.sync()

    """
    def __init__(self, src, dest, check_md5=False, excludes=None):
        self.check_md5 = check_md5
        self.excludes = excludes
        self.src = src
        self.dest = dest
        self.proxy = PROXY_MAP[(self.src.__class__, self.dest.__class__)](self)
        self.adds = self._additions()
        self.removals = self._removals()
        self.modifications = self._modifications()

    def print_status(self):
        print "Additions: %d" % len(self.adds)
        print "Removals: %d" % len(self.removals)
        print "Modifications: %d" % len(self.modifications)
    
    def _excluded(self, f):
        if any(r.search(f) for r in self.excludes):
            log.debug("Excluded: %s" % f)
            return True
        else:
            return False

    def _src_set(self):
        return set(f for f in self.src.files if not self._excluded(f))

    def _dest_set(self):
        return set(f for f in self.dest.files)

    def _additions(self):
        return self._src_set() - self._dest_set()

    def _removals(self):
        return self._dest_set() - self._src_set()

    def _modifications(self):
        log.info("Checking for modified files")
        mods = []
        if self.check_md5:
            possible = self._src_set() & self._dest_set()
            for f in possible:
                src = self.src.files[f]
                dest = self.dest.files[f]
                if src.md5 != dest.md5:
                    mods.append(f)
        else:
            return []

    def sync(self):
        log.info("Syncing with S3")

        if self.adds:
            log.info("Adding Files")
            for i, f in enumerate(self.adds):
                src = self.src.files[f].path
                dest = join(self.dest.root, f)
                self.proxy.add(src, dest)
                log.debug("Added: %s as %s (%d/%d)" % (src, dest, i + 1, len(self.adds)))
        
        if self.removals:
            log.info("Removing Files")
            self.proxy.delete([join(self.dest.root, f) for f in self.removals])
