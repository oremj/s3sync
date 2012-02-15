import re

from optparse import OptionParser
from os.path import expanduser, join

from s3sync.sync import Sync


parser = OptionParser(usage="s3sync -[nv] [-c CONFIG] src dest")
parser.add_option("-n", "--dry-run", action="store_true",
                    default=False, help="Show proposed changes")
parser.add_option("--exclude", action="append")
parser.add_option("-v", "--verbose", action="store_true", default=False)
parser.add_option("-c", "--config", default=expanduser(join('~', '.s3sync')))


def main():
    options, args = parser.parse_args()
    try:
        src_uri = args[0]
        dest_uri = args[1]
    except IndexError:
        parser.error("destinations not specified")

    if options.exclude:
        excludes = [re.compile(e) for e in options.exclude]
    else:
        excludes = []

    s = Sync(src_uri, dest_uri, options.config, options.dry_run, excludes=excludes)
    s.print_status()
    s.sync()


if __name__ == "__main__":
    main()
