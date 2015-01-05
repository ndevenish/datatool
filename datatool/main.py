# coding: utf-8

"""Manage data sets and locations.

Usage:
  data [options] set create [--name=<name>] [<file>...]
  data [options] set tag [--delete] <name-or-id> <tag> [<tag>...]
  data [options] set addfiles <name-or-id> <file> [<file>...]
  data [options] index <file> [<file>...]
  data [options] files <name-or-id>
  data [options] search <tag> [<tag>...]
  data [options] identify <file> [<file>...]
  data [options] sets

Options:
  --authority=<auth>  Use a specific data authority
  --index=<index>     Use a specific data index
  --name=<name>   Give a data set a name when creating
  -d, --delete    Remove given tags from a dataset instead of adding

Commands:
  set           Manipulate and create data sets
  set create    Create a new data set, optionally named and with a file list
  set tag       Add a tag (or list of tags) to a dataset
  set addfiles  Add a set of files to a dataset
  index         Explicitly add a set of files to the index
  files         Retrieve the file list for a specific data set
  search        Find a list of dataset names matching a list of tags
  identify      Find any datasets containing any given files
  sets          List all non-empty data sets
"""

from __future__ import print_function
import sys, os
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)

from docopt import docopt

from .index import find_index, LocalFileIndex
from .authority import find_authority, LocalFileAuthority
from .util import first

def find_sources(authority=None, index=None):
  if not authority:
    authority = find_authority()
  if not (authority):
    logger.error("No data authority specified. Please set DATA_AUTHORITY or pass in with --authority")
    sys.exit(2)
  if not index:
    index = find_index()
  if not (index):
    logger.error("No data index specified. Please set DATA_INDEX or pass in with --index")
    sys.exit(2)
  return (authority, index)


def main(argv):
  args = docopt(__doc__, argv=argv[1:])

  # Find the data index file
  authority_name, index_name = find_sources(args["--authority"], args["--index"])
  authority = LocalFileAuthority(authority_name)
  index = LocalFileIndex(index_name)
  authority.apply_index(index)

  if args["set"]:
    process_set(args, authority, index)
  elif args["index"]:
    for f in args["<file>"]:
      logger.info("Indexing {}".format(f))
    index.add_files([f])
  elif args["files"]:
    dataset = authority.fetch_dataset(args['<name-or-id>'])
    for datafile in dataset.files:
      # Find the last instance that exists
      valid = first([x for x in reversed(datafile.instances) if os.path.isfile(x.filename)])
      if valid:
        print (valid.filename)
      elif not datafile.instances:
        print ("MISSING: ({})".format(datafile.id))
      else:
        print ("MISSING: {}".format(first(datafile.instances).filename))

  elif args["search"]:
    sets = [index[x] for x in authority.search(args["<tag>"])]
    logger.info("{} results".format(len(sets)))
    max_name = max([len(x.name) for x in sets if x.name] + [0])
    for dataset in sets:
      print (dataset.id, dataset.name.ljust(max_name), ",".join(dataset.tags))

  elif args["identify"]:
    assert False
  elif args["sets"]:
    sets = [x for x in authority._data.datasets.values() if x.files]
    nameLen = max(len(x.name or x.id) for x in sets)
    for dataSet in sets:
      print ("{} {} {} files".format((dataSet.name or dataSet.id).ljust(nameLen),
        "(no read)" if not dataSet.can_read() else " "*9, len(dataSet.files)))

  # Write any changes to the index
  authority.write()
  index.write()
  return 0

def process_set(args, authority, index):
  if args["create"]:
    set_id = authority.create_set(name=args["--name"])
    if args["<file>"]:
      # Make sure these are added to the index
      files = list(index.add_files(args["<file>"]))
      authority.add_files(set_id, files)
    print (set_id)
  elif args["tag"]:
    dataset = authority.fetch_dataset(args['<name-or-id>'])
    if args["--delete"]:
      authority.remove_tags(dataset.id, args["<tag>"])
    else:
      authority.add_tags(dataset.id, args["<tag>"])
  elif args["addfiles"]:
    dataset = authority.fetch_dataset(args['<name-or-id>'])
    files = list(index.add_files(args["<file>"]))
    authority.add_files(dataset.id, files)
