# coding: utf-8

"""Manage data sets and locations.

Usage:
  data [options] set create [--name=<name>] [<file>...]
  data [options] set tag [--delete] <name-or-id> <tag> [<tag>...]
  data [options] add <name-or-id> <file> [<file>...]
  data [options] index <file> [<file>...]
  data [options] files <name-or-id>
  data [options] search <tag> [<tag>...]
  data [options] identify <file> [<file>...]

Options:
  --authority=<auth>  Use a specific data authority
  --index=<index>     Use a specific data index
  --name=<name>   Give a data set a name when creating
  -d, --delete    Remove given tags from a dataset instead of adding

Commands:
  set         Manipulate and create data sets
  set create  Create a new data set, optionally named and with a list of files
  set tag     Add a tag (or list of tags) to a dataset
  add         Add a set of files to a dataset
  index       Explicitly add a set of files to the index
  files       Retrieve the file list for a specific data set
  search      Find a list of dataset names matching a list of tags
  identify    Find any datasets containing any given files
"""

from __future__ import print_function
import sys
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)

from docopt import docopt

from .index import find_index, LocalFileIndex
from .authority import find_authority, LocalFileAuthority

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

  if args["set"]:
    process_set(args, authority, index)
  elif args["add"]:
    dataset = index.fetch_dataset(args['<name-or-id>'])
    index.add_files(dataset.id, args["<file>"])
  elif args["files"]:
    dataset = index.fetch_dataset(args['<name-or-id>'])
    for datafile in dataset.files:
      print (datafile.path)
  elif args["search"]:
    sets = [index[x] for x in index.search(args["<tag>"])]
    logger.info("{} results".format(len(sets)))
    max_name = max([len(x.name) for x in sets if x.name] + [0])
    for dataset in sets:
      print (dataset.id, dataset.name.ljust(max_name), ",".join(dataset.tags))

  elif args["identify"]:
    pass
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
  # elif args["tag"]:
  #   dataset = index.fetch_dataset(args['<name-or-id>'])
  #   if args["--delete"]:
  #     index.remove_tags(dataset.id, args["<tag>"])
  #   else:
  #     index.add_tags(dataset.id, args["<tag>"])
