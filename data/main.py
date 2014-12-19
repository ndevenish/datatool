# coding: utf-8

"""Manage data sets and locations.

Usage:
  data set create [--name=<name>] [<file>...]
  data set tag [--delete] <name-or-id> <tag> [<tag>...]
  data add <name-or-id> <file> [<file>...]
  data files <name-or-id>
  data search <tag> [<tag>...]
  data identify <file> [<file>...]

Options:
  --name=<name>   Give a data set a name when creating
  -d, --delete    Remove given tags from a dataset instead of adding

Commands:
  set         Manipulate and create data sets
  set create  Create a new data set, optionally named and with a list of files
  set tag     Add a tag (or list of tags) to a dataset
  add         Add a set of files to a dataset
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

from .index import find_index, Index

def main(argv):
  args = docopt(__doc__, argv=argv[1:])
  # Find the data index file
  index_filename = find_index()
  if not index_filename:
    logger.error("Could not find index file")
    return 1
  # Open and parse the index
  with open(find_index(), 'r') as index_file:
    index = Index(index_file)

  if args["set"]:
    process_set(args, index)
  elif args["add"]:
    dataset = index.fetch_dataset(args['<name-or-id>'])
    index.add_files(dataset.id, args["<file>"])
  elif args["files"]:
    dataset = index.fetch_dataset(args['<name-or-id>'])
    for datafile in dataset.files:
      print (datafile.path)
  elif args["search"]:
    pass
  elif args["identify"]:
    pass
  # Write any changes to the index
  with open(find_index(), 'a') as index_file:
    index.write(index_file)
  print (args)
  return 0

def process_set(args, index):
  if args["create"]:
    set_id = index.create_set(name=args["--name"])
    if args["<file>"]:
      index.add_files(set_id, args["<file>"])
  elif args["tag"]:
    dataset = index.fetch_dataset(args['<name-or-id>'])
    if args["--delete"]:
      index.remove_tags(dataset.id, args["<tag>"])
    else:
      index.add_tags(dataset.id, args["<tag>"])
