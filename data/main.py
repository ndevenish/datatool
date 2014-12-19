# coding: utf-8

"""Manage data sets and locations.

Usage:
  data set create [--name=<name>] [<file>...]
  data set tag [--delete] <name-or-id> <tag> [<tag>...]
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
  files       Retrieve the file list for a specific data set
  search      Find a list of dataset names matching a list of tags
  identify    Find any datasets containing any given files
"""

from __future__ import print_function

from docopt import docopt

def main(argv):
  args = docopt(__doc__, argv=argv[1:])
  print (args)
  return 0
