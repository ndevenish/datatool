# coding: utf-8

"""Manages and reads the data authority"""

import os
import re
import json
import dateutil.parser
import logging
import datetime
from StringIO import StringIO
from collections import namedtuple
logger = logging.getLogger(__name__)

from .datafile import hashfile

reLineHeader = re.compile(r'^\s*([^\s]+)\s+(\w+)\s+(\w+)\s+(\w+)\s+(.*)$')

IndexEntry = namedtuple("IndexEntry", ["date", "hashsum", "timestamp", "size", "filename"])

def entry_for_file(filename):
  fileData = os.stat(filename)
  size, timestamp = (fileData.st_size, fileData.st_mtime)
  sha = hashfile(filename)
  return IndexEntry(datetime.datetime.utcnow(), sha, timestamp, size, filename)
  
class IndexFileError(IOError):
  pass

def find_index():
  """Looks in standard and environmental locations for the data index."""
  locs = [os.environ.get("DATA_INDEX"), "~/.data.index"]
  for loc in [os.path.expanduser(x) for x in locs if x]:
    if os.path.isfile(loc):
      return loc
  return None

def parse_index(indexfile):
  """Read an index file stream and returns the entry history"""
  decoder = json.JSONDecoder()
  for num, line in enumerate(indexfile, 1):
    if line.isspace() or line.startswith('#'):
      continue
    line_data = reLineHeader.match(line)
    if not line_data:
      raise IndexFileError("Could not read index line {}".format(num))
    yield IndexEntry._make(line_data.groups())

class Index(object):
  def __init__(self):
    self._data = {}
    self._names = {}
    self._pending = []

  def _process_entries(self, entries):
    for entry in entries:
      logger.debug("Adding index entry: {}/{}".format(entry.hashsum[:6], entry.filename))
      self._data[entry.hashsum] = entry
      self._names[entry.filename] = entry
      self._pending.append(entry)

  def _update_if_required(self, filename):
    assert filename in self._names
    entry = self._names[filename]
    fileData = os.stat(filename)
    size, timestamp = (fileData.st_size, fileData.st_mtime)
    if size != entry.size or timestamp != entry.timestamp:
      logger.info("File {} appears to have changed, re-indexing".format(filename))
      sha = hashfile(filename)
      entry = IndexEntry(datetime.datetime.utcnow(), sha, timestamp, size, filename)
      self._process_entries([entry])
    return entry

  def add_files(self, filenames):
    # Look for this file in the index
    for filename in [os.path.abspath(x) for x in filenames]:
      if filename in self._names:
        yield self._update_if_required(filename)
      else:
        logger.debug("Adding new file to index: {}".format(filename))
        entry = entry_for_file(filename)
        self._process_entries([entry])
        yield entry


class LocalFileIndex(Index):
  def __init__(self, filename):
    super(LocalFileIndex,self).__init__()
    with open(filename) as index_stream:
      self._process_entries(parse_index(index_stream))
    self._pending = []

  def write(self):
    pass