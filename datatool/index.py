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

from .datafile import hashfile, FileInstance
from .util import first

reLineHeader = re.compile(r'^\s*([^\s]+)\s+(\w+)\s+([^\s]+)\s+(\w+)\s+(.*)$')

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
    date, hashsum, timestamp, size, filename = line_data.groups()
    yield (date, FileInstance(hashsum=hashsum,timestamp=timestamp,size=size,filename=filename))

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
    if size != entry.size or str(timestamp) != str(entry.timestamp):
      logger.info("File {} appears to have changed, re-indexing".format(filename))
      entry = FileInstance.from_file(filename)
      self._process_entries([entry])
    return entry

  def add_files(self, filenames):
    files = []
    # Look for this file in the index
    for filename in [os.path.abspath(x) for x in filenames]:
      if filename in self._names:
        files.append(self._update_if_required(filename))
      else:
        logger.debug("Adding new file to index: {}".format(filename))
        entry = entry_for_file(filename)
        self._process_entries([entry])
        files.append(entry)
    return files

  def fetch_file(self, filename_or_checksum):
    """Fetches a file instance from a filename or partial checksum"""
    fullpath = os.path.abspath(filename_or_checksum)
    results = [x for x in self._data.values() if x.hashsum.startswith(filename_or_checksum) or fullpath == x.filename]
    assert len(results) <= 1
    return first(results)


class LocalFileIndex(Index):
  def __init__(self, filename):
    super(LocalFileIndex,self).__init__()
    self._filename = filename
    logger.debug("Loading index file entries...")
    with open(filename) as index_stream:
      self._process_entries([y for x,y in parse_index(index_stream)])
    logger.debug("done.")
    self._pending = []

  def write(self):
    with open(self._filename, 'a') as stream:
      # Get the last byte and make sure it is a return. Otherwise, push one out
      try:
        stream.seek(-1,os.SEEK_END)
        if not stream.read(1) == '\n':
          stream.write('\n')
      except IOError:
        # Could be an empty file...
        pass
      stream.seek(0,os.SEEK_END)

      # Now dump all the pending entries
      date = datetime.datetime.utcnow().isoformat()
      for entry in self._pending:
        line = " ".join([str(x) for x in [date, entry.hashsum, entry.timestamp, entry.size, entry.filename]]) + "\n"
        logger.debug("Writing: " + line.strip())
        stream.write(line)
      self._pending = []