# coding: utf-8

"""Manages and reads the data index"""

from __future__ import print_function

import os
import re
from json import JSONDecoder
import dateutil.parser
import logging
logger = logging.getLogger(__name__)

# Look for a non-blank line 
reLineHeader = re.compile(r'^\s*([^\s]+)\s+(\w+)\s+(.*)$')

def find_index():
  """Looks in standard and environmental locations for the data index."""
  locs = [os.environ.get("DATA_INDEX"), "~/.data.index"]
  for loc in [os.path.expanduser(x) for x in locs if x]:
    if os.path.isfile(loc):
      return loc

class IndexData(object):
  """The data object, holding the current state of the index"""
  pass

class IndexFileError(IOError):
  pass

class Index(object):
  def __init__(self, index_stream):
    self._parse_index(index_stream)
    self._stream = index_stream
    self._data = IndexData()

  def _parse_index(self, indexfile):
    """Read an index file and builds the database"""
    decoder = JSONDecoder()
    for num, line in enumerate(indexfile, 1):
      if line.isspace() or line.startswith('#'):
        continue
      line_data = reLineHeader.match(line)
      if not line_data:
        raise IndexFileError("Could not read index line {}".format(num))
      date, command, data = line_data.groups()
      logger.debug ("Date: {}, Command: {}, Data: {}".format(date, command, data))