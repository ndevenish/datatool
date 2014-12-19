# coding: utf-8

"""Manages and reads the data index"""

from __future__ import print_function

import os
import re
import json
import dateutil.parser
import logging
logger = logging.getLogger(__name__)

from .handlers import handler_for, CreateSetCommand, SetPropertyCommand

# Look for a non-blank line 
reLineHeader = re.compile(r'^\s*([^\s]+)\s+(\w+)\s+(.*)$')

def find_index():
  """Looks in standard and environmental locations for the data index."""
  locs = [os.environ.get("DATA_INDEX"), "~/.data.index"]
  for loc in [os.path.expanduser(x) for x in locs if x]:
    if os.path.isfile(loc):
      return loc


def parse_index(indexfile):
  """Read an index file stream and returns the command history"""
  decoder = json.JSONDecoder()
  for num, line in enumerate(indexfile, 1):
    if line.isspace() or line.startswith('#'):
      continue
    line_data = reLineHeader.match(line)
    if not line_data:
      raise IndexFileError("Could not read index line {}".format(num))
    command_date, command, raw_data = line_data.groups()
    # Parse the command
    data, dlen = decoder.raw_decode(raw_data)
    if not isinstance(data, dict):
      data = {'data': data}
    logger.debug ("Date: {}, Command: {}, Data: {}".format(command_date, command, data))
    #data['date'] = command_date
    command = handler_for(command)
    cmd = command.from_data(data)
    cmd.timestamp = command_date
    yield cmd

class IndexFileError(IOError):
  pass

class IndexData(object):
  """The data object, holding the current state of the index"""
  def __init__(self):
    self.datasets = {}

class Index(object):
  def __init__(self, index_stream=None):
    self._stream = index_stream
    self._commands = []
    # Parse and apply the commands to a fresh data index
    self._data = IndexData()
    self._process_commands(self._data, parse_index(index_stream))
    # Remember how many commands came from the stream
    self._streamindex = len(self._commands)


  def _process_commands(self, index_data, commands):
    for command in commands:
      self._apply_command(command)
    print (index_data.datasets)

  def _apply_command(self, command):
    logger.debug("Applying {}".format(str(command)))
    self._commands.append(command)
    command.apply(self._data)   
    return command 

  def create_set(self, name=None):
    """Create a (optionally named) data set and return the id"""
    if name:
      assert not name in [x.name for x in self._data.datasets.values()]
    cmd = self._apply_command(CreateSetCommand(name=name))
    return cmd.id

  def rename_set(self, set_id, new_name):
    #dataset = self._data.datasets[set_id]
    assert not new_name in [x.name for x in self._data.datasets.values()]
    self._apply_command(SetPropertyCommand(set_id, "name", new_name))

  def write(self, stream):
    """Writes any changes to a specified stream"""
    # Get the last byte and make sure it is a return. Otherwise, push one out
    stream.seek(-1,os.SEEK_END)
    if not stream.read(1) == '\n':
      stream.write('\n')
    stream.seek(0,os.SEEK_END)
    # Now dump all the commands that are unprocessed
    for command in self._commands[self._streamindex:]:
      line = "{} {} {}\n".format(command.timestamp.isoformat(), command.command, json.dumps(command.to_data()))
      logger.debug("Writing: " + line)
      stream.write(line)
    self._streamindex = len(self._commands)


