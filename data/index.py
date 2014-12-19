# coding: utf-8

"""Manages and reads the data index"""

from __future__ import print_function

import os
import re
import json
import dateutil.parser
import logging
logger = logging.getLogger(__name__)

from .handlers import handler_for, CreateSetCommand

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
  def __init__(self):
    self.datasets = {}

class IndexFileError(IOError):
  pass

class Index(object):
  def __init__(self, index_stream=None):
    self._stream = index_stream
    # Extract the command list to build the index
    self._commands = []
    # Now apply the parsed commands to the data index
    self._data = IndexData()
    self._process_commands(self._data, self._parse_index(index_stream))
    # Keep track of commands we have applied vs from the stream
    self._streamindex = len(self._commands)

  def _parse_index(self, indexfile):
    """Read an index file and returns the command history"""
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

  def write(self, stream=None):
    """Writes any changes to the original stream file, or the specified one"""
    stream = stream or self._stream
    # Get the last byte and make sure it is a return. Otherwise, push one out
    stream.seek(-1,os.SEEK_END)
    if not stream.read(1) == '\n':
      stream.write('\n')
    # Now dump all the commands
    for command in self._commands[self._streamindex:]:
      line = "{} {} {}\n".format(command.timestamp.isoformat(), command.command, json.dumps(command.to_data()))
      logger.debug("Writing: " + line)
      stream.write(line)
    self._streamindex = len(self._commands)


