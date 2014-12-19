# coding: utf-8

"""Manages and reads the data index"""

import os
import re
import json
import dateutil.parser
import logging
from StringIO import StringIO
logger = logging.getLogger(__name__)

from .handlers import handler_for, CreateSetCommand, SetPropertyCommand, \
                      AddFilesCommand, AddTagsCommand, RemoveTagsCommand
from .dataset import Datafile

# Look for a non-blank line 
reLineHeader = re.compile(r'^\s*([^\s]+)\s+(\w+)\s+(.*)$')

def find_index():
  """Looks in standard and environmental locations for the data index."""
  locs = [os.environ.get("DATA_INDEX"), "~/.data.index"]
  for loc in [os.path.expanduser(x) for x in locs if x]:
    if os.path.isfile(loc):
      return loc
  return None


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
    #logger.debug ("Date: {}, Command: {}, Data: {}".format(command_date, command, data))
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
    if isinstance(index_stream, basestring):
      self._stream = StringIO(index_stream)
    self._commands = []
    # Parse and apply the commands to a fresh data index
    self._data = IndexData()
    self._process_commands(self._data, parse_index(index_stream))
    # Remember how many commands came from the stream
    self._streamindex = len(self._commands)


  def _process_commands(self, index_data, commands):
    logger.debug("Applying index file commands...")
    for command in commands:
      self._apply_command(command)
    logger.debug("done.")

  def _apply_command(self, command):
    logger.debug("Applying {}".format(str(command)))
    self._commands.append(command)
    command.apply(self._data)   
    return command 

  def create_set(self, name=None):
    """Create a (optionally named) data set and return the id"""
    if name:
      if name in [x.name for x in self._data.datasets.values()]:
        raise IndexFileError("Dataset named {} already exists".format(name))
    cmd = self._apply_command(CreateSetCommand(name=name))
    return cmd.id

  def rename_set(self, set_id, new_name):
    #dataset = self._data.datasets[set_id]
    assert not new_name in [x.name for x in self._data.datasets.values()]
    self._apply_command(SetPropertyCommand(set_id, "name", new_name))

  def add_files(self, set_id, filenames):
    # Build the list of files then add them to the dataset
    files = []
    for filename in [os.path.abspath(os.path.expanduser(x)) for x in filenames]:
      files.append(Datafile.from_file(filename))
    self._apply_command(AddFilesCommand(set_id, files))

  def add_tags(self, set_id, tags):
    self._apply_command(AddTagsCommand(set_id, tags))

  def remove_tags(self, set_id, tags):
    self._apply_command(RemoveTagsCommand(set_id, tags))

  def write(self, stream):
    """Writes any changes to a specified stream"""
    # Get the last byte and make sure it is a return. Otherwise, push one out
    try:
      stream.seek(-1,os.SEEK_END)
      if not stream.read(1) == '\n':
        stream.write('\n')
    except IOError:
      # Could be an empty file...
      pass
    stream.seek(0,os.SEEK_END)
    # Now dump all the commands that are unprocessed
    for command in self._commands[self._streamindex:]:
      line = "{} {} {}\n".format(command.timestamp.isoformat(), command.command, json.dumps(command.to_data()))
      logger.debug("Writing: " + line.strip())
      stream.write(line)
    self._streamindex = len(self._commands)

  def fetch_dataset(self, name_or_id):
    """Retrieve a single dataset from either the name, or a shortened (or complete) hash"""
    results = [y for x, y in self._data.datasets.items() if x.startswith(name_or_id) or y.name == name_or_id]
    assert len(results) == 1
    return results[0]

  def __getitem__(self, id):
    return self._data.datasets[id]

  def search(self, tags):
    """Retrieve a list of all datasets matching a particular set of tags"""
    tags = set(tags)
    data = [x.id for x in self._data.datasets.values() if tags.issubset(x.tags)]
    return tuple(data)


