# coding: utf-8

"""Manages and reads the data authority"""

import os
import re
import json
import dateutil.parser
import logging
logger = logging.getLogger(__name__)

from six.moves import StringIO

from .handlers import handler_for, CreateSetCommand, CreateFileCommand, \
                      SetPropertyCommand, AddFilesToSetCommand, \
                      AddTagsCommand, RemoveTagsCommand, RmFilesFromSetCommand, \
                      DeleteSetCommand
from .datafile import DataFile, FileInstance
from .dataset import Dataset
from .util import first

# Look for a non-blank line
reLineHeader = re.compile(r'^\s*([^\s]+)\s+(\w+)\s+(.*)$')

def find_authority():
  """Looks in standard and environmental locations for the data index."""
  locs = [os.environ.get("DATA_AUTHORITY"), "~/.data.authority"]
  for loc in [os.path.expanduser(x) for x in locs if x]:
    if os.path.isfile(loc):
      return loc
    elif os.path.isdir(loc):
      path = os.path.join(loc, "data.authority")
      if os.path.isfile(path):
        return path
  return None


def parse_authority(indexfile):
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

class AuthorityFileError(IOError):
  pass

class AuthorityData(object):
  """The data object, holding the current state of the index"""
  def __init__(self):
    self.datasets = {}
    self.files = {}
    self.entries = {}

  def __getitem__(self, id):
    return self.entries[id]

  def __setitem__(self, key, value):
    key = str(key)
    self.entries[key] = value
    if isinstance(value, DataFile):
      self.files[key] = value
    elif isinstance(value, Dataset):
      self.datasets[key] = value
    else:
      raise KeyError("Instance not recognised")

  def __delitem__(self, key):
    del self.entries[key]
    if key in self.datasets:
      del self.datasets[key]
    if key in self.files:
      del self.files[key]

  def values(self):
    return self.entries.values()

  def get(self, key, default=None):
    return self.entries.get(key, default)

class Authority(object):
  def __init__(self):
    self._data = AuthorityData()
    self._commands = []
    self._commandindex = 0

  def _process_commands(self, commands):
    """Process a set of commands"""
    logger.debug("Applying authority file commands...")
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
        raise AuthorityFileError("Dataset named {} already exists".format(name))
    cmd = self._apply_command(CreateSetCommand())
    if name:
      self._apply_command(SetPropertyCommand(cmd.id, "name", name))
    return cmd.id

  def delete_set(self, set_id):
    assert set_id in self._data.entries
    self._apply_command(DeleteSetCommand(set_id))

  def rename_set(self, set_id, new_name):
    #dataset = self._data.datasets[set_id]
    assert not new_name in [x.name for x in self._data.datasets.values()]
    self._apply_command(SetPropertyCommand(set_id, "name", new_name))

  def add_files(self, set_id, file_entries):
    for f in file_entries:
      if not f.hashsum in self._data.files:
        self._apply_command(CreateFileCommand(f))
    self._apply_command(AddFilesToSetCommand(set_id, [x.hashsum for x in file_entries]))

  def remove_files(self, set_id, file_hashes):
    self._apply_command(RmFilesFromSetCommand(set_id, file_hashes))

  def add_tags(self, set_id, tags):
    if not self._data[set_id].tags.issuperset(tags):
      self._apply_command(AddTagsCommand(set_id, tags))

  def remove_tags(self, set_id, tags):
    if not self._data[set_id].tags.isdisjoint(tags):
      self._apply_command(RemoveTagsCommand(set_id, tags))

  def fetch_dataset(self, name_or_id):
    """Retrieve a single dataset from either the name, or a shortened (or complete) hash"""
    results = [y for x, y in self._data.datasets.items() if x.lower().startswith(name_or_id.lower())
                                                         or (y.name and y.name.lower() == name_or_id.lower())]
    assert len(results) <= 1
    return first(results)

  def __getitem__(self, id):
    return self._data.datasets[id]

  def search(self, tags):
    """Retrieve a list of all datasets matching a particular set of tags"""
    tags = set(tags)
    data = [x.id for x in self._data.datasets.values() if tags.issubset(x.tags)]
    return tuple(data)

  def apply_index(self, index):
    """Applies an index set to the authority, temporarily merging the data"""
    self.index = index
    for f in index._data.values():
      if not f.hashsum in self._data.files:
        self._apply_command(CreateFileCommand(f))
      self._data.files[f.hashsum].instances.append(f)

  def get_file(self, fileid):
    return self._data.files[fileid]

class StringAuthority(object):
  def __init__(self, stringdata):
    super(StringAuthority,self).__init__()
    # Make a StringIO object from this data
    iodata = StringIO(stringdata)
    for command in parse_authority(iodata):
      command.apply(self._data)
    self._commandindex = len(self._commands)


class LocalFileAuthority(Authority):
  def __init__(self, filename):
    super(LocalFileAuthority,self).__init__()
    self.filename = filename
    with open(filename) as index_stream:
      self._process_commands(parse_authority(index_stream))
    self._commandindex = len(self._commands)

  def write(self):
    """Writes any changes"""
    with open(self.filename, "a") as stream:
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
      for command in self._commands[self._commandindex:]:
        line = "{} {} {}\n".format(command.timestamp.isoformat(), command.command, json.dumps(command.to_data()))
        logger.debug("Writing: " + line.strip())
        stream.write(line)
      self._commandindex = len(self._commands)

class RemoteDeploymentAuthority(Authority):
  def __init__(self, filename):
    """Handles deployments where only a remote authority snapshot may be present"""
    super(RemoteDeploymentAuthority, self).__init__()
    self._parse_remote_authority(filename)

  def _parse_remote_authority(self, filename):
    for line in open(filename).readlines():
      parts = line.split()
      # Extract the metadata from this line
      file_hashsum, dataset_name, file_name = parts[:3]
      file_tags = parts[3:]
      # Retrieve or create a dataset with this name
      dataset = self.fetch_dataset(dataset_name)
      if not dataset:
        dataset = self[self.create_set(name=dataset_name)]
      # Add this file
      fileinstance = FileInstance(filename=file_name, hashsum=file_hashsum)
      self.add_files(dataset.id, [fileinstance])
      self.add_tags(fileinstance.hashsum, file_tags)
      # Explicitly add an instance of this file
      self._data.files[fileinstance.hashsum].instances.append(fileinstance)


