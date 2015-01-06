# coding: utf-8

import uuid
import datetime
import logging
logger = logging.getLogger(__name__)

import dateutil.parser

from .dataset import Dataset
from .datafile import DataFile, FileInstance

_HANDLERS = {}

def handles(name):
  def passthrough(func):
    _HANDLERS[name.lower()] = func
    func.command = name.lower()
    return func
  return passthrough

class UnknownHandlerError(Exception):
  pass

class CommandTimestampFormatError(ValueError):
  pass

def handler_for(name):
  if not name.lower() in _HANDLERS:
    raise UnknownHandlerError("Unknown command: {}".format(name))
  return _HANDLERS[name.lower()]

class Command(object):
  def __init__(self):
    self._timestamp = datetime.datetime.utcnow()

  @property
  def timestamp(self):
    if isinstance(self._timestamp, basestring):
      try:
        self._timestamp = dateutil.parser.parse(self._timestamp)
      except ValueError:
        raise CommandTimestampFormatError("Invalid time format: {}".format(self._timestamp))
    return self._timestamp
  @timestamp.setter
  def timestamp(self, value):
      self._timestamp = value
  
@handles("createset")
class CreateSetCommand(Command):
  def __init__(self, cid=None):
    super(CreateSetCommand, self).__init__()
    self.id = cid or uuid.uuid4().hex
  @classmethod
  def from_data(cls, data):
    return cls(data["id"])
  def to_data(self):
    return {"id": self.id}
  def apply(self, index):
    assert not self.id in index.datasets
    index[self.id] = Dataset(self.id)
  def __str__(self):
    return "[Create Set {}]".format(self.id)

@handles("createfile")
class CreateFileCommand(Command):
  def __init__(self, entry):
    super(CreateFileCommand, self).__init__()
    self.id = str(entry.hashsum)
    self.entry = FileInstance(filename=entry.filename,hashsum=entry.hashsum,size=entry.size,timestamp=entry.timestamp)
  def to_data(self):
    return self.entry.to_data()
  @classmethod
  def from_data(cls, data):
    return cls(FileInstance.from_data(data))
  def apply(self, index):
    index[self.id] = DataFile(self.id)
  def __str__(self):
    return "[Create file {}]".format(self.id)

@handles("addfilestoset")
class AddFilesToSetCommand(Command):
  def __init__(self, dataset, files):
    super(AddFilesToSetCommand, self).__init__()
    self.dataset = dataset
    self.files = files
  @classmethod
  def from_data(cls, data):
    return cls(data.get("set"), data.get("files"))
  def to_data(self):
    return {"files": self.files, "set": self.dataset}
  def apply(self, authority):
    dataset = authority.datasets[self.dataset]
    files = [authority.files[str(x)] for x in self.files if not authority.files[x] in dataset.files]
    dataset.files.extend(files)
  def __str__(self):
    return "[Add {} files to {}]".format(len(self.files), self.dataset)

@handles("addtags")
class AddTagsCommand(Command):
  command = "addtags"
  def __init__(self, objId, tags=None):
    super(AddTagsCommand, self).__init__()
    self.objId = objId
    self.tags = set(tags) or {}
  @classmethod
  def from_data(cls, data):
    return cls(data["id"], data["tags"])
  def to_data(self):
    return {"id": self.objId, "tags": list(self.tags)}
  def apply(self, authority):
    tagee = authority[self.objId]
    tagee.tags = tagee.tags.union(self.tags)
  def __str__(self):
    return "[Add tags {{{}}} to set {}]".format(", ".join(self.tags), self.objId)

@handles("removetags")
class RemoveTagsCommand(AddTagsCommand):
  command = "removetags"
  def apply(self, authority):
    tagee = authority[self.objId]
    tagee.tags = tagee.tags.difference(self.tags)
  def __str__(self):
    return "[Remove tags {{{}}} from set {}]".format(", ".join(self.tags), self.objId)

@handles("setproperty")
class SetPropertyCommand(Command):
  def __init__(self, _id, property, value):
    super(SetPropertyCommand, self).__init__()
    self.id = _id
    self.property = property
    self.value = value
  def apply(self, authority):
    authority[self.id].attrs[self.property] = self.value
  def __str__(self):
    return "[Set {}.{} to {}]".format(self.id, self.property, self.value)
  @classmethod
  def from_data(cls, data):
    return cls(data["id"], data["property"], data["value"])
  def to_data(self):
    return {"id": self.id, "property": self.property, "value":self.value}