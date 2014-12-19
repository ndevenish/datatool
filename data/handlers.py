# coding: utf-8

import uuid
import datetime
import logging
logger = logging.getLogger(__name__)

import dateutil.parser

from .dataset import Dataset, Datafile

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
  def __init__(self, cid=None, name=None):
    super(CreateSetCommand, self).__init__()
    self.id = cid or uuid.uuid4().hex
    self.name = name 

  @classmethod
  def from_data(cls, data):
    return cls(data["id"], data.get("name"))
  def to_data(self):
    data = {"id": self.id}
    if self.name:
      data["name"] = self.name
    return data
  def apply(self, index):
    assert not self.id in index.datasets
    index.datasets[self.id] = Dataset(self.id, self.name)

  def __str__(self):
    s = "[Create Set {}".format(self.id)
    if self.name:
      s = s + " ({})".format(self.name)
    return s + "]"

@handles("addfiles")
class AddFilesCommand(Command):
  def __init__(self, dataset, files=None):
    super(AddFilesCommand, self).__init__()
    self.dataset_id = dataset
    self.files = files or []

  @classmethod
  def from_data(cls, data):
    # Extract the file list from the data
    files = [Datafile.from_data(x) for x in data["files"]]
    return cls(data.get("set"), files=files)
  def to_data(self):
    # Build the file info, but trim off blank fields
    files = [x.to_data() for x in self.files]
    return {'set': self.dataset_id, "files": files}
  def apply(self, index):
    dataset = index.datasets[self.dataset_id]
    dataset.files.extend(self.files)

  def __str__(self):
    return "[Add {} files to set {}]".format(len(self.files), self.dataset_id)

@handles("addtags")
class AddTagsCommand(Command):
  command = "addtags"
  def __init__(self, dataset, tags=None):
    super(AddTagsCommand, self).__init__()
    self.dataset_id = dataset
    self.tags = set(tags) or {}
  @classmethod
  def from_data(cls, data):
    return cls(data["set"], data["tags"])
  def to_data(self):
    return {"set": self.dataset_id, "tags": list(self.tags)}
  def apply(self, index):
    dataset = index.datasets[self.dataset_id]
    dataset.tags = dataset.tags.union(self.tags)
  def __str__(self):
    return "[Add tags {{{}}} to set {}]".format(", ".join(self.tags), self.dataset_id)

@handles("removetags")
class RemoveTagsCommand(AddTagsCommand):
  command = "removetags"
  def apply(self, index):
    dataset = index.datasets[self.dataset_id]
    dataset.tags = dataset.tags.difference(self.tags)
  def __str__(self):
    return "[Remove tags {{{}}} from set {}]".format(", ".join(self.tags), self.dataset_id)

@handles("setproperty")
class SetPropertyCommand(Command):
  def __init__(self, dataset_id, property, value):
    super(SetPropertyCommand, self).__init__()
    self.set = dataset_id
    # Ensure it is a valid property
    assert property in ["name"]
    self.property = property
    self.value = value
  def apply(self, index):
    dataset = index.datasets[self.set]
    if self.property == "name":
      dataset.name = self.value
  def __str__(self):
    return "[Set {} property {} to {}]".format(self.set, self.property, self.value)
  @classmethod
  def from_data(cls, data):
    return cls(data["set"], data["property"], data["value"])
  def to_data(self):
    return {"set": self.set, "property": self.property, "value":self.value}