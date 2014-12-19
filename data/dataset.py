# coding: utf-8

import uuid

class Dataset(object):
  def __init__(self, setid=None, name=None):
    """Initialise a dataset with a given set of properties"""
    self.id = setid or uuid.uuid4()
    self.name = name
    self.files = []
    self.tags = set()

  def __str__(self):
    return "{" + self.id + "}"

  def __repr__(self):
    return "<Dataset {}:{} files>".format(self.id, len(self.files))


class Datafile(object):
  def __init__(self, path, shasum=None, size=None, timestamp=None):
    self.path = path
    self.shasum = shasum
    self.size = size
    self.timestamp = timestamp
  @classmethod
  def from_data(cls, data):
    return cls(**data)
  def to_data(self):
    return {x:y for x, y in {
      "path": self.path,
      "shasum": self.shasum,
      "size": self.size,
      "timestamp": self.timestamp
    }.items() if y}

  