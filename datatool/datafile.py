# coding: utf-8

import os
import uuid
import hashlib

from .util import first

def hashfile(filename):
  hasher = hashlib.sha1()
  with open(filename, 'rb') as ofile:
    data = ofile.read(4096)
    while data:
      hasher.update(data)
      data = ofile.read(4096)
  return hasher.hexdigest()

class DataFile(object):
  def __init__(self, _id, instances=None):
    self.id = _id
    self.instances = instances or []
    self.tags = set()
    self.attrs = {}

  def can_read(self):
    return any(os.path.isfile(x.filename) for x in self.instances)
  
  def get_valid_instance(self):
    return first([x for x in reversed(self.instances) if os.path.isfile(x.filename)])

class FileInstance(object):
  def __init__(self, filename=None, hashsum=None, size=None, timestamp=None):
    self.filename = filename
    self.hashsum = hashsum
    self.size = int(size) if size is not None else None
    self.timestamp = float(timestamp) if timestamp is not None else None

  @classmethod
  def from_data(cls, data):
    return cls(**data)

  @classmethod
  def from_file(cls, filename):
    stats = os.stat(filename)
    return FileInstance(filename, hashsum=hashfile(filename), 
                    size=stats.st_size, timestamp=stats.st_mtime)

  def to_data(self):
    return {x:y for x, y in {
      "filename": self.filename,
      "hashsum": self.hashsum,
      "size": self.size,
      "timestamp": self.timestamp
    }.items() if y}