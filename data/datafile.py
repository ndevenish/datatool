# coding: utf-8

import os
import uuid
import hashlib

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

class FileInstance(object):
  def __init__(self, path, shasum=None, size=None, timestamp=None):
    self.path = path
    self.shasum = shasum
    self.size = size
    self.timestamp = timestamp

  @classmethod
  def from_data(cls, data):
    return cls(**data)

  @classmethod
  def from_file(cls, filename):
    stats = os.stat(filename)
    return FileInstance(filename, shasum=hashfile(filename), 
                    size=stats.st_size, timestamp=stats.st_mtime)

  def to_data(self):
    return {x:y for x, y in {
      "path": self.path,
      "shasum": self.shasum,
      "size": self.size,
      "timestamp": self.timestamp
    }.items() if y}