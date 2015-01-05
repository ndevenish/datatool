# coding: utf-8

import os
import uuid
import hashlib

class Dataset(object):
  def __init__(self, setid=None):
    """Initialise a dataset with a given set of properties"""
    self.id = setid or uuid.uuid4()
    self.files = []
    self.tags = set()
    self.attrs = {}

  @property
  def name(self):
      return self.attrs.get("name")
  
  def __str__(self):
    return "{" + self.id + "}"

  def __repr__(self):
    return "<Dataset {}:{} files, [{}]>".format(self.id, len(self.files), ",".join(self.tags))


  