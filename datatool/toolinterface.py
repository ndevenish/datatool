import os

from .index import find_index, LocalFileIndex
from .authority import find_authority, LocalFileAuthority
from .util import first

class MissingDatafileError(IOError):
  pass

class DatasetInterface(object):
  """An interface to data sets, to be handed to the python user"""
  def __init__(self, dataset):
    self._dataset = dataset

  def _filenames(self):
    for filei in self._dataset.files:
      # work backwards and find the first valid instance
      valid = first([x for x in reversed(filei.instances) if os.path.isfile(x.filename)])
      if valid:
        yield valid.filename
      elif not filei.instances:
        raise MissingDatafileError("Missing file and index for ({})".format(filei.id))
      else:
        raise MissingDatafileError("Could not find instance of file in [{}]".format(", ".join(x.filename for x in filei.instances)))

  def __str__(self):
    return "<Dataset '{}', {} files>".format(self._dataset.name or self._Dataset.id[:5], len(self._dataset.files))
  def __repr__(self):
    return str(self)

  def __iter__(self):
    return iter(self._filenames())

class Datatool(object):
  def __init__(self):
    self._authority = LocalFileAuthority(find_authority())
    index = LocalFileIndex(find_index())
    self._authority.apply_index(index)

  def get_dataset(self, name_or_id):
    """Retrieves a particular dataset"""
    dset = self._authority.fetch_dataset(name_or_id)
    return DatasetInterface(dset)

  def get_file(self, name_or_id):
    """Retrieves the single file from a named dataset"""
    dset = self._authority.fetch_dataset(name_or_id)
    if len(dset.files) > 1:
      raise IndexError("More than one file in dataset '{}'".format(name_or_id))
    elif len(dset.files) == 0:
      raise IndexError("No files in dataset '{}'".format(name_or_id))
    return first(DatasetInterface(dset))
