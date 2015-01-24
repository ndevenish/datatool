import os
import itertools

from .index import find_index, LocalFileIndex
from .authority import find_authority, LocalFileAuthority
from .util import first

class MissingDatafileError(IOError):
  pass

class SubsetError(IndexError):
  pass

class DataSetFileNavigator(object):
  def __init__(self, dataset, subset, path=None):
    """Initialise for a specific dataset, with subset narrowed"""
    if len(subset) == 0:
      raise SubsetError("Indexed subset contains no entries")
    self._dataset = dataset
    self._subset = subset
    self._path = path or []
    self._tags = set([str(x) for x in itertools.chain(*[x.tags for x in self._subset])])
    self._extensions = [os.path.splitext(x.get_valid_instance().filename)[1].lstrip(".").lower() for x in self._subset if x.get_valid_instance()]

  @property
  def all(self):
    return [x.get_valid_instance().filename for x in self._subset]
  
  @property
  def only(self):
    if len(self._subset) > 1:
      raise SubsetError("More than one entry for selected subset; {}".format(",".join(self.all)))
    return first(self._subset).get_valid_instance().filename

  @property
  def tags(self):
    return self._tags

  def tagged(self, name):
    if name in [x.lower() for x in self._tags]:
      path = self._path + [name]
      return DataSetFileNavigator(self._dataset, [x for x in self._subset if name in [y.lower() for y in x.tags]], path)
    else:
      raise SubsetError("No entries in subset with tag or extension named '{}'".format(attr))
    
  def __getattr__(self, attr):
    """Allow addressing via tag"""
    attr = attr.lower()
    path = self._path + [attr]
    if attr in [x.lower() for x in self._tags]:
      return self.tagged(attr)      
    elif attr in [x.lower() for x in self._extensions]:
      return DataSetFileNavigator(self._dataset, [x for x in self._subset if x.get_valid_instance().filename.lower().endswith(attr)], path)
    else:
      raise SubsetError("No entries in subset with tag or extension named '{}'".format(attr))

  def __str__(self):
    return "<DataSetFileNavigator {}.{}>".format(self._dataset.__display_str__(), ".".join(self._path))
  def __repr__(self):
    return self.__str__()
  def __len__(self):
    return len(self._subset)
  def __iter__(self):
    return iter(self.all)

class DatasetInterface(object):
  """An interface to data sets, to be handed to the python user"""
  def __init__(self, dataset):
    self._dataset = dataset


  def _filenames(self):
    for filei in self._dataset.files:
      # work backwards and find the first valid instance
      valid = filei.get_valid_instance()
      if valid:
        yield valid.filename
      elif not filei.instances:
        raise MissingDatafileError("Missing file and index for ({})".format(filei.id))
      else:
        raise MissingDatafileError("Could not find instance of file in [{}]".format(", ".join(x.filename for x in filei.instances)))

  def __display_str__(self):
    return self._dataset.name or self._Dataset.id[:5]

  def __str__(self):
    return "<Dataset '{}', {} files>".format(self.__display_str__(), len(self._dataset.files))
  def __repr__(self):
    return str(self)
  def __iter__(self):
    return iter(self._filenames())
  def __getattr__(self, name):
    """Access files via tag."""
    return getattr(DataSetFileNavigator(self, self._dataset.files), name)


class Datatool(object):
  def __init__(self):
    self._authority = LocalFileAuthority(find_authority())
    index = LocalFileIndex(find_index())
    self._authority.apply_index(index)

  def get_dataset(self, name_or_id):
    """Retrieves a particular dataset"""
    dset = self._authority.fetch_dataset(name_or_id)
    if not dset:
      raise IndexError("Could not find dataset entry for " + name_or_id)
    return DatasetInterface(dset)

  def get_file(self, name_or_id):
    """Retrieves the single file from a named dataset"""
    dset = self._authority.fetch_dataset(name_or_id)
    if len(dset.files) > 1:
      raise IndexError("More than one file in dataset '{}'".format(name_or_id))
    elif len(dset.files) == 0:
      raise IndexError("No files in dataset '{}'".format(name_or_id))
    return first(DatasetInterface(dset))
