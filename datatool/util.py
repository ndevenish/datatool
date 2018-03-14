# coding: utf-8

import glob
import os
import collections

def first(it):
  return next(iter(it),None)

def get_wildcards(file_list):
  """Turns a list of files into a wildcard/list of wildcards."""
  wildcards = []
  # Filter out all separate directories
  files = collections.defaultdict(set)
  for filename in file_list:
    d, f = os.path.split(filename)
    files[d].add(f)

  # Now, process each directory separately
  for dirname, files in files.iteritems():
    extensions = {os.path.splitext(x)[1] for x in files}
    for extension in extensions:
      sublist = {os.path.join(dirname, x) for x in files if x.endswith(extension)}
      real_list = set(glob.glob(os.path.join(dirname, "*"+extension)))
      if sublist == real_list:
        wildcards.append(os.path.join(dirname, "*"+extension))
        continue
      else:
        # Need to narrow down beyond just extension.
        prefix = os.path.commonprefix(files)
        real_list = set(glob.glob(os.path.join(dirname, prefix+"*"+extension)))
        if sublist == real_list:
          wildcards.append(os.path.join(dirname, prefix+"*"+extension))
          continue
        for filename in sublist:
          wildcards.append(os.path.join(dirname, filename))
  return wildcards