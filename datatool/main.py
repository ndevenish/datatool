# coding: utf-8

"""Manage data sets and locations.

Usage:
  data [options] set create [--name=<name>] <file> [<file>...]
  data [options] set addfiles <name-or-id> <file> [<file>...]
  data [options] set rmfiles <name-or-id> <file-or-hash> [<file-or-hash>...]
  data [options] set delete <name-or-id>
  data [options] set rename <name-or-id> <name>
  data [options] tag [-d] (<name-or-id-or-file>) <tag> [<tag>...]
  data [options] tag [-d] --tag=<tag> [--tag=<tag>...] <name-or-id-or-file>...
  data [options] index <file> [<file>...]
  data [options] files [--wildcard] <name-or-id> [<tag> [<tag>...]]
  data [options] search <tag> [<tag>...]
  data [options] identify <file> [<file>...]
  data [options] sets [--all]

Options:
  --authority=<auth>  Use a specific data authority
  --index=<index>     Use a specific data index
  --name=<name>       Give a data set a name when creating
  -t, --tag=<tag>     Explicitly specify tags when adding to multiple items
  -d, --delete        Remove given tags from a dataset instead of adding
  -1                  Output only one (filename, set) per line. For parsing.
  -w, --wildcard      Attempt to output filenames as wildcards
  -a, --all           Show all entries, even empty ones

Commands:
  set           Manipulate and create data sets
  set create    Create a new data set, optionally named, with a file list
  set addfiles  Add a set of files to a dataset
  set rmfiles   Remove files from a dataset
  set delete    Remove a dataset.
  set rename    Name, or rename, a dataset
  tag           Add a tag (or list of tags) to a dataset, or a file, or several
  index         Explicitly add a set of files to the index
  files         Retrieve the file list for a specific data set
  search        Find a list of dataset names matching a list of tags
  identify      Find any datasets containing any given files
  sets          List all non-empty data sets
"""

from __future__ import print_function
import sys, os
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, stream=sys.stderr)

from docopt import docopt

from .index import find_index, LocalFileIndex
from .authority import find_authority, LocalFileAuthority
from .util import first, get_wildcards
from .datafile import FileInstance

class ArgumentError(RuntimeError):
  pass

def find_sources(authority=None, index=None):
  if not authority:
    authority = find_authority()
  if not (authority):
    logger.error("No data authority specified. Please set DATA_AUTHORITY or pass in with --authority")
    sys.exit(2)
  if not index:
    index = find_index()
  if not (index):
    logger.error("No data index specified. Please set DATA_INDEX or pass in with --index")
    sys.exit(2)
  return (authority, index)

def print_sets(sets):
  if len(sets) == 0:
    print("(no sets)")
    return
  nameLen = max(len(x.name) for x in sets)
  lenlen = max(len(str(len(x.files))) for x in sets)
  for dataSet in sets:
    tagMessage = ""
    if dataSet.tags:
      tagMessage = "Tags: {}".format(", ".join(dataSet.tags))
    print ("{}  {} {} {} files  {}".format(dataSet.id, (dataSet.name or "").ljust(nameLen),
      "(no read)" if not dataSet.can_read() else " "*9, str(len(dataSet.files)).rjust(lenlen),
      tagMessage))

def main(argv):
  args = docopt(__doc__, argv=argv[1:])

  # Find the data index file
  authority_name, index_name = find_sources(args["--authority"], args["--index"])
  authority = LocalFileAuthority(authority_name)
  index = LocalFileIndex(index_name)
  authority.apply_index(index)

  if args["set"]:
    process_set(args, authority, index)
  elif args["index"]:
    for f in args["<file>"]:
      logger.info("Indexing {}".format(f))
      index.add_files([f])
  elif args["files"]:
    dataset = authority.fetch_dataset(args['<name-or-id>'])
    tagfilter = set(x.lower() for x in args["<tag>"])
    entries = []
    for datafile in dataset.files:
      # Find the last instance that exists
      valid = datafile.get_valid_instance()
      # Check that this file contains all the tags passed in 
      if not tagfilter.issubset(set(x.lower() for x in datafile.tags)):
        continue
      if valid:
        entries.append((valid.filename, "", datafile.tags))
      elif not datafile.instances:
        entries.append((datafile.id, "(no meta)", datafile.tags))
      else:
        entries.append((first(datafile.instances).filename, "(no read)", datafile.tags))
    if entries:
      if args["--wildcard"]:
        # Only use filenames, and reduce the list
        reduced_entries = get_wildcards(x for x,_,_ in entries)
        print ("\n".join(reduced_entries))
      else:
        nameLen = max(len(x[0]) for x in entries)
        for name, msg, tags in entries:
          tagtext = ""
          if args["-1"]:
            print (name)
          else:
            if tags:
              tagtext = "Tags: " + ", ".join(tags)
            print ("{} {}  {}".format(name.ljust(nameLen), msg.ljust(9), tagtext))

  elif args["search"]:
    sets = [authority.fetch_dataset(x) for x in authority.search(args["<tag>"])]
    logger.info("{} results".format(len(sets)))
    print_sets(sets)

  elif args["identify"]:
    assert False
  elif args["sets"]:
    sets = authority._data.datasets.values()
    if not args["--all"]:
      sets = [x for x in sets if x.files]
    print_sets(sets)
  elif args["tag"]:
    tagees = args["<name-or-id-or-file>"]
    tags = set(args["--tag"]).union(args["<tag>"])
    for tageeName in tagees:
      tagee = first([x for x in authority._data.values() if x.id.startswith(tageeName)])
      if not tagee:
        tagee = authority.fetch_dataset(tageeName)
      if not tagee:
        # Look for this in the index
        fileEntry = index.fetch_file(tageeName)
        if fileEntry:
          tagee = authority._data.get(fileEntry.hashsum)
      if not tagee:
        logger.error("Could not find entry from criteria '{}'".format(tageeName))
        return 1

      if args["--delete"]:
        authority.remove_tags(tagee.id, tags)
      else:
        authority.add_tags(tagee.id, tags)


  # Write any changes to the index
  authority.write()
  index.write()
  return 0

def process_set(args, authority, index):
  if args["create"]:
    set_id = authority.create_set(name=args["--name"])
    if args["<file>"]:
      # Make sure these are added to the index
      files = list(index.add_files(args["<file>"]))
      authority.add_files(set_id, files)
    print (set_id)
  elif args["addfiles"]:
    dataset = authority.fetch_dataset(args['<name-or-id>'])
    files = list(index.add_files(args["<file>"]))
    authority.add_files(dataset.id, files)
  elif args["rmfiles"]:
    #   data [options] set rmfiles <name-or-id> <file-or-hash> [<file-or-hash>...]
    dataset = authority.fetch_dataset(args['<name-or-id>'])
    hashesToRemove = []
    for toRemove in args["<file-or-hash>"]:
      # Find the file instance and remove it
      # Look for this file already
      filei = index.fetch_file(toRemove)
      if filei in dataset.files:
        hashesToRemove.append(filei.id)
      elif os.path.isfile(toRemove):
        # Harder case: Hash the file if it exists
        instance = FileInstance.from_file(toRemove)
        # Is this in the dataset?
        results = [x for x in dataset.files if x.id == instance.hashsum]
        for res in results:
          logger.debug("Removing file {}".format(res))
          hashesToRemove.append(res.id)
      else:
        # Hardest case: No file on disk. remove from instance location.
        logger.warn("Removing file {} from instance location only".format(toRemove))
        assert false
        # (needs more thought)
    authority.remove_files(dataset.id, hashesToRemove)

  elif args["delete"]:
    # data [options] set delete <name-or-id>
    dataset = authority.fetch_dataset(args['<name-or-id>'])
    if not dataset:
      #logger.error("Dataset {} does not exist!".format(args["<name-or-id>"]))
      raise ArgumentError("Dataset {} does not exist!".format(args["<name-or-id>"]))
    authority.delete_set(dataset.id)
  elif args["rename"]:
    dataset = authority.fetch_dataset(args['<name-or-id>'])
    authority.rename_set(dataset.id, args["<name>"])
  else:
    raise RuntimeError("Unhandled set command!")
