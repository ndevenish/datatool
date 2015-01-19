Datatool
========

A utility to manage and coordinate access to consistent data sets across
multiple machine instances and (remote, local) filesystems.

The tool maintains separate files for authority (which record unique file 
instances, and groups these files into datasets) and index (lists the files
accessible locally and their locations). By accessing the same authority
from several machines with separate indices, any analysis code needs not know
the exact filesystem location for all data files, while still guaranteeing
that the exact correct files are used.

By requiring that files are explicitly indexed, each file can be verified by
hash, timestamp and modification date, so that if the files change without the
index being notified, then a data validation error can be thrown.


Usage
=====

    data [options] set create [--name=<name>] <file> [<file>...]
    data [options] set addfiles <name-or-id> <file> [<file>...]
    data [options] tag [-d] (<name-or-id-or-file>) <tag> [<tag>...]
    data [options] tag [-d] --tag=<tag> [--tag=<tag>...] <name-or-id-or-file>...
    data [options] index <file> [<file>...]
    data [options] files <name-or-id>
    data [options] search <tag> [<tag>...]
    data [options] identify <file> [<file>...]
    data [options] sets

Creating data sets is simple:

    $ data set create --name=sampleset /data/samples/sample*.data
    b1a99207c91b4bc5a5101677a4fa1b0b

As are listing the available sets:

    $ data sets
    b1a99207c91b4bc5a5101677a4fa1b0b  sampleset           6 files

Sets can be retrieved/inspected by name or (partial) ID:
    
    $ data files b1a99   --or--  $ data files sampleset
    /data/samples/sample1.data
    /data/samples/sample2.data
    /data/samples/sample3.data
    /data/samples/sampleA.data
    /data/samples/sampleB.data
    /data/samples/sampleC.data

Files within sets can be tagged:
    
    $ data tag --tag=numeric /data/samples/sample[123].*
    $ data tag --tag=alpha /data/samples/sample[ABC].*
    $ data tag /data/samples/sample1.data set1
    $ data files sampleset
    /data/samples/sample1.data            Tags: numeric, set1
    /data/samples/sample2.data            Tags: numeric
    /data/samples/sample3.data            Tags: numeric
    /data/samples/sampleA.data            Tags: alpha
    /data/samples/sampleB.data            Tags: alpha
    /data/samples/sampleC.data            Tags: alpha

As can sets themselves:

    $ data tag sampleset sample fake example
    $ data sets
    b1a99207c91b4bc5a5101677a4fa1b0b  sampleset           6 files  Tags: example, fake, sample  

Python Interface
================

Setting up to access the data is simple:

    from datatool import Datatool
    datatool = Datatool()
    sample = datatool.get_dataset("sampleset")

And accessing files in this dataset is also simple:

    >>> sample.all
    ['/data/samples/sample1.data', '/data/samples/sample2.data', '/data/samples/sample3.data', 
    '/data/samples/sampleA.data', '/data/samples/sampleB.data', '/data/samples/sampleC.data']
    >>> for filename in sample:
    ...   print (filename)
    /data/samples/sample2.data
    /data/samples/sample2.data
    /data/samples/sample3.data
    /data/samples/sampleA.data
    /data/samples/sampleB.data
    /data/samples/sampleC.data

As well as accessing subsets of a single dataset, by tag:

    >>> sample.tagged("alpha").all
    ['/data/samples/sampleA.data', '/data/samples/sampleB.data', '/data/samples/sampleC.data']

Or directly by attribute, to narrow down tags:
    
    >>> sample.alpha.all
    ['/data/samples/sampleA.data', '/data/samples/sampleB.data', '/data/samples/sampleC.data']

Or to get a single file, if you are expecting one (if more than one file is found,
then an error is thrown):

    >>> sample.set1.only
    '/data/samples/sample1.data'

Along with tags, you can use attribute-access to narrow down file extension,
e.g. if you have a dataset:

    /data/samples/sampleA.data
    /data/samples/sampleA.altdata

You can narrow down by extension:

    >>> sample.data.only
    '/data/samples/sampleA.data'

    >>> sample.altdata.only
    '/data/samples/sampleA.altdata'

Future Plans
============
- Authority data accessible in a form other than local file (e.g. cached remote
  or some sort of repository)
- Multiple index files, so that you could have local, user-specific indices
- Date-range specification - so that you can retrieve a data set specified at
  some particular point in the past. The data-system is designed around this
  being implemented at some future point.
- Subsets: So a single dataset can be a small collection of files if needed,
  but can also reference a large collection. This would also allow sanity with
  e.g. asserting that all retrieved files are a complete, single set