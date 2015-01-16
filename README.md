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

Usage
=====

Creating data sets is simple:

    $ data set create --name=sampleset ./samples/sample*.root
    


Python Interface
================


Configuration
=============