
from data import Index
from StringIO import StringIO

import logging
logging.basicConfig(level=logging.DEBUG)

def testSimpleParse():
  idx = Index(StringIO("  DATE COMMAND DATA DATA2\n\n# A test comment\nDATE2 COMMAND2 MORE_DATA\nDATE3   COMMAND3 {some: dslof, data}"))