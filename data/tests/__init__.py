# coding: utf-8

import json
from data.authority import StringAuthority
from StringIO import StringIO

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

#def testSimpleParse():
#  idx = Index(StringIO("  DATE COMMAND [ 3 ]\n\n# A test comment\nDATE2 COMMAND2 \"MORE_\\nDATA\"\nDATE3   COMMAND3 {\"some\": \"dslof\", \"data\": 4}"))

def testBasicSetCreate():
  data = StringIO('2014/1/1 createset {"id": "sdsds"}\n2014/2/2 addfiles {"set": "sdsds", "files": [{"path":"/some/file","shasum":"aAAAA"}]}\n2014/3/3 addtags {"set":"sdsds", "tags":["a", "v"]}\n2014/4/4 removetags {"set":"sdsds", "tags":["v"]}')
  idx = StringAuthority(data)
  cid = idx.create_set("newset")
  idx.rename_set(cid, "old_newset")
  logger.debug("Creating set {}".format(cid))
  # Dump out everything
  for cmd in idx._commands:
    logger.debug("{} {} {}".format(cmd.timestamp.isoformat(),cmd.command,json.dumps(cmd.to_data())))
  idx.write(data)
  logger.debug("After write:" + data.getvalue())