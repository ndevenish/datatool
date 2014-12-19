from distutils.core import setup
import setuptools

setup(name="data",
      version='0.1',
      author="Nicholas Devenish",
      author_email="ndevenish@gmail.com",
      packages=['data'],
      scripts=['bin/data'],
      requires=["docopt", "python-dateutil"],
     )
