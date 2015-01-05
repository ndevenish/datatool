from distutils.core import setup
import setuptools

setup(name="datatool",
      version='0.1',
      author="Nicholas Devenish",
      author_email="ndevenish@gmail.com",
      packages=['datatool'],
      scripts=['bin/data'],
      install_requires=["docopt", "python-dateutil"],
     )
