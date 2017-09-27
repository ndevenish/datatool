from distutils.core import setup
import setuptools

setup(name="datatool",
      version='0.1',
      author="Nicholas Devenish",
      author_email="ndevenish@gmail.com",
      packages=['datatool'],
      entry_points = {
        'console_scripts': [
          'data=datatool.main:main',
        ],
    },
      install_requires=["docopt", "python-dateutil"],
     )
