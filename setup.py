#!/usr/bin/env python
from setuptools import setup, find_packages
from pupa import __version__


long_description = ''

setup(name='banzai',
      version=__version__,
      packages=find_packages(),
      author='Thom Neale',
      author_email='twneale@gmail.com',
      license='BSD',
      url='http://github.com/twneale/banzai/',
      description='Tool for pipelining tasks together through a CLI interface',
      long_description=long_description,
      platforms=['any'],
      install_requires=[
        'cement==2.2.0'
      ]
)
