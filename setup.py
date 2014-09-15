#!/usr/bin/env python
from setuptools import setup, find_packages


VERSION = '0'


long_description = ''

setup(name='banzai',
      version=VERSION,
      packages=find_packages(exclude="tests"),
      author='Thom Neale',
      author_email='twneale@gmail.com',
      license='BSD',
      url='http://github.com/twneale/banzai/',
      description='Tool for pipelining tasks together through a CLI interface',
      long_description=long_description,
      platforms=['any'],
)
