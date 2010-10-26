#!/usr/bin/env python
from distutils.core import setup
from crawle import VERSION

setup(name='CRAWL-E',
      version=VERSION,
      description='Highly distributed web crawling framework',
      author='Bryce Boe',
      author_email='bboe (_at_) cs.ucsb.edu',
      url='http://code.google.com/p/crawl-e',
      py_modules = ['crawle']
      )
