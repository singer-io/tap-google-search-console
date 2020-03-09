#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-google-search-console',
      version='0.0.5',
      description='Singer.io tap for extracting data from the Google Search Console API',
      author='jeff.huth@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_google_search_console'],
      install_requires=[
          'backoff==1.8.0',
          'requests==2.22.0',
          'singer-python==5.8.1'
      ],
      entry_points='''
          [console_scripts]
          tap-google-search-console=tap_google_search_console:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_google_search_console': [
              'schemas/*.json'
          ]
      })
