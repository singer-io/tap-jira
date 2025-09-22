#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name="tap-jira",
      version="2.5.0",
      description="Singer.io tap for extracting data from the Jira API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_jira"],
      install_requires=[
          "singer-python==6.0.1",
          "requests==2.32.4",
          "dateparser"
      ],
      extras_require={
          'dev': [
              'pylint',
              'nose2',
              'ipdb'
          ]
      },
      entry_points="""
          [console_scripts]
          tap-jira=tap_jira:main
      """,
      packages=["tap_jira"],
      package_data = {
          "schemas": ["tap_jira/schemas/*.json"]
      },
      include_package_data=True,
)
