#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name="tap-jira",
      version="0.3.3",
      description="Singer.io tap for extracting data from the Jira API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_jira"],
      install_requires=[
          "singer-python==5.4.0",
          "requests==2.20.0",
      ],
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
