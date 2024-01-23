#!/usr/bin/env python

from setuptools import find_packages, setup

setup(
    name="tap-google-search-console",
    version="1.1.0",
    description="Singer.io tap for extracting data from the Google Search Console API",
    author="jeff.huth@bytecode.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_google_search_console"],
    install_requires=["backoff==2.2.1", "requests==2.31.0", "singer-python==6.0.0"],
    extras_require={
        "dev": [
            "ipdb",
            "pylint",
        ]
    },
    entry_points="""
          [console_scripts]
          tap-google-search-console=tap_google_search_console:main
      """,
    packages=find_packages(),
    package_data={"tap_google_search_console": ["schemas/*.json"]},
)
