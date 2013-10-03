#!/usr/bin/env python
"""Setup specs for packaging, distributing, and installing MR lib."""

import distribute_setup
# User may not have setuptools installed on their machines.
# This script will automatically install the right version from PyPI.
distribute_setup.use_setuptools()


# pylint: disable=g-import-not-at-top
import setuptools


# To debug, set DISTUTILS_DEBUG env var to anything.
setuptools.setup(
    name="GoogleAppEngineMapReduce",
    version="1.8.3.2",
    packages=setuptools.find_packages(),
    author="Google App Engine",
    author_email="app-engine-pipeline-api@googlegroups.com",
    keywords="google app engine mapreduce data processing",
    url="https://code.google.com/p/appengine-mapreduce/",
    license="Apache License 2.0",
    description=("Enable MapReduce style data processing on "
                 "App Engine"),
    zip_safe=True,
    # Exclude these files from installation.
    exclude_package_data={"": ["README"]},
    install_requires=["GoogleAppEngineCloudStorageClient >= 1.8.3"]
)
