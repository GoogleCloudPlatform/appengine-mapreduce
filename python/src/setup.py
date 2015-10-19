#!/usr/bin/env python
# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
    version="1.9.21.0",
    packages=setuptools.find_packages(),
    author="Google App Engine",
    author_email="app-engine-pipeline-api@googlegroups.com",
    keywords="google app engine mapreduce data processing",
    url="https://code.google.com/p/appengine-mapreduce/",
    license="Apache License 2.0",
    description=("Enable MapReduce style data processing on "
                 "App Engine"),
    zip_safe=True,
    # Include package data except README.
    include_package_data=True,
    exclude_package_data={"": ["README"]},
    install_requires=[
        "GoogleAppEngineCloudStorageClient >= 1.9.21",
        "GoogleAppEnginePipeline >= 1.9.21",
        "Graphy >= 1.0.0",
        "simplejson >= 3.6.5",
        "mock >= 1.0.1",
        "mox >= 0.5.3",
        ]
)
