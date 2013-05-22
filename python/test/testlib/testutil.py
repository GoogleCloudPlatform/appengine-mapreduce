#!/usr/bin/env python
#
# Copyright 2010 Google Inc.
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

"""Test utilities for mapreduce framework."""



# pylint: disable=g-bad-name
# pylint: disable=g-import-not-at-top
# pylint: disable=invalid-import-order

# os_compat must be first to ensure timezones are UTC.
# Disable "unused import" and "invalid import order"
# pylint: disable-msg=W0611
from google.appengine.tools import os_compat
# pylint: enable-msg=W0611

import imp
from testlib import mox
import os
import shutil
import sys
import tempfile
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api.files import file_service_stub
from google.appengine.api.blobstore import blobstore_stub
from google.appengine.api import datastore_file_stub
from google.appengine.api import queueinfo
from google.appengine.api.blobstore import file_blob_storage
from google.appengine.api.memcache import memcache_stub
from google.appengine.api.taskqueue import taskqueue_stub

# pylint: disable=unused-import
try:
  import mock
except ImportError, e:
  _NAME = os.environ.get("ROOT_PACKAGE_NAME")
  if not _NAME:
    raise e
  mod = sys.modules.setdefault(_NAME, imp.new_module(_NAME))
  mod.__path__ = [os.environ["ROOT_PACKAGE_PATH"]]
  import mock


class MatchesDatastoreConfig(mox.Comparator):
  """Mox comparator for MatchesDatastoreConfig objects."""

  def __init__(self, **kwargs):
    self.kwargs = kwargs

  def equals(self, config):
    """Check to see if config matches arguments."""
    if self.kwargs.get("deadline", None) != config.deadline:
      return False
    if self.kwargs.get("force_writes", None) != config.force_writes:
      return False
    return True

  def __repr__(self):
    return "MatchesDatastoreConfig(%s)" % self.kwargs


class MatchesUserRPC(mox.Comparator):
  """Mox comparator for UserRPC objects."""

  def __init__(self, **kwargs):
    self.kwargs = kwargs

  def equals(self, rpc):
    """Check to see if rpc matches arguments."""
    if self.kwargs.get("deadline", None) != rpc.deadline:
      return False
    return True

  def __repr__(self):
    return "MatchesUserRPC(%s)" % self.kwargs


class HandlerTestBase(unittest.TestCase):
  """Base class for all webapp.RequestHandler tests."""

  MAPREDUCE_URL = "/_ah/mapreduce/kickoffjob_callback"

  def setUp(self):
    unittest.TestCase.setUp(self)
    self.mox = mox.Mox()

    self.appid = "testapp"
    self.version_id = "1.23456789"
    os.environ["APPLICATION_ID"] = self.appid
    os.environ["CURRENT_VERSION_ID"] = self.version_id
    os.environ["HTTP_HOST"] = "localhost"

    self.memcache = memcache_stub.MemcacheServiceStub()
    self.taskqueue = taskqueue_stub.TaskQueueServiceStub()
    self.taskqueue.queue_yaml_parser = (
        lambda x: queueinfo.LoadSingleQueue(
            "queue:\n"
            "- name: default\n"
            "  rate: 10/s\n"
            "- name: crazy-queue\n"
            "  rate: 2000/d\n"
            "  bucket_size: 10\n"))
    self.datastore = datastore_file_stub.DatastoreFileStub(
        self.appid, "/dev/null", "/dev/null")

    self.blob_storage_directory = tempfile.mkdtemp()
    blob_storage = file_blob_storage.FileBlobStorage(
        self.blob_storage_directory, self.appid)
    self.blobstore_stub = blobstore_stub.BlobstoreServiceStub(blob_storage)
    self.file_service = self.createFileServiceStub(blob_storage)

    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub("taskqueue", self.taskqueue)
    apiproxy_stub_map.apiproxy.RegisterStub("memcache", self.memcache)
    apiproxy_stub_map.apiproxy.RegisterStub("datastore_v3", self.datastore)
    apiproxy_stub_map.apiproxy.RegisterStub("blobstore", self.blobstore_stub)
    apiproxy_stub_map.apiproxy.RegisterStub("file", self.file_service)

  def createFileServiceStub(self, blob_storage):
    return file_service_stub.FileServiceStub(blob_storage)

  def tearDown(self):
    try:
      self.mox.VerifyAll()
    finally:
      self.mox.UnsetStubs()
      shutil.rmtree(self.blob_storage_directory)
    unittest.TestCase.tearDown(self)

  def assertTaskStarted(self, queue="default"):
    tasks = self.taskqueue.GetTasks(queue)
    self.assertEquals(1, len(tasks))
    self.assertEquals(tasks[0]["url"], self.MAPREDUCE_URL)
