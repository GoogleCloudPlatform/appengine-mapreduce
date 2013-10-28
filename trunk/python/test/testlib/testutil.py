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
# pylint: disable=unused-import
from google.appengine.tools import os_compat
# pylint: enable=unused-import

import imp
from testlib import mox
import os
import sys
import unittest

from google.appengine.api import queueinfo
from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import testbed

# TODO(user): Cleanup imports if/when cloudstorage becomes part of runtime.
try:
  # Check if the full cloudstorage package exists. The stub part is in runtime.
  import cloudstorage
  enable_cloudstorage_tests = True
  if hasattr(cloudstorage, "_STUB"):
    cloudstorage = None
except ImportError:
  cloudstorage = None

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
    self.major_version_id = "1"
    self.version_id = self.major_version_id + ".23456789"
    self.module_id = "foo_module"
    self.host = "%s.%s.%s" % (
        self.major_version_id, self.module_id, "testapp.appspot.com")

    self.testbed = testbed.Testbed()
    self.testbed.activate()

    os.environ["APPLICATION_ID"] = self.appid
    os.environ["CURRENT_VERSION_ID"] = self.version_id
    os.environ["CURRENT_MODULE_ID"] = self.module_id
    os.environ["DEFAULT_VERSION_HOSTNAME"] = "%s.appspot.com" % self.appid
    os.environ["HTTP_HOST"] = self.host

    self.testbed.init_app_identity_stub()
    self.testbed.init_blobstore_stub()
    # HRD with no eventual consistency.
    policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=1)
    self.testbed.init_datastore_v3_stub(consistency_policy=policy)
    self.testbed.init_logservice_stub()
    self.testbed.init_files_stub()
    self.testbed.init_memcache_stub()
    self.testbed.init_taskqueue_stub()
    self.testbed.init_urlfetch_stub()

    # For backwards compatibility, maintain easy references to some stubs
    self.taskqueue = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)

    self.taskqueue.queue_yaml_parser = (
        lambda x: queueinfo.LoadSingleQueue(
            "queue:\n"
            "- name: default\n"
            "  rate: 10/s\n"
            "- name: crazy-queue\n"
            "  rate: 2000/d\n"
            "  bucket_size: 10\n"))

  def tearDown(self):
    try:
      self.mox.VerifyAll()
    finally:
      self.mox.UnsetStubs()

    del os.environ["APPLICATION_ID"]
    del os.environ["CURRENT_VERSION_ID"]
    del os.environ["CURRENT_MODULE_ID"]
    del os.environ["DEFAULT_VERSION_HOSTNAME"]
    del os.environ["HTTP_HOST"]

    self.testbed.deactivate()

    unittest.TestCase.tearDown(self)

  def assertTaskStarted(self, queue="default"):
    tasks = self.taskqueue.GetTasks(queue)
    self.assertEquals(1, len(tasks))
    self.assertEquals(tasks[0]["url"], self.MAPREDUCE_URL)


class CloudStorageTestBase(HandlerTestBase):
  """Test base class that ensures cloudstorage library is available."""

  def setUp(self):
    if cloudstorage is None:
      # skipTest is only supported starting in Python 2.7, prior to 2.7
      # the test will result in an error due to the ImportWarning
      if sys.version_info < (2, 7):
        raise ImportWarning("Unable to test Google Cloud Storage. "
                            "Library not found,")
      else:
        self.skipTest("Unable to test Google Cloud Storage. Library not found.")
    super(CloudStorageTestBase, self).setUp()
