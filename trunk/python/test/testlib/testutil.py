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

"""Test utilities for mapreduce framework.
"""



# Disable "Invalid method name"
# pylint: disable-msg=C6409

# os_compat must be first to ensure timezones are UTC.
# Disable "unused import" and "invalid import order"
# pylint: disable-msg=W0611
from google.appengine.tools import os_compat
# pylint: enable-msg=W0611

import base64
import cgi
from testlib import mox
import os
import re
import shutil
import sys
import tempfile
import unittest
import urllib

from google.appengine.api import apiproxy_stub_map
from google.appengine.api.files import file_service_stub
from google.appengine.api.blobstore import blobstore_stub
from google.appengine.api import datastore_file_stub
from google.appengine.api import queueinfo
from google.appengine.api.blobstore import file_blob_storage
from google.appengine.api.memcache import memcache_stub
from google.appengine.api.taskqueue import taskqueue_stub
from mapreduce import main
from mapreduce import util
from testlib import mock_webapp


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

    self.appid = "testapp"
    os.environ["APPLICATION_ID"] = self.appid

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

    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub("taskqueue", self.taskqueue)
    apiproxy_stub_map.apiproxy.RegisterStub("memcache", self.memcache)
    apiproxy_stub_map.apiproxy.RegisterStub("datastore_v3", self.datastore)
    apiproxy_stub_map.apiproxy.RegisterStub("blobstore", self.blobstore_stub)
    apiproxy_stub_map.apiproxy.RegisterStub(
        "file", file_service_stub.FileServiceStub(blob_storage))

  def tearDown(self):
    shutil.rmtree(self.blob_storage_directory)
    unittest.TestCase.tearDown(self)

  def assertTaskStarted(self, queue="default"):
    tasks = self.taskqueue.GetTasks(queue)
    self.assertEquals(1, len(tasks))
    self.assertEquals(tasks[0]["url"], self.MAPREDUCE_URL)


def decode_task_payload(task):
  """Decodes POST task payload.

  Args:
    task: a task to decode its payload.

  Returns:
    parameter_name -> parameter_value dict. If multiple parameter values are
    present, then parameter_value will be a list.
  """
  body = task["body"]
  if not body:
    return {}
  decoded = base64.b64decode(body)
  result = {}
  for (name, value) in cgi.parse_qs(decoded).items():
    if len(value) == 1:
      result[name] = value[0]
    else:
      result[name] = value
  return util.HugeTask.decode_payload(result)


def execute_task(task, handlers_map=None):
  """Execute mapper's executor task.

  This will try to determine the correct mapper handler for the task, will set
  up all mock environment necessary for task execution, and execute the task
  itself.

  This function can be used for functional-style testing of functionality
  depending on mapper framework.
  """
  if not handlers_map:
    handlers_map = main.create_handlers_map()

  url = task["url"]
  handler = None

  for (re_str, handler_class) in handlers_map:
    if re.match(re_str, url):
      handler = handler_class()
      break

  if not handler:
    raise Exception("Can't determine handler for %s" % task)

  handler.initialize(mock_webapp.MockRequest(),
                     mock_webapp.MockResponse())
  handler.request.set_url(url)

  for k, v in task["headers"]:
    handler.request.headers[k] = v
    environ_key = "HTTP_" + k.replace("-", "_").upper()
    handler.request.environ[environ_key] = v
  handler.request.environ["HTTP_X_APPENGINE_TASKNAME"] = task["name"]
  handler.request.environ["HTTP_X_APPENGINE_QUEUENAME"] = task["queue_name"]
  handler.request.environ["PATH_INFO"] = handler.request.path

  saved_os_environ = os.environ
  try:
    os.environ = dict(os.environ)
    os.environ.update(handler.request.environ)
    if task["method"] == "POST":
      for k, v in decode_task_payload(task).items():
        handler.request.set(k, v)
      handler.post()
    elif task["method"] == "GET":
      handler.get()
    else:
      raise Exception("Unsupported method: %s" % task.method)
  finally:
    os.environ = saved_os_environ

  if handler.response.status != 200:
    raise Exception("Handler failure: %s (%s). \nTask: %s\nHandler: %s" %
                    (handler.response.status,
                     handler.response.status_message,
                     task,
                     handler))


def execute_all_tasks(taskqueue, queue="default", handlers_map=None):
  """Run and remove all tasks in the taskqueue.

  Args:
    taskqueue: An instance of taskqueue stub.
    queue: Queue name to run all tasks from.
  """
  tasks = taskqueue.GetTasks(queue)
  taskqueue.FlushQueue(queue)
  for task in tasks:
    execute_task(task,handlers_map=handlers_map)

def execute_until_empty(taskqueue, queue="default", handlers_map=None):
  """Execute taskqueue tasks until it becomes empty.

  Args:
    taskqueue: An instance of taskqueue stub.
    queue: Queue name to run all tasks from.
  """
  while taskqueue.GetTasks(queue):
    execute_all_tasks(taskqueue, queue, handlers_map)
