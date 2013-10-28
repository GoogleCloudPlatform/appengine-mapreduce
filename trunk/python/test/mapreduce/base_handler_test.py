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




import httplib

import unittest

from mapreduce import base_handler
from mapreduce import errors
from mapreduce import parameters
from mapreduce import util
from google.appengine.ext.webapp import mock_webapp


# pylint: disable=g-import-not-at-top
# TODO(user): Cleanup imports if/when cloudstorage becomes part of runtime.
try:
  from cloudstorage import api_utils
  enable_cloudstorage_tests = True
except ImportError:
  enable_cloudstorage_tests = False

# pylint: disable=g-bad-name


class BaseHandlerTest(unittest.TestCase):
  """Tests for BaseHandler."""

  def setUp(self):
    self.handler = base_handler.BaseHandler()
    self.handler.initialize(mock_webapp.MockRequest(),
                            mock_webapp.MockResponse())

  def testBasePath(self):
    """Test base_path calculation."""
    self.handler.request.path = "/mapreduce_base/start"
    self.assertEquals("/mapreduce_base", self.handler.base_path())

    self.handler.request.path = "/start"
    self.assertEquals("", self.handler.base_path())

    self.handler.request.path = "/map/reduce/base/start"
    self.assertEquals("/map/reduce/base", self.handler.base_path())


class TaskQueueHandlerTest(unittest.TestCase):
  """Tests for TaskQueueHandler."""

  def setUp(self):
    self.handler = base_handler.TaskQueueHandler()
    self.request = mock_webapp.MockRequest()
    self.request.headers["X-AppEngine-QueueName"] = "default"
    self.request.headers["X-AppEngine-TaskName"] = "task_name"

  def init(self):
    self.handler.initialize(self.request,
                            mock_webapp.MockResponse())

  def testDefaultRetryParams(self):
    if not enable_cloudstorage_tests:
      return
    self.assertTrue(api_utils._get_default_retry_params().save_access_token)

  def testPostNoTaskQueueHeader(self):
    """Test calling post() without valid taskqueue header."""
    del self.request.headers["X-AppEngine-QueueName"]
    self.init()
    self.handler.post()
    self.assertEquals(httplib.FORBIDDEN, self.handler.response.status)

  def testTaskRetryCount(self):
    self.init()
    self.assertEquals(0, self.handler.task_retry_count())

    self.request.headers["X-AppEngine-TaskExecutionCount"] = 5
    self.init()
    self.assertEquals(5, self.handler.task_retry_count())


class FaultyTaskQueueHandler(base_handler.TaskQueueHandler):
  """A handler that always fails at _preprocess."""

  dropped = False
  handled = False

  def _preprocess(self):
    raise Exception()

  def _drop_gracefully(self):
    self.dropped = True

  def handle(self):
    self.handled = True

  @classmethod
  def reset(cls):
    cls.dropped = False
    cls.handled = False


class FaultyTaskQueueHandlerTest(unittest.TestCase):

  def setUp(self):
    FaultyTaskQueueHandler.reset()
    self.handler = FaultyTaskQueueHandler()
    self.request = mock_webapp.MockRequest()
    self.request.headers["X-AppEngine-QueueName"] = "default"
    self.request.headers["X-AppEngine-TaskName"] = "task_name"
    self.request.headers[util._MR_ID_TASK_HEADER] = "mr_id"

  def init(self):
    self.handler.initialize(self.request,
                            mock_webapp.MockResponse())

  def testSmoke(self):
    self.init()
    self.handler.post()
    self.assertTrue(self.handler.dropped)
    self.assertFalse(self.handler.handled)
    self.assertEqual(httplib.OK, self.handler.response.status)

  def testTaskRetriedTooManyTimes(self):
    self.request.headers["X-AppEngine-TaskExecutionCount"] = (
        parameters.config.TASK_MAX_ATTEMPTS)
    self.init()
    self.handler.post()
    self.assertTrue(self.handler.dropped)
    self.assertFalse(self.handler.handled)
    self.assertEqual(httplib.OK, self.handler.response.status)


class JsonErrorHandler(base_handler.JsonHandler):
  """JsonHandler that raises an error when invoked."""

  def __init__(self, error):
    """Constructor.

    Args:
      error The error to raise when handle() is called.
    """
    self.error = error
    self.json_response = {}

  def handle(self):
    """Raise an error."""
    raise self.error


class JsonHandlerTest(unittest.TestCase):
  """Tests for JsonHandler."""

  def setUp(self):
    self.handler = base_handler.JsonHandler()
    self.handler.initialize(mock_webapp.MockRequest(),
                            mock_webapp.MockResponse())

  def testBasePath(self):
    """Test base_path calculation."""
    self.handler.request.path = "/mapreduce_base/start"
    self.assertRaises(base_handler.BadRequestPathError,
                      self.handler.base_path)

    self.handler.request.path = "/mapreduce_base/command/start"
    self.assertEquals("/mapreduce_base", self.handler.base_path())

    self.handler.request.path = "/command/start"
    self.assertEquals("", self.handler.base_path())

    self.handler.request.path = "/map/reduce/base/command/start"
    self.assertEquals("/map/reduce/base", self.handler.base_path())

  def testMissingYamlError(self):
    """Test that this error sets the expected response fields."""
    handler = JsonErrorHandler(errors.MissingYamlError)
    request = mock_webapp.MockRequest()
    response = mock_webapp.MockResponse()
    request.headers["X-Requested-With"] = "XMLHttpRequest"
    handler.initialize(request, response)

    handler._handle_wrapper()
    self.assertEquals("Notice", handler.json_response["error_class"])
    self.assertEquals("Could not find 'mapreduce.yaml'",
                      handler.json_response["error_message"])

  def testError(self):
    """Test that an error sets the expected response fields."""
    handler = JsonErrorHandler(Exception('bill hicks'))
    request = mock_webapp.MockRequest()
    response = mock_webapp.MockResponse()
    request.headers["X-Requested-With"] = "XMLHttpRequest"
    handler.initialize(request, response)

    handler._handle_wrapper()
    self.assertEquals("Exception", handler.json_response["error_class"])
    self.assertEquals("bill hicks",
                      handler.json_response["error_message"])


if __name__ == '__main__':
  unittest.main()
