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




# pylint: disable=g-bad-name

import datetime
import os
import unittest

from google.appengine.api import taskqueue
from mapreduce import model
from mapreduce import parameters
from mapreduce import util


class TestHandler(object):
  """Test handler class."""

  def __call__(self, entity):
    pass

  def process(self, entity):
    pass

  @staticmethod
  def process2(entity):
    pass

  @classmethod
  def process3(cls):
    pass


def test_handler_function(entity):
  """Empty test handler function."""
  pass


class TestHandlerWithArgs(object):
  """Test handler with argument in constructor."""

  def __init__(self, arg_unused):
    """Constructor."""
    pass

  def process(self, entity):
    """Empty process function."""
    pass


# pylint: disable=g-old-style-class
class TestHandlerOldStyle():
  """Old style class."""

  def __call__(self, entity):
    pass


def test_handler_yield(entity):
  """Yielding handler function."""
  yield 1
  yield 2


class MockMapreduceSpec:
  """Mock MapreduceSpec class."""

  def __init__(self):
    self.params = {}


class ForNameTest(unittest.TestCase):
  """Test util.for_name function."""

  def testClassName(self):
    """Test passing fq class name."""
    self.assertEquals(TestHandler, util.for_name("__main__.TestHandler"))

  def testFunctionName(self):
    """Test passing function name."""
    self.assertEquals(test_handler_function,
                      util.for_name("__main__.test_handler_function"))

  def testMethodName(self):
    """Test passing method name."""
    self.assertEquals(TestHandler.process,
                      util.for_name("__main__.TestHandler.process"))

  def testClassWithArgs(self):
    """Test passing method name of class with constructor args."""
    self.assertEquals(TestHandlerWithArgs.process,
                      util.for_name("__main__.TestHandlerWithArgs.process"))

  def testBadModule(self):
    """Tests when the module name is bogus."""
    try:
      util.for_name("this_is_a_bad_module_name.stuff")
    except ImportError, e:
      self.assertEquals(
          "Could not find 'stuff' on path 'this_is_a_bad_module_name'",
          str(e))
    else:
      self.fail("Did not raise exception")

  def testBadFunction(self):
    """Tests when the module name is good but the function is missing."""
    try:
      util.for_name("__main__.does_not_exist")
    except ImportError, e:
      self.assertEquals(
          "Could not find 'does_not_exist' on path '__main__'",
          str(e))
    else:
      self.fail("Did not raise exception")

  def testBadClass(self):
    """Tests when the class is found but the function name is missing."""
    try:
      util.for_name("__main__.TestHandlerWithArgs.missing")
    except ImportError, e:
      self.assertEquals(
          "Could not find 'missing' on path '__main__.TestHandlerWithArgs'",
          str(e))
    else:
      self.fail("Did not raise exception")

  def testGlobalName(self):
    """Tests when the name has no dots in it."""
    try:
      util.for_name("this_is_a_bad_module_name")
    except ImportError, e:
      self.assertTrue(str(e).startswith(
          "Could not find 'this_is_a_bad_module_name' on path "))
    else:
      self.fail("Did not raise exception")


class TestGetQueueName(unittest.TestCase):

  def testGetQueueName(self):
    self.assertEqual("foo", util.get_queue_name("foo"))

    os.environ["HTTP_X_APPENGINE_QUEUENAME"] = "foo"
    self.assertEqual("foo", util.get_queue_name(None))

    os.environ["HTTP_X_APPENGINE_QUEUENAME"] = "__cron"
    self.assertEqual(parameters.config.QUEUE_NAME, util.get_queue_name(None))


class SerializeHandlerTest(unittest.TestCase):
  """Test util.try_*serialize_handler works on various types."""

  def testNonSerializableTypes(self):
    # function.
    self.assertEquals(None, util.try_serialize_handler(test_handler_function))
    # Unbound method.
    self.assertEquals(None, util.try_serialize_handler(TestHandler.process))
    # bounded method.
    self.assertEquals(None, util.try_serialize_handler(TestHandler().process))
    # class method.
    self.assertEquals(None, util.try_serialize_handler(TestHandler.process3))
    # staticmethod, which is really a function.
    self.assertEquals(None, util.try_serialize_handler(TestHandler.process2))

  def testSerializableTypes(self):
    # new style callable instance.
    i = TestHandler()
    self.assertNotEquals(
        None, util.try_deserialize_handler(util.try_serialize_handler(i)))

    i = TestHandlerOldStyle()
    self.assertNotEquals(
        None, util.try_deserialize_handler(util.try_serialize_handler(i)))


class IsGeneratorFunctionTest(unittest.TestCase):
  """Test util.is_generator function."""

  def testGenerator(self):
    self.assertTrue(util.is_generator(test_handler_yield))

  def testNotGenerator(self):
    self.assertFalse(util.is_generator(test_handler_function))


class GetTaskHeadersTest(unittest.TestCase):

  def setUp(self):
    super(GetTaskHeadersTest, self).setUp()
    os.environ["CURRENT_VERSION_ID"] = "v7.1"
    os.environ["CURRENT_MODULE_ID"] = "foo-module"
    os.environ["DEFAULT_VERSION_HOSTNAME"] = "foo.appspot.com"

  def testGetTaskHost(self):
    self.assertEqual("v7.foo-module.foo.appspot.com", util._get_task_host())
    task = taskqueue.Task(url="/relative_url",
                          headers={"Host": util._get_task_host()})
    self.assertEqual("v7.foo-module.foo.appspot.com",
                     task.headers["Host"])
    self.assertEqual("v7.foo-module", task.target)

  def testGetTaskHostDefaultModule(self):
    os.environ["CURRENT_MODULE_ID"] = "default"
    self.assertEqual("v7.foo.appspot.com", util._get_task_host())
    task = taskqueue.Task(url="/relative_url",
                          headers={"Host": util._get_task_host()})
    self.assertEqual("v7.foo.appspot.com",
                     task.headers["Host"])
    self.assertEqual("v7", task.target)

  def testGetTaskHeaders(self):
    mr_spec = model.MapreduceSpec(
        name="foo", mapreduce_id="foo_id",
        mapper_spec=model.MapperSpec("foo", "foo", {}, 8).to_json())
    task = taskqueue.Task(url="/relative_url",
                          headers=util._get_task_headers(mr_spec))
    self.assertEqual("foo_id", task.headers[util._MR_ID_TASK_HEADER])
    self.assertEqual("v7.foo-module.foo.appspot.com",
                     task.headers["Host"])
    self.assertEqual("v7.foo-module", task.target)


class GetShortNameTest(unittest.TestCase):
  """Test util.get_short_name function."""

  def testGetShortName(self):
    self.assertEquals("blah", util.get_short_name("blah"))
    self.assertEquals("blah", util.get_short_name(".blah"))
    self.assertEquals("blah", util.get_short_name("__mmm__.blah"))
    self.assertEquals("blah", util.get_short_name("__mmm__.Krb.blah"))


class TotalSecondsTest(unittest.TestCase):
  """Test util.total_seconds."""

  def testTotalSeconds(self):
    td = datetime.timedelta(days=1, seconds=1)
    self.assertEqual(24 * 60 * 60 + 1, util.total_seconds(td))

    td = datetime.timedelta(days=1, seconds=1, microseconds=1)
    self.assertEqual(24 * 60 * 60 + 2, util.total_seconds(td))


class ParseBoolTest(unittest.TestCase):
  """Test util.parse_bool function."""

  def testParseBool(self):
    self.assertEquals(True, util.parse_bool(True))
    self.assertEquals(False, util.parse_bool(False))
    self.assertEquals(True, util.parse_bool("True"))
    self.assertEquals(False, util.parse_bool("False"))
    self.assertEquals(True, util.parse_bool(1))
    self.assertEquals(False, util.parse_bool(0))
    self.assertEquals(True, util.parse_bool("on"))
    self.assertEquals(False, util.parse_bool("off"))


class CreateConfigTest(unittest.TestCase):
  """Test create_datastore_write_config function."""

  def setUp(self):
    super(CreateConfigTest, self).setUp()
    self.spec = MockMapreduceSpec()

  def testDefaultConfig(self):
    config = util.create_datastore_write_config(self.spec)
    self.assertTrue(config)
    self.assertFalse(config.force_writes)

  def testForceWrites(self):
    self.spec.params["force_writes"] = "True"
    config = util.create_datastore_write_config(self.spec)
    self.assertTrue(config)
    self.assertTrue(config.force_writes)


if __name__ == "__main__":
  unittest.main()
