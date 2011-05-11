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




# Disable "Invalid method name"
# pylint: disable-msg=C6409

# os_compat must be first to ensure timezones are UTC.
# Disable "unused import" and "invalid import order"
# pylint: disable-msg=W0611
from google.appengine.tools import os_compat
# pylint: enable-msg=W0611

import base64
import cgi
import datetime
from testlib import mox
import os
from mapreduce.lib import simplejson
import time
import urllib
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_file_stub
from google.appengine.api import memcache
from google.appengine.api.memcache import memcache_stub
from google.appengine.api.taskqueue import taskqueue_stub
from google.appengine.ext import db
from mapreduce.lib import key_range
from mapreduce import context
from mapreduce import control
from mapreduce import handlers
from mapreduce import hooks
from mapreduce import input_readers
from mapreduce import operation
from mapreduce import output_writers
from mapreduce import model
from mapreduce import quota
from testlib import testutil
from testlib import mock_webapp


MAPPER_PARAMS = {"batch_size": 50}
PARAM_DONE_CALLBACK = model.MapreduceSpec.PARAM_DONE_CALLBACK
PARAM_DONE_CALLBACK_QUEUE = model.MapreduceSpec.PARAM_DONE_CALLBACK_QUEUE


class TestHooks(hooks.Hooks):
  """Test hooks class."""

  def __init__(self, mapper):
    super(TestHooks, self).__init__(mapper)
    TestHooks.enqueue_worker_task_calls = []
    TestHooks.enqueue_done_task_calls = []
    TestHooks.enqueue_controller_task_calls = []

  def enqueue_worker_task(self, task, queue_name):
    self.enqueue_worker_task_calls.append((task, queue_name))

  def enqueue_kickoff_task(self, task, queue_name):
    # Tested by control_test.ControlTest.testStartMap_Hooks.
    pass

  def enqueue_done_task(self, task, queue_name):
    self.enqueue_done_task_calls.append((task, queue_name))

  def enqueue_controller_task(self, task, queue_name):
    self.enqueue_controller_task_calls.append((task, queue_name))


class TestKind(db.Model):
  """Used for testing."""

  foobar = db.StringProperty(default="meep")


def TestMap(entity):
  """Used for testing."""
  pass


class TestException(Exception):
  """Test exception to use in test handlers."""


class MockTime(object):
  """Simple class to use for mocking time() funciton."""

  now = time.time()

  @staticmethod
  def time():
    """Get current mock time."""
    return MockTime.now

  @staticmethod
  def advance_time(delta):
    """Advance current mock time by delta."""
    MockTime.now += delta


class TestEntity(db.Model):
  """Test entity class."""


class TestHandler(object):
  """Test handler which stores all processed entities keys.

  Properties:
    processed_keys: all keys of processed entities.
    delay: advances mock time by this delay on every call.
  """

  processed_keys = []
  delay = 0

  def __call__(self, entity):
    """Main handler process function.

    Args:
      entity: entity to process.
    """
    TestHandler.processed_keys.append(str(entity.key()))
    MockTime.advance_time(TestHandler.delay)

  @staticmethod
  def reset():
    """Clear processed_keys & reset delay to 0."""
    TestHandler.processed_keys = []
    TestHandler.delay = 0


class TestOperation(operation.Operation):
  """Test operation which records entity on execution."""

  processed_keys = []

  def __init__(self, entity):
    self.entity = entity

  def __call__(self, context):
    TestOperation.processed_keys.append(str(self.entity.key()))

  @classmethod
  def rest(cls):
    cls.processed_keys = []


def test_handler_raise_exception(entity):
  """Test handler function which always raises exception.

  Raises:
    TestException: always.
  """
  raise TestException()


def test_handler_yield_op(entity):
  """Test handler function which yields test operation twice for entity."""
  yield TestOperation(entity)
  yield TestOperation(entity)


def test_param_validator_success(params):
  """Test parameter validator that is successful."""
  params["test"] = "good"


def test_param_validator_raise_exception(params):
  """Test parameter validator that fails."""
  raise Exception("These params are bad")


def test_handler_yield_keys(entity):
  """Test handler which yeilds entity keys."""
  yield entity.key()


class InputReader(input_readers.DatastoreInputReader):
  """Test input reader which records number of yields."""

  yields = 0

  def __iter__(self):
    for entity in input_readers.DatastoreInputReader.__iter__(self):
      InputReader.yields += 1
      yield entity

  @classmethod
  def reset(cls):
    cls.yields = 0


class TestOutputWriter(output_writers.OutputWriter):
  """Test output writer."""

  # store lifecycle events.
  events = []

  @classmethod
  def reset(cls):
    cls.events = []

  @classmethod
  def validate(cls, mapper_spec):
    assert isinstance(mapper_spec, model.MapperSpec)
    if "fail_writer_validate" in mapper_spec.params:
      raise Exception("Failed Validation")

  @classmethod
  def init_job(cls, mapreduce_state):
    assert isinstance(mapreduce_state, model.MapreduceState)
    cls.events.append("init_job")

  @classmethod
  def finalize_job(cls, mapreduce_state):
    assert isinstance(mapreduce_state, model.MapreduceState)
    cls.events.append("finalize_job")

  @classmethod
  def create(cls, mapreduce_state, shard_number):
    assert isinstance(mapreduce_state, model.MapreduceState)
    cls.events.append("create-" + str(shard_number))
    return cls()

  def to_json(self):
    return {}

  @classmethod
  def from_json(cls, json_dict):
    return cls()

  def write(self, data, ctx):
    assert isinstance(ctx, context.Context)
    self.events.append("write-" + str(data))

  def finalize(self, ctx, shard_number):
    assert isinstance(ctx, context.Context)
    self.events.append("finalize-" + str(shard_number))


class MatchesContext(mox.Comparator):
  """Mox comparator to match context instances."""

  def __init__(self, **kwargs):
    self.kwargs = kwargs

  def equals(self, ctx):
    """Check to see if ctx matches arguments."""
    if self.kwargs.get("task_retry_count", 0) != ctx.task_retry_count:
      return False
    return True

  def __repr__(self):
    return "MatchesContext(%s)" % self.kwargs


class FixedShardSizeInputReader(input_readers.DatastoreInputReader):
  """Test rider which trucates the list of readers to specified size."""
  readers_size = 3

  @classmethod
  def split_input(cls, mapper_spec):
    readers = input_readers.DatastoreInputReader.split_input(mapper_spec)
    return readers[:cls.readers_size]


ENTITY_KIND = "__main__.TestEntity"
MAPPER_HANDLER_SPEC = __name__ + "." + TestHandler.__name__

COUNTER_MAPPER_CALLS = context.COUNTER_MAPPER_CALLS


class MapreduceHandlerTestBase(testutil.HandlerTestBase):
  """Base class for all mapreduce handler tests.

  Contains common fixture and utility methods.
  """

  def setUp(self):
    """Sets up the test harness."""
    testutil.HandlerTestBase.setUp(self)
    TestHandler.reset()
    TestOutputWriter.reset()

  def find_task_by_name(self, tasks, name):
    """Find a task with given name.

    Args:
      tasks: iterable of tasks.
      name: a name to look for.

    Returns:
      task or None
    """
    for task in tasks:
      if task["name"] == name:
        return task
    return None

  def verify_input_reader_state(self, str_state, **kwargs):
    """Check that input reader state has expected values.

    Args:
      str_state: input reader state serialized into string.
    """
    state = simplejson.loads(str_state)
    self.assertEquals(ENTITY_KIND, state["entity_kind"])
    self.assertTrue("key_range" in state or "current_key_range" in state,
                    "invalid state: %r" % str_state)
    self.assertEquals(50, state["batch_size"])

  def verify_shard_task(self, task, shard_id, slice_id=0, eta=None,
                        countdown=None, **kwargs):
    """Checks that all shard task properties have expected values.

    Args:
      task: task to check.
      shard_id: expected shard id.
      slice_id: expected slice_id.
      eta: expected task eta.
      countdown: expected task delay from now.
      kwargs: Extra keyword arguments to pass to verify_mapreduce_spec.
    """
    expected_task_name = handlers.MapperWorkerCallbackHandler.get_task_name(
        shard_id, slice_id)
    self.assertEquals(expected_task_name, task["name"])
    self.assertEquals("POST", task["method"])
    self.assertEquals("/mapreduce/worker_callback", task["url"])
    if eta:
      self.assertEquals(eta.strftime("%Y/%m/%d %H:%M:%S"), task["eta"])
    if countdown:
      expected_etc_sec = time.time() + countdown
      eta_sec = time.mktime(time.strptime(task["eta"], "%Y/%m/%d %H:%M:%S"))
      self.assertTrue(expected_etc_sec < eta_sec + 10)

    payload = testutil.decode_task_payload(task)
    self.assertEquals(str(shard_id), payload["shard_id"])
    self.assertEquals(str(slice_id), payload["slice_id"])

    self.assertTrue(payload["mapreduce_spec"])
    mapreduce_spec = model.MapreduceSpec.from_json_str(
        payload["mapreduce_spec"])
    self.verify_mapreduce_spec(mapreduce_spec, **kwargs)
    self.verify_input_reader_state(payload["input_reader_state"], **kwargs)

  def verify_mapreduce_spec(self, mapreduce_spec, **kwargs):
    """Check all mapreduce spec properties to have expected values.

    Args:
      mapreduce_spec: mapreduce spec to check as MapreduceSpec.
      kwargs: expected property values. Checks for default property value if
        particular property is not specified.
    """
    self.assertTrue(mapreduce_spec)
    self.assertEquals(kwargs.get("mapper_handler_spec", MAPPER_HANDLER_SPEC),
                      mapreduce_spec.mapper.handler_spec)
    self.assertEquals(kwargs.get("output_writer_spec", None),
                      mapreduce_spec.mapper.output_writer_spec)
    self.assertEquals(ENTITY_KIND,
                      mapreduce_spec.mapper.params["entity_kind"])
    self.assertEquals(kwargs.get("shard_count", 8),
                      mapreduce_spec.mapper.shard_count)
    self.assertEquals(kwargs.get("hooks_class_name"),
                      mapreduce_spec.hooks_class_name)

  def verify_shard_state(self, shard_state, **kwargs):
    """Checks that all shard state properties have expected values.

    Args:
      shard_state: shard state to check.
      kwargs: expected property values. Checks for default property value if
        particular property is not specified.
    """
    self.assertTrue(shard_state)

    self.assertEquals(kwargs.get("active", True), shard_state.active)
    self.assertEquals(kwargs.get("processed", 0),
                      shard_state.counters_map.get(COUNTER_MAPPER_CALLS))
    self.assertEquals(kwargs.get("result_status", None),
                      shard_state.result_status)

  def verify_mapreduce_state(self, mapreduce_state, **kwargs):
    """Checks mapreduce state to have expected property values.

    Args:
      mapreduce_state: mapreduce state to check.
      kwargs: expected property values. Checks for default property value if
        particular property is not specified.
    """
    self.assertTrue(mapreduce_state)
    self.assertTrue(
        mapreduce_state.chart_url.startswith("http://chart.apis.google.com/"),
        "Wrong chart url: " + mapreduce_state.chart_url)

    self.assertEquals(kwargs.get("active", True), mapreduce_state.active)
    self.assertEquals(kwargs.get("processed", 0),
                      mapreduce_state.counters_map.get(COUNTER_MAPPER_CALLS))
    self.assertEquals(kwargs.get("result_status", None),
                      mapreduce_state.result_status)

    mapreduce_spec = mapreduce_state.mapreduce_spec
    self.verify_mapreduce_spec(mapreduce_spec, **kwargs)

  def verify_controller_task(self, task, **kwargs):
    """Checks that all update status task properties have expected values.

    Args:
      task: task to check.
      kwargs: expected property values. Checks for default if property is not
        specified.
    """
    self.assertEquals("POST", task["method"])
    self.assertEquals("/mapreduce/controller_callback", task["url"])

    payload = testutil.decode_task_payload(task)
    mapreduce_spec = model.MapreduceSpec.from_json_str(
        payload["mapreduce_spec"])
    self.verify_mapreduce_spec(mapreduce_spec, **kwargs)

  def create_mapreduce_spec(self,
                            mapreduce_id,
                            shard_count=8,
                            mapper_handler_spec=MAPPER_HANDLER_SPEC,
                            mapper_parameters=None,
                            hooks_class_name=None,
                            output_writer_spec=None):
    """Create a new valid mapreduce_spec.

    Args:
      mapreduce_id: mapreduce id.
      shard_count: number of shards in the handlers.
      mapper_handler_spec: handler specification to use for handlers.
      hooks_class_name: fully qualified name of the hooks class.

    Returns:
      new MapreduceSpec.
    """
    params = {"entity_kind": __name__ + "." + TestEntity.__name__}
    if mapper_parameters is not None:
      params.update(mapper_parameters)

    mapper_spec = model.MapperSpec(
        mapper_handler_spec,
        __name__ + ".InputReader",
        params,
        shard_count,
        output_writer_spec=output_writer_spec)
    mapreduce_spec = model.MapreduceSpec("my job",
                                         mapreduce_id,
                                         mapper_spec.to_json(),
                                         hooks_class_name=hooks_class_name)

    self.verify_mapreduce_spec(mapreduce_spec,
                               shard_count=shard_count,
                               mapper_handler_spec=mapper_handler_spec,
                               hooks_class_name=hooks_class_name,
                               output_writer_spec=output_writer_spec)

    state = model.MapreduceState(
        key_name=mapreduce_id,
        last_poll_time=datetime.datetime.now())
    state.mapreduce_spec = mapreduce_spec
    state.active = True
    state.shard_count = shard_count
    state.active_shards = shard_count
    state.put()

    return mapreduce_spec

  def create_shard_state(self, mapreduce_id, shard_number):
    """Creates a new valid shard state.

    Args:
      mapreduce_id: mapreduce id to create state for as string.
      shard_number: shard number as int.

    Returns:
      new ShardState.
    """
    shard_state = model.ShardState.create_new(mapreduce_id, shard_number)
    self.verify_shard_state(shard_state)
    return shard_state

  def create_and_store_shard_state(self, mapreduce_id, shard_number):
    """Creates a new valid shard state and saves it into memcache.

    Args:
      mapreduce_id: mapreduce id to create state for as string.
      shard_number: shard number as int.

    Returns:
      new ShardState.
    """
    shard_state = self.create_shard_state(mapreduce_id, shard_number)
    shard_state.put()
    return shard_state

  def key(self, entity_id):
    """Create a key for TestEntity with specified id.

    Used to shorted expected data.

    Args:
      entity_id: entity id
    Returns:
      db.Key instance with specified id for TestEntity.
    """
    return db.Key.from_path("TestEntity", entity_id)


class StartJobHandlerTest(MapreduceHandlerTestBase):
  """Test handlers.StartJobHandler."""

  def setUp(self):
    """Sets up the test harness."""
    MapreduceHandlerTestBase.setUp(self)
    self.handler = handlers.StartJobHandler()
    self.handler.initialize(mock_webapp.MockRequest(),
                            mock_webapp.MockResponse())

    self.handler.request.path = "/mapreduce/command/start_job"
    self.handler.request.set("name", "my job")
    self.handler.request.set(
        "mapper_input_reader",
        "mapreduce.input_readers.DatastoreInputReader")
    self.handler.request.set("mapper_handler", MAPPER_HANDLER_SPEC)
    self.handler.request.set("mapper_params.entity_kind",
                             (__name__ + "." + TestEntity.__name__))

    self.handler.request.headers["X-Requested-With"] = "XMLHttpRequest"

  def get_mapreduce_spec(self, task):
    """Get mapreduce spec form kickoff task payload."""
    payload = testutil.decode_task_payload(task)
    return model.MapreduceSpec.from_json_str(payload["mapreduce_spec"])

  def testCSRF(self):
    """Tests that that handler only accepts AJAX requests."""
    del self.handler.request.headers["X-Requested-With"]
    self.handler.post()
    self.assertEquals(403, self.handler.response.status)

  def testSmoke(self):
    """Verifies main execution path of starting scan over several entities."""
    for _ in range(100):
      TestEntity().put()
    self.handler.post()

    # Only kickoff task should be there.
    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    mapreduce_spec = self.get_mapreduce_spec(tasks[0])
    self.assertTrue(mapreduce_spec)
    self.assertEquals(MAPPER_HANDLER_SPEC, mapreduce_spec.mapper.handler_spec)

  def testSmokeOtherApp(self):
    """Verifies main execution path of starting scan over several entities."""
    apiproxy_stub_map.apiproxy.GetStub("datastore_v3").SetTrusted(True)
    self.handler.request.set("mapper_params._app", "otherapp")
    TestEntity(_app="otherapp").put()
    self.handler.post()

    # Only kickoff task should be there.
    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    payload = testutil.decode_task_payload(tasks[0]) 
    self.assertEquals("otherapp", payload["app"])
    self.assertTrue(self.get_mapreduce_spec(tasks[0]))

  def testRequiredParams(self):
    """Tests that required parameters are enforced."""
    TestEntity().put()
    self.handler.post()

    self.handler.request.set("name", None)
    self.assertRaises(handlers.NotEnoughArgumentsError, self.handler.handle)

    self.handler.request.set("name", "my job")
    self.handler.request.set("mapper_input_reader", None)
    self.assertRaises(handlers.NotEnoughArgumentsError, self.handler.handle)

    self.handler.request.set(
        "mapper_input_reader",
        "mapreduce.input_readers.DatastoreInputReader")
    self.handler.request.set("mapper_handler", None)
    self.assertRaises(handlers.NotEnoughArgumentsError, self.handler.handle)

    self.handler.request.set("mapper_handler", MAPPER_HANDLER_SPEC)
    self.handler.request.set("mapper_params.entity_kind", None)
    self.assertRaises(input_readers.BadReaderParamsError, self.handler.handle)

    self.handler.request.set("mapper_params.entity_kind",
                             (__name__ + "." + TestEntity.__name__))
    self.handler.post()

  def testParameterValidationSuccess(self):
    """Tests validating user-supplied parameters."""
    TestEntity().put()
    self.handler.request.set("mapper_params.one", ["red", "blue"])
    self.handler.request.set("mapper_params.two", "green")
    self.handler.request.set("mapper_params_validator",
                             __name__ + ".test_param_validator_success")
    self.handler.post()

    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    mapreduce_spec = self.get_mapreduce_spec(tasks[0])
    params = mapreduce_spec.mapper.params

    self.assertEquals(["red", "blue"], params["one"])
    self.assertEquals("green", params["two"])

    # From the validator function
    self.assertEquals("good", params["test"])

    # Defaults always present.
    self.assertEquals(model._DEFAULT_PROCESSING_RATE_PER_SEC,
                      params["processing_rate"])
    self.assertEquals("default", params["queue_name"])

  def testMapreduceParameters(self):
    """Tests propagation of user-supplied mapreduce parameters."""
    TestEntity().put()
    self.handler.request.set("params.one", ["red", "blue"])
    self.handler.request.set("params.two", "green")
    self.handler.request.set("params_validator",
                             __name__ + ".test_param_validator_success")
    self.handler.post()

    kickoff_task = self.taskqueue.GetTasks("default")[0]
    mapreduce_spec = self.get_mapreduce_spec(kickoff_task)
    params = mapreduce_spec.params
    self.assertEquals(["red", "blue"], params["one"])
    self.assertEquals("green", params["two"])

    # From the validator function
    self.assertEquals("good", params["test"])

  def testParameterValidationFailure(self):
    """Tests when validating user-supplied parameters fails."""
    self.handler.request.set("mapper_params_validator",
                             __name__ + ".test_param_validator_raise_exception")
    try:
      self.handler.handle()
      self.fail()
    except Exception, e:
      self.assertEquals("These params are bad", str(e))

  def testParameterValidationUnknown(self):
    """Tests the user-supplied parameter validation function cannot be found."""
    self.handler.request.set("mapper_params_validator", "does_not_exist")
    self.assertRaises(ImportError, self.handler.handle)

  def testHandlerUnknown(self):
    """Tests when the handler function cannot be found."""
    self.handler.request.set("mapper_handler", "does_not_exist")
    self.assertRaises(ImportError, self.handler.handle)

  def testInputReaderUnknown(self):
    """Tests when the input reader function cannot be found."""
    self.handler.request.set("mapper_input_reader", "does_not_exist")
    self.assertRaises(ImportError, self.handler.handle)

  def testQueueName(self):
    """Tests that the optional queue_name parameter is used."""
    TestEntity().put()
    self.handler.request.set("mapper_params.queue_name", "crazy-queue")
    self.handler.post()

    tasks = self.taskqueue.GetTasks("crazy-queue")
    self.assertEquals(1, len(tasks))
    mapreduce_spec = self.get_mapreduce_spec(tasks[0])
    self.assertEquals(
        "crazy-queue",
        mapreduce_spec.mapper.params["queue_name"])
    self.assertEquals(0, len(self.taskqueue.GetTasks("default")))

  def testProcessingRate(self):
    """Tests that the optional processing rate parameter is used."""
    TestEntity().put()
    self.handler.request.set("mapper_params.processing_rate", "1234")
    self.handler.post()

    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    mapreduce_spec = self.get_mapreduce_spec(tasks[0])

    self.assertEquals(
        1234,
        mapreduce_spec.mapper.params["processing_rate"])

  def testShardCount(self):
    """Tests that the optional shard count parameter is used."""
    TestEntity().put()
    self.handler.request.set("mapper_params.shard_count", "9")
    self.handler.post()

    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    mapreduce_spec = self.get_mapreduce_spec(tasks[0])
    self.assertEquals(9, mapreduce_spec.mapper.shard_count)

  def testOutputWriter(self):
    """Tests setting output writer parameter."""
    TestEntity().put()
    self.handler.request.set("mapper_output_writer",
                             __name__ + ".TestOutputWriter")
    self.handler.handle()

    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    mapreduce_spec = self.get_mapreduce_spec(tasks[0])
    self.assertEquals("__main__.TestOutputWriter",
                      mapreduce_spec.mapper.output_writer_spec)

  def testOutputWriterValidateFails(self):
    TestEntity().put()
    self.handler.request.set("mapper_output_writer",
                             __name__ + ".TestOutputWriter")
    self.handler.request.set("mapper_params.fail_writer_validate",
                             "true")
    self.assertRaises(Exception, self.handler.handle)

  def testInvalidOutputWriter(self):
    """Tests setting output writer parameter."""
    TestEntity().put()
    self.handler.request.set("mapper_output_writer", "Foo")
    self.assertRaises(ImportError, self.handler.handle)


class KickOffJobHandlerTest(MapreduceHandlerTestBase):
  """Test handlers.StartJobHandler."""

  def setUp(self):
    """Sets up the test harness."""
    MapreduceHandlerTestBase.setUp(self)

    self.mapreduce_id = "mapreduce0"
    self.mapreduce_spec = self.create_mapreduce_spec(self.mapreduce_id)

    self.handler = handlers.KickOffJobHandler()
    self.handler.initialize(mock_webapp.MockRequest(),
                            mock_webapp.MockResponse())

    self.handler.request.path = "/mapreduce/kickoffjob_callback"
    self.handler.request.set(
        "mapreduce_spec",
        self.mapreduce_spec.to_json_str())

    self.handler.request.headers["X-AppEngine-QueueName"] = "default"

  def testCSRF(self):
    """Tests that that handler only accepts requests from the task queue."""
    del self.handler.request.headers["X-AppEngine-QueueName"]
    self.handler.post()
    self.assertEquals(403, self.handler.response.status)

  def testSmoke(self):
    """Verifies main execution path of starting scan over several entities."""
    for i in range(100):
      TestEntity().put()
    self.handler.post()
    shard_count = 8

    state = model.MapreduceState.all()[0]
    self.assertTrue(state)
    self.assertTrue(state.active)

    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(shard_count + 1, len(tasks))

    for i in xrange(shard_count):
      shard_id = model.ShardState.shard_id_from_number(self.mapreduce_id, i)
      task_name = handlers.MapperWorkerCallbackHandler.get_task_name(
          shard_id, 0)

      shard_task = self.find_task_by_name(tasks, task_name)
      self.assertTrue(shard_task)
      tasks.remove(shard_task)
      self.verify_shard_task(shard_task, shard_id, shard_count=8)

      self.verify_shard_state(
          model.ShardState.get_by_shard_id(shard_id))

    # only update task should be left in tasks array
    self.assertEquals(1, len(tasks))
    self.verify_controller_task(tasks[0], shard_count=8)

  def testHooks(self):
    """Verifies main execution path with a hooks class installed."""
    for i in range(100):
      TestEntity().put()
    self.mapreduce_spec.hooks_class_name = __name__ + "." + TestHooks.__name__
    self.handler.request.set(
        "mapreduce_spec",
        self.mapreduce_spec.to_json_str())

    self.handler.post()
    self.assertEquals(8, len(TestHooks.enqueue_worker_task_calls))
    self.assertEquals(1, len(TestHooks.enqueue_controller_task_calls))
    task, queue_name = TestHooks.enqueue_controller_task_calls[0]
    self.assertEquals("default", queue_name)
    self.assertEquals("/mapreduce/controller_callback", task.url)

  def testRequiredParams(self):
    """Tests that required parameters are enforced."""
    self.handler.post()

    self.handler.request.set("mapreduce_spec", None)
    self.assertRaises(handlers.NotEnoughArgumentsError, self.handler.post)

  def testInputReaderUnknown(self):
    """Tests when the input reader function cannot be found."""
    self.mapreduce_spec.mapper.input_reader_spec = "does_not_exist"
    self.handler.request.set("mapreduce_spec",
                             self.mapreduce_spec.to_json_str())

    self.assertRaises(ImportError, self.handler.post)

  def testQueueName(self):
    """Tests that the optional queue_name parameter is used."""
    os.environ["HTTP_X_APPENGINE_QUEUENAME"] = "crazy-queue"
    TestEntity().put()
    self.handler.post()
    del os.environ["HTTP_X_APPENGINE_QUEUENAME"]

    self.assertEquals(0, len(self.taskqueue.GetTasks("default")))
    self.assertEquals(2, len(self.taskqueue.GetTasks("crazy-queue")))

  def testNoData(self):
    self.handler.post()
    self.assertEquals(2, len(self.taskqueue.GetTasks("default")))

    state = model.MapreduceState.get_by_job_id(self.mapreduce_id)
    self.assertTrue(state.active)
    self.assertEquals(1, state.active_shards)

  def testDifferentShardCount(self):
    """Verifies the case when input reader created diffrent shard number."""
    for _ in range(100):
      TestEntity().put()
    self.mapreduce_spec.mapper.input_reader_spec = (
        __name__ + ".FixedShardSizeInputReader")
    self.handler.request.set(
        "mapreduce_spec",
        self.mapreduce_spec.to_json_str())
    self.handler.post()

    shard_count = FixedShardSizeInputReader.readers_size
    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(shard_count + 1, len(tasks))

    for i in xrange(shard_count):
      shard_id = model.ShardState.shard_id_from_number(self.mapreduce_id, i)
      task_name = handlers.MapperWorkerCallbackHandler.get_task_name(
          shard_id, 0)

      shard_task = self.find_task_by_name(tasks, task_name)
      self.assertTrue(shard_task)
      tasks.remove(shard_task)
      self.verify_shard_task(shard_task, shard_id, shard_count=shard_count)

      self.verify_shard_state(
          model.ShardState.get_by_shard_id(shard_id))

    # only update task should be left in tasks list
    self.assertEquals(1, len(tasks))
    self.verify_controller_task(tasks[0], shard_count=shard_count)

  def testAppParam(self):
    """Tests that app parameter is correctly passed in the state."""
    self.handler.request.set("app", "otherapp")

    self.handler.post()

    state = model.MapreduceState.all()[0]
    self.assertTrue(state)
    self.assertEquals("otherapp", state.app_id)

  def testOutputWriter(self):
    """Test output writer initialization."""
    for _ in range(100):
      TestEntity().put()

    self.mapreduce_spec.mapper.output_writer_spec = (
        __name__ + ".TestOutputWriter")
    self.handler.request.set(
        "mapreduce_spec",
        self.mapreduce_spec.to_json_str())
    self.handler.post()

    self.assertEquals(
        ["init_job", "create-0", "create-1", "create-2", "create-3",
         "create-4", "create-5", "create-6", "create-7",],
        TestOutputWriter.events)


class MapperWorkerCallbackHandlerTest(MapreduceHandlerTestBase):
  """Test handlers.MapperWorkerCallbackHandler."""

  def setUp(self):
    """Sets up the test harness."""
    MapreduceHandlerTestBase.setUp(self)
    self.init()

  def init(self,
           mapper_handler_spec=MAPPER_HANDLER_SPEC,
           mapper_parameters=None,
           hooks_class_name=None,
           output_writer_spec=None):
    """Init everything needed for testing worker callbacks.

    Args:
      mapper_handler_spec: handler specification to use in test.
      mapper_params: mapper specification to use in test.
      hooks_class_name: fully qualified name of the hooks class to use in test.
    """
    InputReader.reset()
    self.handler = handlers.MapperWorkerCallbackHandler(MockTime.time)
    self.handler.initialize(mock_webapp.MockRequest(),
                            mock_webapp.MockResponse())
    self.handler.request.path = "/mapreduce/worker_callback"

    self.mapreduce_id = "mapreduce0"
    self.mapreduce_spec = self.create_mapreduce_spec(
        self.mapreduce_id,
        mapper_handler_spec=mapper_handler_spec,
        hooks_class_name=hooks_class_name,
        output_writer_spec=output_writer_spec,
        mapper_parameters=mapper_parameters)
    self.shard_number = 1
    self.slice_id = 3

    self.shard_state = self.create_and_store_shard_state(
        self.mapreduce_id, self.shard_number)
    self.shard_id = self.shard_state.shard_id

    output_writer = None
    if self.mapreduce_spec.mapper.output_writer_class():
      output_writer = self.mapreduce_spec.mapper.output_writer_class()()

    self.transient_state = model.TransientShardState(
        "/mapreduce",
        self.mapreduce_spec,
        self.shard_id,
        self.slice_id,
        InputReader(ENTITY_KIND, [key_range.KeyRange()]),
        output_writer=output_writer
        )

    worker_params = self.transient_state.to_dict()
    for param_name in worker_params:
      self.handler.request.set(param_name, worker_params[param_name])

    self.quota_manager = quota.QuotaManager(memcache.Client())
    self.initial_quota = 100000
    self.quota_manager.set(self.shard_id, self.initial_quota)

    self.handler.request.headers["X-AppEngine-QueueName"] = "default"

  def testCSRF(self):
    """Tests that that handler only accepts requests from the task queue."""
    del self.handler.request.headers["X-AppEngine-QueueName"]
    self.handler.post()
    self.assertEquals(403, self.handler.response.status)

  def testSmoke(self):
    """Test main execution path of entity scanning."""
    e1 = TestEntity()
    e1.put()

    e2 = TestEntity()
    e2.put()

    self.handler.post()

    self.assertEquals([str(e1.key()), str(e2.key())],
                      TestHandler.processed_keys)

    # we should have finished
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=False, processed=2,
        result_status=model.ShardState.RESULT_SUCCESS)

    # quota should be reclaimed correctly
    self.assertEquals(self.initial_quota - len(TestHandler.processed_keys),
                      self.quota_manager.get(self.shard_id))

  def testNoShardState(self):
    """Correct handling of missing shard state."""
    self.shard_state.delete()
    e1 = TestEntity()
    e1.put()

    self.handler.post()

    # no state => no data processed
    self.assertEquals([], TestHandler.processed_keys)

  def testNoData(self):
    """Test no data to scan case."""
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id), active=True)

    self.handler.post()

    self.assertEquals([], TestHandler.processed_keys)

    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=False,
        result_status=model.ShardState.RESULT_SUCCESS)

  def testUserAbort(self):
    """Tests a user-initiated abort of the shard."""
    model.MapreduceControl.abort(self.mapreduce_id)
    self.handler.post()
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=False,
        result_status=model.ShardState.RESULT_ABORTED)

  def testLongProcessingShouldStartAnotherSlice(self):
    """Long scan.

    If scanning takes too long, it should be paused, and new continuation task
    should be spawned.
    """
    e1 = TestEntity()
    e1.put()

    e2 = TestEntity()
    e2.put()

    TestHandler.delay = handlers._SLICE_DURATION_SEC + 10

    self.handler.post()

    # only first entity should be processed
    self.assertEquals([str(e1.key())], TestHandler.processed_keys)

    # quota should be reclaimed correctly
    self.assertEquals(self.initial_quota - len(TestHandler.processed_keys),
                      self.quota_manager.get(self.shard_id))

    # slice should be still active
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        processed=1)

    # new task should be spawned
    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    self.verify_shard_task(tasks[0], self.shard_id, self.slice_id + 1)

  def testScheduleSlice(self):
    """Test _schedule_slice method."""
    input_reader = input_readers.DatastoreInputReader(
        ENTITY_KIND,
        [key_range.KeyRange(key_start=self.key(75),
                            key_end=self.key(100),
                            direction="ASC",
                            include_start=False,
                            include_end=True)])
    self.handler._schedule_slice(
        self.shard_state,
        model.TransientShardState(
            "/mapreduce", self.mapreduce_spec,
            self.shard_id, 123, input_reader))

    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    self.verify_shard_task(tasks[0], self.shard_id, 123)

  def testScheduleSlice_Eta(self):
    """Test _schedule_slice method."""
    eta = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
    input_reader = input_readers.DatastoreInputReader(
        ENTITY_KIND,
        [key_range.KeyRange(key_start=self.key(75),
                            key_end=self.key(100),
                            direction="ASC",
                            include_start=False,
                            include_end=True)])
    self.handler._schedule_slice(
        self.shard_state,
        model.TransientShardState(
            "/mapreduce", self.mapreduce_spec,
            self.shard_id, 123, input_reader),
        eta=eta)

    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    self.verify_shard_task(tasks[0], self.shard_id, 123, eta=eta)

  def testScheduleSlice_Countdown(self):
    """Test _schedule_slice method."""
    countdown = 60 * 60
    input_reader = input_readers.DatastoreInputReader(
        ENTITY_KIND,
        [key_range.KeyRange(key_start=self.key(75),
                            key_end=self.key(100),
                            direction="ASC",
                            include_start=False,
                            include_end=True)])
    self.handler._schedule_slice(
        self.shard_state,
        model.TransientShardState(
            "/mapreduce", self.mapreduce_spec,
            self.shard_id, 123, input_reader),
        countdown=countdown)

    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    self.verify_shard_task(tasks[0], self.shard_id, 123, countdown=countdown)

  def testScheduleSlice_QueuePreserved(self):
    """Tests that _schedule_slice will enqueue tasks on the calling queue."""
    os.environ["HTTP_X_APPENGINE_QUEUENAME"] = "crazy-queue"
    try:
      query_range = input_readers.DatastoreInputReader(
          ENTITY_KIND,
          [key_range.KeyRange(key_start=self.key(75),
                              key_end=self.key(100),
                              direction="ASC",
                              include_start=False,
                              include_end=True)])
      self.handler._schedule_slice(
          self.shard_state,
          model.TransientShardState(
              "/mapreduce", self.mapreduce_spec,
              self.shard_id, 123, query_range))

      tasks = self.taskqueue.GetTasks("crazy-queue")
      self.assertEquals(1, len(tasks))
      self.verify_shard_task(tasks[0], self.shard_id, 123)
    finally:
      del os.environ["HTTP_X_APPENGINE_QUEUENAME"]

  def testScheduleSlice_TombstoneErrors(self):
    """Tests when the scheduled slice already exists."""
    self.handler._schedule_slice(self.shard_state, self.transient_state)

    # This catches the exception.
    self.handler._schedule_slice(self.shard_state, self.transient_state)

    # The task won't re-enqueue because it has the same name.
    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))

  def testScheduleSlice_Hooks(self):
    """Test _schedule_slice method with a hooks class installed."""
    hooks_class_name = __name__ + '.' + TestHooks.__name__
    self.init(hooks_class_name=hooks_class_name)

    self.handler._schedule_slice(self.shard_state, self.transient_state)

    self.assertEquals(0, len(self.taskqueue.GetTasks("default")))
    self.assertEquals(1, len(TestHooks.enqueue_worker_task_calls))
    task, queue_name = TestHooks.enqueue_worker_task_calls[0]
    self.assertEquals("/mapreduce/worker_callback", task.url)
    self.assertEquals("default", queue_name)

  def testScheduleSlice_RaisingHooks(self):
    """Test _schedule_slice method with an empty hooks class installed.

    The installed hooks class will raise NotImplementedError in response to
    all method calls.
    """
    hooks_class_name = hooks.__name__ + '.' + hooks.Hooks.__name__
    self.init(hooks_class_name=hooks_class_name)

    input_reader = input_readers.DatastoreInputReader(
        ENTITY_KIND,
        [key_range.KeyRange(key_start=self.key(75),
                            key_end=self.key(100),
                             direction="ASC",
                            include_start=False,
                            include_end=True)])
    self.handler._schedule_slice(
        self.shard_state,
        model.TransientShardState(
            "/mapreduce", self.mapreduce_spec,
            self.shard_id, 123, input_reader))

    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    self.verify_shard_task(tasks[0], self.shard_id, 123,
                           hooks_class_name=hooks_class_name)

  def testQuotaCanBeOptedOut(self):
    """Test work cycle if there was no quota at the very beginning."""
    e1 = TestEntity()
    e1.put()
    self.init(mapper_parameters={"enable_quota": False})
    self.quota_manager.set(self.shard_id, 0)

    self.handler.post()

    # something should still be processed.
    self.assertEquals([str(e1.key())], TestHandler.processed_keys)
    self.assertEquals(1, InputReader.yields)

    # slice should be still active
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        processed=1, active=False, result_status="success")

    # new task should be spawned
    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(0, len(tasks))

  def testNoQuotaAtAll(self):
    """Test work cycle if there was no quota at the very beginning."""
    TestEntity().put()
    self.quota_manager.set(self.shard_id, 0)

    self.handler.post()

    # nothing should be processed.
    self.assertEquals([], TestHandler.processed_keys)
    self.assertEquals(0, InputReader.yields)

    # slice should be still active
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        processed=0)

    # new task should be spawned
    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    self.verify_shard_task(tasks[0], self.shard_id, self.slice_id + 1)

  def testQuotaForPartialBatchOnly(self):
    """Test work cycle if there was quota for less than a batch."""
    for i in range(handlers._QUOTA_BATCH_SIZE * 2):
      TestEntity().put()

    quota = handlers._QUOTA_BATCH_SIZE / 2
    self.quota_manager.set(self.shard_id, quota)

    self.handler.post()

    # only quota size should be processed
    self.assertEquals(quota, len(TestHandler.processed_keys))
    self.assertEquals(0, self.quota_manager.get(self.shard_id))
    self.assertEquals(quota, InputReader.yields)

    # slice should be still active
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        processed=quota)

    # new task should be spawned
    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    self.verify_shard_task(tasks[0], self.shard_id, self.slice_id + 1)

  def testQuotaForBatchAndAHalf(self):
    """Test work cycle if there was quota for batch and a half."""
    for i in range(handlers._QUOTA_BATCH_SIZE * 2):
      TestEntity().put()

    quota = 3 * handlers._QUOTA_BATCH_SIZE / 2
    self.quota_manager.set(self.shard_id, quota)

    self.handler.post()

    # only quota size should be processed
    self.assertEquals(quota, len(TestHandler.processed_keys))
    self.assertEquals(0, self.quota_manager.get(self.shard_id))
    self.assertEquals(quota, InputReader.yields)

    # slice should be still active
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        processed=quota)

    # new task should be spawned
    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    self.verify_shard_task(tasks[0], self.shard_id, self.slice_id + 1)

  def testExceptionInHandler(self):
    """Test behavior when handler throws exception."""
    self.init(__name__ + ".test_handler_raise_exception")
    TestEntity().put()

    # Stub out context._set
    m = mox.Mox()
    m.StubOutWithMock(context.Context, "_set", use_mock_anything=True)

    # Record calls
    context.Context._set(mox.IsA(context.Context))
    # Context should not be flushed on error
    context.Context._set(None)

    m.ReplayAll()
    try: # test, verify
      self.assertRaises(TestException, self.handler.post)

      # quota should be still consumed
      self.assertEquals(self.initial_quota - 1,
                        self.quota_manager.get(self.shard_id))

      # slice should be still active
      shard_state = model.ShardState.get_by_shard_id(self.shard_id)
      self.verify_shard_state(shard_state, processed=0)
      # mapper calls counter should not be incremented
      self.assertEquals(0, shard_state.counters_map.get(
          context.COUNTER_MAPPER_CALLS))

      # new task should not be spawned
      tasks = self.taskqueue.GetTasks("default")
      self.assertEquals(0, len(tasks))

      m.VerifyAll()
    finally:
      m.UnsetStubs()

  def testContext(self):
    """Test proper context initialization."""
    self.handler.request.headers["X-AppEngine-TaskRetryCount"] = 5
    TestEntity().put()

    m = mox.Mox()
    m.StubOutWithMock(context.Context, "_set", use_mock_anything=True)

    context.Context._set(MatchesContext(task_retry_count=5))
    context.Context._set(None)

    m.ReplayAll()
    try: # test, verify
      self.handler.post()
      m.VerifyAll()
    finally:
      m.UnsetStubs()

  def testContextFlush(self):
    """Test context handling."""
    TestEntity().put()

    # Stub out context
    m = mox.Mox()
    m.StubOutWithMock(context.Context, "_set", use_mock_anything=True)
    m.StubOutWithMock(context.Context, "flush", use_mock_anything=True)

    # Record calls
    context.Context._set(mox.IsA(context.Context))
    context.Context.flush()
    context.Context._set(None)

    m.ReplayAll()
    try: # test, verify
      self.handler.post()

      #  1 entity should be processed
      self.assertEquals(1, len(TestHandler.processed_keys))

      m.VerifyAll()
    finally:
      m.UnsetStubs()

  def testOperationYield(self):
    """Test yielding operations from handler."""
    self.init(__name__ + ".test_handler_yield_op")
    e1 = TestEntity().put()
    e2 = TestEntity().put()

    self.handler.post()
    self.assertEquals([str(e1), str(e1), str(e2), str(e2)],
                      TestOperation.processed_keys)

  def testOutputWriter(self):
    self.init(__name__ + ".test_handler_yield_keys",
              output_writer_spec=__name__ + ".TestOutputWriter")
    e1 = TestEntity().put()
    e2 = TestEntity().put()

    self.handler.post()

    self.assertEquals(
        ["write-" + str(e1),
         "write-" + str(e2),
         "finalize-1",
         ], TestOutputWriter.events)


class ControllerCallbackHandlerTest(MapreduceHandlerTestBase):
  """Test handlers.ControllerCallbackHandler."""

  def setUp(self):
    """Sets up the test harness."""
    MapreduceHandlerTestBase.setUp(self)
    self.handler = handlers.ControllerCallbackHandler(MockTime.time)
    self.handler.initialize(mock_webapp.MockRequest(),
                            mock_webapp.MockResponse())
    self.handler.request.path = "/mapreduce/worker_callback"

    self.mapreduce_state = model.MapreduceState.create_new()
    self.mapreduce_state.put()

    self.mapreduce_id = self.mapreduce_state.key().name()
    mapreduce_spec = self.create_mapreduce_spec(self.mapreduce_id, 3)
    mapreduce_spec.params[PARAM_DONE_CALLBACK] = "/fin"
    mapreduce_spec.params[PARAM_DONE_CALLBACK_QUEUE] = "crazy-queue"

    self.mapreduce_state.mapreduce_spec = mapreduce_spec
    self.mapreduce_state.chart_url = "http://chart.apis.google.com/chart?"
    self.mapreduce_state.active = True
    self.mapreduce_state.put()

    self.verify_mapreduce_state(self.mapreduce_state, shard_count=3)

    self.handler.request.set("mapreduce_spec", mapreduce_spec.to_json_str())
    self.handler.request.set("serial_id", "1234")
    self.quota_manager = quota.QuotaManager(memcache.Client())

    self.handler.request.headers["X-AppEngine-QueueName"] = "default"

  def verify_done_task(self):
    tasks = self.taskqueue.GetTasks("crazy-queue")
    self.assertEquals(1, len(tasks))
    task = tasks[0]
    self.assertTrue(task)

    self.assertEquals("/fin", task["url"])
    self.assertEquals("POST", task["method"])
    headers = dict(task["headers"])
    self.assertEquals(self.mapreduce_id, headers["Mapreduce-Id"])

  def testCSRF(self):
    """Tests that that handler only accepts requests from the task queue."""
    del self.handler.request.headers["X-AppEngine-QueueName"]
    self.handler.post()
    self.assertEquals(403, self.handler.response.status)

  def testSmoke(self):
    """Verify main execution path.

    Should aggregate all data from all shards correctly.
    """
    # check that chart_url is updated.
    self.mapreduce_state.chart_url = ""
    self.mapreduce_state.put()

    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      shard_state.counters_map.increment(
          COUNTER_MAPPER_CALLS, i * 2 + 1)  # 1, 3, 5
      # We should have mapreduce active even some (not all)
      # shards are not active
      if i == 0:
        shard_state.active = False
      shard_state.put()

    self.handler.post()

    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    # we should have 1 + 3 + 5 = 9 elements processed
    self.verify_mapreduce_state(mapreduce_state, processed=9, shard_count=3)
    self.assertEquals(0, mapreduce_state.failed_shards)
    self.assertEquals(0, mapreduce_state.aborted_shards)

    # new task should be spawned
    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    self.verify_controller_task(tasks[0], shard_count=3)

  def testMissingShardState(self):
    """Correct handling of missing shard state."""
    self.handler.post()

    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    self.verify_mapreduce_state(mapreduce_state, active=False, shard_count=3,
                                result_status=model.ShardState.RESULT_FAILED)
    self.assertEquals(0, mapreduce_state.failed_shards)
    self.assertEquals(0, mapreduce_state.aborted_shards)

    # Abort signal should be present.
    self.assertEquals(
        model.MapreduceControl.ABORT,
        db.get(model.MapreduceControl.get_key_by_job_id(
            self.mapreduce_id)).command)

    tasks = self.taskqueue.GetTasks("default")
    # Finalize task should be spawned.
    self.assertEquals(1, len(tasks))
    self.assertEquals("/mapreduce/finalizejob_callback", tasks[0]["url"])

    # Done Callback task should be spawned
    self.verify_done_task()

  def testAllShardsAreDone(self):
    """Mapreduce should become inactive when all shards have finished."""
    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      shard_state.counters_map.increment(
          COUNTER_MAPPER_CALLS, i * 2 + 1)  # 1, 3, 5
      shard_state.active = False
      shard_state.result_status = model.ShardState.RESULT_SUCCESS
      shard_state.put()

    self.handler.post()

    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    self.verify_mapreduce_state(
        mapreduce_state, processed=9, active=False, shard_count=3,
        result_status=model.MapreduceState.RESULT_SUCCESS)
    self.assertEquals(0, mapreduce_state.failed_shards)
    self.assertEquals(0, mapreduce_state.aborted_shards)

    tasks = self.taskqueue.GetTasks("default")
    # Finalize task should be spawned.
    self.assertEquals(1, len(tasks))
    self.assertEquals("/mapreduce/finalizejob_callback", tasks[0]["url"])

    # Done Callback task should be spawned
    self.verify_done_task()

    self.assertEquals(
        3, len(model.ShardState.find_by_mapreduce_id(self.mapreduce_id)))

  def testShardsDoneFinalizeOutputWriter(self):
    self.mapreduce_state.mapreduce_spec.mapper.output_writer_spec = (
        __name__ + '.' + TestOutputWriter.__name__)
    self.handler.request.set("mapreduce_spec",
                             self.mapreduce_state.mapreduce_spec.to_json_str())

    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      shard_state.counters_map.increment(
          COUNTER_MAPPER_CALLS, i * 2 + 1)  # 1, 3, 5
      shard_state.active = False
      shard_state.result_status = model.ShardState.RESULT_SUCCESS
      shard_state.put()

    self.handler.post()

    self.assertEquals(["finalize_job"], TestOutputWriter.events)


  def testShardsDoneWithHooks(self):
    self.mapreduce_state.mapreduce_spec.hooks_class_name = (
        __name__ + '.' + TestHooks.__name__)
    self.handler.request.set("mapreduce_spec",
                             self.mapreduce_state.mapreduce_spec.to_json_str())

    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      shard_state.active = False
      shard_state.result_status = model.ShardState.RESULT_SUCCESS
      shard_state.put()

    self.handler.post()
    self.assertEquals(1, len(TestHooks.enqueue_done_task_calls))
    task, queue_name = TestHooks.enqueue_done_task_calls[0]
    self.assertEquals('crazy-queue', queue_name)
    self.assertEquals('/fin', task.url)

  def testShardFailureContinue(self):
    """Tests that when one shard fails the whole job continues."""
    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      if i == 0:
        shard_state.result_status = model.ShardState.RESULT_FAILED
        shard_state.active = False
      else:
        shard_state.result_status = model.ShardState.RESULT_SUCCESS
        shard_state.active = True
      shard_state.put()

    self.handler.post()

    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    self.verify_mapreduce_state(
        mapreduce_state, active=True, shard_count=3)
    self.assertEquals(1, mapreduce_state.failed_shards)
    self.assertEquals(0, mapreduce_state.aborted_shards)

    # new task should be spawned
    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    self.verify_controller_task(tasks[0], shard_count=3)

  def testShardFailureAllDone(self):
    """Tests that individual shard failure affects the job outcome."""
    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      shard_state.active = False
      if i == 0:
        shard_state.result_status = model.ShardState.RESULT_FAILED
      else:
        shard_state.result_status = model.ShardState.RESULT_SUCCESS
      shard_state.put()

    self.handler.post()

    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    self.verify_mapreduce_state(
        mapreduce_state, active=False, shard_count=3,
        result_status=model.ShardState.RESULT_FAILED)
    self.assertEquals(1, mapreduce_state.failed_shards)
    self.assertEquals(0, mapreduce_state.aborted_shards)

    tasks = self.taskqueue.GetTasks("default")
    # Finalize task should be spawned.
    self.assertEquals(1, len(tasks))
    self.assertEquals("/mapreduce/finalizejob_callback", tasks[0]["url"])

    # Done Callback task should be spawned
    self.verify_done_task()

    self.assertEquals(
        3, len(model.ShardState.find_by_mapreduce_id(self.mapreduce_id)))

  def testUserAbort(self):
    """Tests that user abort will stop the job."""
    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      shard_state.active = True
      shard_state.put()

    model.MapreduceControl.abort(self.mapreduce_id)
    self.handler.post()
    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    self.verify_mapreduce_state(
        mapreduce_state, active=True, shard_count=3)
    self.assertEquals(0, mapreduce_state.failed_shards)
    self.assertEquals(0, mapreduce_state.aborted_shards)

    # new task should be spawned
    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    self.verify_controller_task(tasks[0], shard_count=3)
    self.taskqueue.FlushQueue("default")

    # Repeated calls to callback closure while the shards are active will
    # result in a no op. As the controller waits for the shards to finish.
    self.handler.post()
    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    self.verify_mapreduce_state(
        mapreduce_state, active=True, shard_count=3)
    self.assertEquals(0, mapreduce_state.failed_shards)
    self.assertEquals(0, mapreduce_state.aborted_shards)

    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    self.verify_controller_task(tasks[0], shard_count=3)
    self.taskqueue.FlushQueue("default")

    # Force all shards to completion state (success, failure, or abort).
    shard_state_list = model.ShardState.find_by_mapreduce_id(self.mapreduce_id)
    self.assertEquals(3, len(shard_state_list))
    shard_state_list[0].active = False
    shard_state_list[0].result_status = model.ShardState.RESULT_SUCCESS
    shard_state_list[1].active = False
    shard_state_list[1].result_status = model.ShardState.RESULT_FAILED
    shard_state_list[2].active = False
    shard_state_list[2].result_status = model.ShardState.RESULT_ABORTED
    db.put(shard_state_list)

    self.handler.post()
    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    self.verify_mapreduce_state(
        mapreduce_state, active=False, shard_count=3,
        result_status=model.ShardState.RESULT_ABORTED)
    self.assertEquals(1, mapreduce_state.failed_shards)
    self.assertEquals(1, mapreduce_state.aborted_shards)

    tasks = self.taskqueue.GetTasks("default")
    # Finalize task should be spawned.
    self.assertEquals(1, len(tasks))
    self.assertEquals("/mapreduce/finalizejob_callback", tasks[0]["url"])

    # Done Callback task should be spawned
    self.verify_done_task()

  def testInitialQuota(self):
    """Tests that the controller gives shards no quota to start."""
    shard_states = []
    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      shard_states.append(shard_state)
      shard_state.put()

    self.handler.post()

    for shard_state in shard_states:
      self.assertEquals(0, self.quota_manager.get(shard_state.shard_id))

  def testQuotaRefill(self):
    """Test that controller refills quota after some time."""
    shard_states = []
    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      shard_states.append(shard_state)
      shard_state.put()

    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    mapreduce_state.last_poll_time = \
        datetime.datetime.utcfromtimestamp(int(MockTime.time()))
    mapreduce_state.put()

    self.handler.post()

    # 0 second passed. No quota should be filled
    for shard_state in shard_states:
      self.assertEquals(0, self.quota_manager.get(shard_state.shard_id))

    MockTime.advance_time(1)
    self.handler.post()

    # 1 second passed. ceil(33.3) = 34 quotas should be refilled.
    # (100 entities/sec = 33.3 entities/shard/sec in our case).
    for shard_state in shard_states:
      self.assertEquals(333334, self.quota_manager.get(shard_state.shard_id))

  def testQuotaIsSplitOnlyBetweenActiveShards(self):
    """Test that quota is split only between active shards."""
    active_shard_states = []
    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      if i == 1:
        shard_state.active = False
      else:
        active_shard_states.append(shard_state)
      shard_state.put()

    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    mapreduce_state.last_poll_time = \
        datetime.datetime.fromtimestamp(int(MockTime.time()))
    mapreduce_state.put()

    MockTime.advance_time(1)
    self.handler.post()

    for shard_state in active_shard_states:
      self.assertEquals(500000, self.quota_manager.get(shard_state.shard_id))

  def testScheduleQueueName(self):
    """Tests that the calling queue name is preserved on schedule calls."""
    os.environ["HTTP_X_APPENGINE_QUEUENAME"] = "crazy-queue"
    try:
      self.mapreduce_state.put()
      for i in range(3):
        shard_state = self.create_shard_state(self.mapreduce_id, i)
        shard_state.put()

      self.handler.post()

      # new task should be spawned on the calling queue
      tasks = self.taskqueue.GetTasks("crazy-queue")
      self.assertEquals(1, len(tasks))
      self.verify_controller_task(tasks[0], shard_count=3)
    finally:
      del os.environ["HTTP_X_APPENGINE_QUEUENAME"]


class CleanUpJobTest(testutil.HandlerTestBase):
  """Tests cleaning up jobs."""

  def setUp(self):
    """Sets up the test harness."""
    testutil.HandlerTestBase.setUp(self)

    TestKind().put()
    self.mapreduce_id = control.start_map(
        "my job 1",
        "__main__.TestMap",
        "mapreduce.input_readers.DatastoreInputReader",
        {"entity_kind": "__main__.TestKind"},
        4)

    self.handler = handlers.CleanUpJobHandler()
    self.handler.initialize(mock_webapp.MockRequest(),
                            mock_webapp.MockResponse())
    self.handler.request.path = "/mapreduce/command/clean_up_job"

    self.handler.request.headers["X-Requested-With"] = "XMLHttpRequest"

  def KickOffMapreduce(self):
    """Executes pending kickoff task."""
    kickoff_task = self.taskqueue.GetTasks("default")[0]
    handler = handlers.KickOffJobHandler()
    handler.initialize(mock_webapp.MockRequest(), mock_webapp.MockResponse())
    handler.request.path = "/mapreduce/kickoffjob_callback"
    handler.request.params.update(
        cgi.parse_qsl(base64.b64decode(kickoff_task["body"])))
    handler.request.headers["X-AppEngine-QueueName"] = "default"
    handler.post()
    self.taskqueue.DeleteTask("default", kickoff_task["name"])

  def testCSRF(self):
    """Test that we check the X-Requested-With header."""
    del self.handler.request.headers["X-Requested-With"]
    self.handler.post()
    self.assertEquals(403, self.handler.response.status)

  def testBasic(self):
    """Tests cleaning up the job.

    Note: This cleans up a running mapreduce, but that's okay because
    the prohibition against doing so is done on the client side.
    """
    self.KickOffMapreduce()
    key = model.MapreduceState.get_key_by_job_id(self.mapreduce_id)
    self.assertTrue(db.get(key))
    self.handler.request.set("mapreduce_id", self.mapreduce_id)
    self.handler.post()
    result = simplejson.loads(self.handler.response.out.getvalue())
    self.assertEquals({"status": ("Job %s successfully cleaned up." %
                                  self.mapreduce_id) },
                      result)
    self.assertFalse(db.get(key))


if __name__ == "__main__":
  unittest.main()
