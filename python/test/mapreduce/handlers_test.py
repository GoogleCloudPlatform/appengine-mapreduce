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

# os_compat must be first to ensure timezones are UTC.
# Disable "unused import" and "invalid import order"
# pylint: disable=unused-import
from google.appengine.tools import os_compat
# testutil must be imported before mock.
from testlib import testutil
# pylint: enable=unused-import
# pylint: disable=unused-argument

import base64
import datetime
import httplib
import mock
from testlib import mox
import os
from mapreduce.lib import simplejson
import time
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore
from google.appengine.api import datastore_errors
from google.appengine.api import datastore_file_stub
from google.appengine.api import files
from google.appengine.api import logservice
from google.appengine.api import memcache
from google.appengine.api import taskqueue
from google.appengine.api.memcache import memcache_stub
from google.appengine.api.taskqueue import taskqueue_stub
from google.appengine.ext import db
from google.appengine.ext import key_range
from google.appengine.ext import testbed
from mapreduce import context
from mapreduce import control
from mapreduce import datastore_range_iterators as db_iters
from mapreduce import errors
from mapreduce import handlers
from mapreduce import hooks
from mapreduce import input_readers
from mapreduce import key_ranges
from mapreduce import model
from mapreduce import operation
from mapreduce import output_writers
from mapreduce import test_support
from google.appengine.ext.webapp import mock_webapp


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
    task.add(queue_name=queue_name)

  def enqueue_kickoff_task(self, task, queue_name):
    # Tested by control_test.ControlTest.testStartMap_Hooks.
    task.add(queue_name=queue_name)

  def enqueue_done_task(self, task, queue_name):
    self.enqueue_done_task_calls.append((task, queue_name))
    task.add(queue_name=queue_name)

  def enqueue_controller_task(self, task, queue_name):
    self.enqueue_controller_task_calls.append((task, queue_name))
    task.add(queue_name=queue_name)


class TestKind(db.Model):
  """Used for testing."""

  foobar = db.StringProperty(default="meep")


def TestMap(entity):
  """Used for testing."""
  pass


class MockTime(object):
  """Simple class to use for mocking time() function."""

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

  a = db.IntegerProperty(default=1)


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
  def reset(cls):
    cls.processed_keys = []


def test_handler_raise_exception(entity):
  """Test handler function which always raises exception.

  Raises:
    ValueError: always
  """
  raise ValueError()


def test_handler_raise_fail_job_exception(entity):
  """Test handler function which always raises exception.

  Raises:
    FailJobError: always.
  """
  raise errors.FailJobError()


def test_handler_raise_slice_retry_exception(entity):
  """Test handler function that always raises a fatal error.

  Raises:
    errors.RetrySliceError: always.
  """
  raise errors.RetrySliceError("")


def test_handler_raise_shard_retry_exception(entity):
  """Test handler function that always raises a fatal error.

  Raises:
    files.ExistenceError: always.
  """
  raise files.ExistenceError("")


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
  def split_input(cls, mapper_spec):
    """Split into the exact number of shards asked for."""
    shard_count = mapper_spec.shard_count
    query_spec = cls._get_query_spec(mapper_spec)

    k_ranges = [key_ranges.KeyRangesFactory.create_from_list(
        [key_range.KeyRange()]) for _ in range(shard_count)]
    iters = [db_iters.RangeIteratorFactory.create_key_ranges_iterator(
        r, query_spec, cls._KEY_RANGE_ITER_CLS) for r in k_ranges]

    return [cls(i) for i in iters]

  @classmethod
  def reset(cls):
    cls.yields = 0


class EmptyInputReader(input_readers.DatastoreInputReader):
  """Always returns nothing from input splits."""

  @classmethod
  def split_input(cls, mapper_spec):
    return None


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
  def create(cls, mapreduce_state, shard_state):
    assert isinstance(mapreduce_state, model.MapreduceState)
    cls.events.append("create-" + str(shard_state.shard_number))
    return cls()

  def to_json(self):
    return {}

  @classmethod
  def from_json(cls, json_dict):
    return cls()

  def write(self, data, ctx):
    assert isinstance(ctx, context.Context)
    self.events.append("write-" + str(data))

  def finalize(self, ctx, shard_state):
    assert isinstance(ctx, context.Context)
    self.events.append("finalize-" + str(shard_state.shard_number))


class UnfinalizableTestOutputWriter(TestOutputWriter):
  """An output writer where all calls to finalize fail."""

  def finalize(self, ctx, shard_state):
    raise Exception("This will always break")


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
  """Test reader which truncates the list of readers to specified size."""
  readers_size = 3

  @classmethod
  def split_input(cls, mapper_spec):
    readers = input_readers.DatastoreInputReader.split_input(mapper_spec)
    return readers[:cls.readers_size]


ENTITY_KIND = "__main__.TestEntity"
MAPPER_HANDLER_SPEC = __name__ + "." + TestHandler.__name__

COUNTER_MAPPER_CALLS = context.COUNTER_MAPPER_CALLS


class MapreduceHandlerTestBase(testutil.HandlerTestBase):
  """Base class for all mapreduce's HugeTaskHandler tests.

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

    request = mock_webapp.MockRequest()
    request.body = base64.b64decode(task["body"])
    request.headers = dict(task["headers"])

    payload = model.HugeTask.decode_payload(request)
    self.assertEquals(str(shard_id), payload["shard_id"])
    self.assertEquals(str(slice_id), payload["slice_id"])

    self.assertTrue(payload["mapreduce_spec"])
    mapreduce_spec = model.MapreduceSpec.from_json_str(
        payload["mapreduce_spec"])
    self.verify_mapreduce_spec(mapreduce_spec, **kwargs)

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
    self.assertEquals(
        ENTITY_KIND,
        mapreduce_spec.mapper.params["input_reader"]["entity_kind"])
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

    self.assertEqual(kwargs.get("active", True), shard_state.active)
    self.assertEqual(kwargs.get("processed", 0),
                     shard_state.counters_map.get(COUNTER_MAPPER_CALLS))
    self.assertEqual(kwargs.get("result_status", None),
                     shard_state.result_status)
    self.assertEqual(kwargs.get("slice_retries", 0),
                     shard_state.slice_retries)

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

    request = mock_webapp.MockRequest()
    request.body = base64.b64decode(task["body"])
    request.headers = dict(task["headers"])

    payload = model.HugeTask.decode_payload(request)
    mapreduce_spec = model.MapreduceSpec.from_json_str(
        payload["mapreduce_spec"])
    self.verify_mapreduce_spec(mapreduce_spec, **kwargs)

  def create_mapreduce_spec(self,
                            mapreduce_id,
                            shard_count=8,
                            mapper_handler_spec=MAPPER_HANDLER_SPEC,
                            mapper_parameters=None,
                            hooks_class_name=None,
                            output_writer_spec=None,
                            input_reader_spec=None):
    """Create a new valid mapreduce_spec.

    Args:
      mapreduce_id: mapreduce id.
      shard_count: number of shards in the handlers.
      mapper_handler_spec: handler specification to use for handlers.
      hooks_class_name: fully qualified name of the hooks class.

    Returns:
      new MapreduceSpec.
    """
    params = {
        "input_reader": {
            "entity_kind": __name__ + "." + TestEntity.__name__
        },
    }
    if mapper_parameters is not None:
      params.update(mapper_parameters)

    mapper_spec = model.MapperSpec(
        mapper_handler_spec,
        input_reader_spec or __name__ + ".InputReader",
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


class StartJobHandlerTest(testutil.HandlerTestBase):
  """Test handlers.StartJobHandler."""

  def setUp(self):
    """Sets up the test harness."""
    super(StartJobHandlerTest, self).setUp()
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
    payload = test_support.decode_task_payload(task)
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
    payload = test_support.decode_task_payload(tasks[0])
    self.assertEquals("otherapp", payload["app"])
    self.assertTrue(self.get_mapreduce_spec(tasks[0]))

  def testRequiredParams(self):
    """Tests that required parameters are enforced."""
    TestEntity().put()
    self.handler.post()

    self.handler.request.set("name", None)
    self.assertRaises(errors.NotEnoughArgumentsError, self.handler.handle)

    self.handler.request.set("name", "my job")
    self.handler.request.set("mapper_input_reader", None)
    self.assertRaises(errors.NotEnoughArgumentsError, self.handler.handle)

    self.handler.request.set(
        "mapper_input_reader",
        "mapreduce.input_readers.DatastoreInputReader")
    self.handler.request.set("mapper_handler", None)
    self.assertRaises(errors.NotEnoughArgumentsError, self.handler.handle)

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

  def enqueue_task_and_run(self, queue="default", app=None):
    handlers.StartJobHandler._add_kickoff_task(
        "/mapreduce", self.mapreduce_spec, None, None, None, queue, False, app)
    tasks = self.taskqueue.GetTasks(queue)
    self.assertEqual(1, len(tasks))
    self.taskqueue.FlushQueue(queue)
    self.handler = test_support.execute_task(tasks[0])

  def testSmoke(self):
    """Verifies main execution path of starting scan over several entities."""
    for i in range(100):
      TestEntity().put()
    self.enqueue_task_and_run()
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
    self.enqueue_task_and_run()

    self.assertEquals(8, len(TestHooks.enqueue_worker_task_calls))
    self.assertEquals(1, len(TestHooks.enqueue_controller_task_calls))
    task, queue_name = TestHooks.enqueue_controller_task_calls[0]
    self.assertEquals("default", queue_name)
    self.assertEquals("/mapreduce/controller_callback", task.url)

  def testInputReaderUnknown(self):
    """Tests when the input reader function cannot be found."""
    self.mapreduce_spec.mapper.input_reader_spec = "does_not_exist"

    self.assertRaises(ImportError, self.enqueue_task_and_run)

  def testQueueName(self):
    """Tests that the optional queue_name parameter is used."""
    os.environ["HTTP_X_APPENGINE_QUEUENAME"] = "crazy-queue"
    TestEntity().put()
    self.enqueue_task_and_run(queue="crazy-queue")
    del os.environ["HTTP_X_APPENGINE_QUEUENAME"]

    self.assertEquals(0, len(self.taskqueue.GetTasks("default")))
    self.assertEquals(9, len(self.taskqueue.GetTasks("crazy-queue")))

  def testNoInputReader(self):
    """Test split input returned no input reader."""
    self.mapreduce_spec = self.create_mapreduce_spec(
        self.mapreduce_id,
        input_reader_spec=__name__ + "." + "EmptyInputReader")
    self.enqueue_task_and_run()

    state = model.MapreduceState.get_by_job_id(self.mapreduce_id)
    self.assertFalse(state.active)
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS, state.result_status)
    self.assertEqual(0, state.active_shards)

  def testNoData(self):
    """Test no data exists for input reader to consume."""
    self.enqueue_task_and_run()
    self.assertEquals(9, len(self.taskqueue.GetTasks("default")))

    state = model.MapreduceState.get_by_job_id(self.mapreduce_id)
    self.assertTrue(state.active)
    self.assertEquals(8, state.active_shards)

  def testDifferentShardCount(self):
    """Verifies the case when input reader created diffrent shard number."""
    for _ in range(100):
      TestEntity().put()
    self.mapreduce_spec.mapper.input_reader_spec = (
        __name__ + ".FixedShardSizeInputReader")
    self.enqueue_task_and_run()

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
    self.enqueue_task_and_run(app="otherapp")

    state = model.MapreduceState.all()[0]
    self.assertTrue(state)
    self.assertEquals("otherapp", state.app_id)

  def testOutputWriter(self):
    """Test output writer initialization."""
    for _ in range(100):
      TestEntity().put()

    self.mapreduce_spec.mapper.output_writer_spec = (
        __name__ + ".TestOutputWriter")
    self.enqueue_task_and_run()

    self.assertEquals(
        ["init_job", "create-0", "create-1", "create-2", "create-3",
         "create-4", "create-5", "create-6", "create-7",],
        TestOutputWriter.events)


class MapperWorkerCallbackHandlerLeaseTest(unittest.TestCase):
  """Test lease related logics of handlers.MapperWorkerCallbackHandler.

  These tests creates a WorkerHandler for the same shard.
  WorkerHandler gets a payload that's (in)consistent with datastore's
  ShardState in some way.
  """

  # This shard's number.
  SHARD_NUMBER = 1
  # Current slice id in datastore.
  CURRENT_SLICE_ID = 3
  CURRENT_REQUEST_ID = "20150131a"
  # Request id from the previous slice execution.
  PREVIOUS_REQUEST_ID = "19991231a"

  def setUp(self):
    super(MapperWorkerCallbackHandlerLeaseTest, self).setUp()
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_files_stub()
    self.testbed.init_taskqueue_stub()
    self.testbed.init_logservice_stub()

    os.environ["REQUEST_LOG_ID"] = self.CURRENT_REQUEST_ID

    self.mr_spec = None
    self._init_job()

    self.shard_id = None
    self.shard_state = None
    self._init_shard()

    self._original_duration = handlers._SLICE_DURATION_SEC
    # Make sure handler can process at most one entity and thus
    # shard will still be in active state after one call.
    handlers._SLICE_DURATION_SEC = 0

  def tearDown(self):
    self.testbed.deactivate()
    handlers._SLICE_DURATION_SEC = self._original_duration
    super(MapperWorkerCallbackHandlerLeaseTest, self).tearDown()

  def _init_job(self, handler_spec=MAPPER_HANDLER_SPEC):
    """Init job specs."""
    mapper_spec = model.MapperSpec(
        handler_spec=handler_spec,
        input_reader_spec=__name__ + "." + InputReader.__name__,
        params={"entity_kind": ENTITY_KIND},
        shard_count=self.SHARD_NUMBER)
    self.mr_spec = model.MapreduceSpec(
        name="mapreduce_name",
        mapreduce_id="mapreduce_id",
        mapper_spec=mapper_spec.to_json())

  def _init_shard(self):
    """Init shard state."""
    self.shard_state = model.ShardState.create_new(
        self.mr_spec.mapreduce_id,
        shard_number=self.SHARD_NUMBER)
    self.shard_state.slice_id = self.CURRENT_SLICE_ID
    self.shard_state.put()
    self.shard_id = self.shard_state.shard_id

  def _create_handler(self, slice_id=CURRENT_SLICE_ID):
    """Create a handler instance with payload for a particular slice."""
    # Reset map handler.
    TestHandler.reset()

    # Create input reader and test entity.
    InputReader.reset()
    TestEntity().put()
    TestEntity().put()
    reader_iter = db_iters.RangeIteratorFactory.create_key_ranges_iterator(
        key_ranges.KeyRangesFactory.create_from_list([key_range.KeyRange()]),
        model.QuerySpec("ENTITY_KIND", model_class_path=ENTITY_KIND),
        db_iters.KeyRangeModelIterator)

    # Create worker handler.
    handler = handlers.MapperWorkerCallbackHandler()
    request = mock_webapp.MockRequest()
    request.headers[model.HugeTask.PAYLOAD_VERSION_HEADER] = (
        model.HugeTask.PAYLOAD_VERSION)
    handler.initialize(request, mock_webapp.MockResponse())
    handler.request.headers["X-AppEngine-QueueName"] = "default"

    # Create transient shard state.
    tstate = model.TransientShardState(
        base_path="base_path",
        mapreduce_spec=self.mr_spec,
        shard_id=self.shard_state.shard_id,
        slice_id=slice_id,
        input_reader=InputReader(reader_iter),
        initial_input_reader=InputReader(reader_iter))

    # Set request according to transient shard state.
    worker_params = tstate.to_dict()
    for param_name in worker_params:
      handler.request.set(param_name, worker_params[param_name])
    return handler, tstate

  def assertNoEffect(self, no_shard_state=False):
    """Assert shard state and taskqueue didn't change."""
    stub = apiproxy_stub_map.apiproxy.GetStub("taskqueue")
    self.assertEqual(0, len(stub.GetTasks("default")))

    shard_state = model.ShardState.get_by_shard_id(self.shard_state.shard_id)

    if not no_shard_state:
      assert shard_state

    if shard_state:
      # sync auto_now field
      shard_state.update_time = self.shard_state.update_time
      self.assertEqual(str(self.shard_state), str(shard_state))

  def testStateNotFound(self):
    handler, _ = self._create_handler()
    self.shard_state.delete()
    handler.post()
    self.assertNoEffect(no_shard_state=True)
    self.assertEqual(None, model.ShardState.get_by_shard_id(self.shard_id))

  def testStateNotActive(self):
    handler, _ = self._create_handler()
    self.shard_state.active = False
    self.shard_state.put()
    handler.post()
    self.assertNoEffect()

  def testOldTask(self):
    handler, _ = self._create_handler(slice_id=self.CURRENT_SLICE_ID - 1)
    handler.post()
    self.assertNoEffect()

  def testFutureTask(self):
    handler, _ = self._create_handler(slice_id=self.CURRENT_SLICE_ID + 1)
    handler.post()
    self.assertEqual(httplib.SERVICE_UNAVAILABLE, handler.response.status)

  def testLeaseHasNotEnd(self):
    self.shard_state.slice_start_time = datetime.datetime.now()
    self.shard_state.put()
    handler, _ = self._create_handler()

    with mock.patch("datetime.datetime", autospec=True) as dt:
      # One millisecons after.
      dt.now.return_value = (self.shard_state.slice_start_time +
                             datetime.timedelta(milliseconds=1))
      self.assertEqual(
          handlers._LEASE_GRACE_PERIOD + handlers._SLICE_DURATION_SEC,
          handler._wait_time(self.shard_state,
                             handlers._LEASE_GRACE_PERIOD +
                             handlers._SLICE_DURATION_SEC))
      handler.post()
      self.assertEqual(httplib.SERVICE_UNAVAILABLE, handler.response.status)

  def testRequestHasNotEnd(self):
    # Previous request's lease has timed out but the request has not.
    now = datetime.datetime.now()
    old = (now -
           datetime.timedelta(seconds=handlers._LEASE_GRACE_PERIOD +
                              handlers._SLICE_DURATION_SEC +
                              1))
    self.shard_state.slice_start_time = old
    self.shard_state.slice_request_id = self.PREVIOUS_REQUEST_ID
    self.shard_state.put()
    handler, _ = self._create_handler()
    # Lease has ended.
    self.assertEqual(0,
                     handler._wait_time(self.shard_state,
                                        handlers._LEASE_GRACE_PERIOD +
                                        handlers._SLICE_DURATION_SEC),
                     lambda: now)
    # Logs API doesn't think the request has ended.
    self.assertFalse(handler._old_request_ended(self.shard_state))
    # Request has not timed out.
    self.assertTrue(handler._wait_time(self.shard_state,
                                       handlers._REQUEST_EVENTUAL_TIMEOUT,
                                       lambda: now))
    handler.post()
    self.assertEqual(httplib.SERVICE_UNAVAILABLE, handler.response.status)

  def testRequestHasTimedOut(self):
    self.shard_state.slice_start_time = datetime.datetime(2000, 1, 1)
    self.shard_state.slice_request_id = self.PREVIOUS_REQUEST_ID
    self.shard_state.put()
    handler, tstate = self._create_handler()
    # Lease has ended.
    self.assertEqual(0,
                     handler._wait_time(self.shard_state,
                                        handlers._LEASE_GRACE_PERIOD +
                                        handlers._SLICE_DURATION_SEC))
    # Logs API doesn't think the request has ended.
    self.assertFalse(handler._old_request_ended(self.shard_state))
    # But request has timed out.
    self.assertEqual(0, handler._wait_time(self.shard_state,
                                           handlers._REQUEST_EVENTUAL_TIMEOUT))
    # acquire lease should succeed.
    handler._try_acquire_lease(self.shard_state, tstate)

    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    self.assertTrue(shard_state.active)
    self.assertEqual(self.CURRENT_SLICE_ID, shard_state.slice_id)
    self.assertEqual(self.CURRENT_REQUEST_ID, shard_state.slice_request_id)
    self.assertTrue(shard_state.slice_start_time >
                    self.shard_state.slice_start_time)

  def testContentionWhenAcquireLease(self):
    # Shard has moved on AFTER we got shard state.
    self.shard_state.slice_id += 1
    self.shard_state.put()

    # Revert in memory shard state.
    self.shard_state.slice_id -= 1
    handler, tstate = self._create_handler()
    self.assertEqual(
        handler._TASK_STATE.RETRY_TASK,
        # Use old shard state.
        handler._try_acquire_lease(self.shard_state, tstate))

  def testAcquireLeaseSuccess(self):
    # lease acquired a long time ago.
    self.shard_state.slice_start_time = datetime.datetime(2000, 1, 1)
    self.shard_state.slice_request_id = self.PREVIOUS_REQUEST_ID
    self.shard_state.put()
    handler, tstate = self._create_handler()
    with mock.patch("google.appengine.api"
                    ".logservice.fetch") as fetch:
      mock_request_log = mock.Mock()
      mock_request_log.finished = True
      fetch.return_value = [mock_request_log]
      handler._try_acquire_lease(self.shard_state, tstate)

    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    self.assertTrue(shard_state.active)
    self.assertEqual(self.CURRENT_SLICE_ID, shard_state.slice_id)
    self.assertEqual(self.CURRENT_REQUEST_ID, shard_state.slice_request_id)
    self.assertTrue(shard_state.slice_start_time >
                    self.shard_state.slice_start_time)

  def testAcquireLeaseSuccessWithServer(self):
    """Same as testAcquireLeaseSuccess but fetchs log with server options."""
    # lease acquired a long time ago.
    self.shard_state.slice_start_time = datetime.datetime(2000, 1, 1)
    self.shard_state.slice_request_id = self.PREVIOUS_REQUEST_ID
    self.shard_state.put()
    handler, tstate = self._create_handler()
    with mock.patch("google.appengine.api.logservice"
                    ".fetch", autospec=True) as fetch:
      mock_request_log = mock.Mock()
      mock_request_log.finished = True
      def side_effect(*args, **kwds):
        if "module_versions" in kwds:
          return [mock_request_log]
        raise logservice.InvalidArgumentError()
      fetch.side_effect = side_effect
      handler._try_acquire_lease(self.shard_state, tstate)

    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    self.assertTrue(shard_state.active)
    self.assertEqual(self.CURRENT_SLICE_ID, shard_state.slice_id)
    self.assertEqual(self.CURRENT_REQUEST_ID, shard_state.slice_request_id)
    self.assertTrue(shard_state.slice_start_time >
                    self.shard_state.slice_start_time)

  def testLeaseFreedOnSuccess(self):
    self.shard_state.slice_start_time = datetime.datetime(2000, 1, 1)
    self.shard_state.slice_request_id = self.PREVIOUS_REQUEST_ID
    self.shard_state.put()
    handler, _ = self._create_handler()
    with mock.patch("google.appengine.api"
                    ".logservice.fetch") as fetch:
      mock_request_log = mock.Mock()
      mock_request_log.finished = True
      fetch.return_value = [mock_request_log]
      handler.post()

    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    self.assertTrue(shard_state.active)
    # Slice moved on.
    self.assertEquals(self.CURRENT_SLICE_ID + 1, shard_state.slice_id)
    # Lease is freed.
    self.assertFalse(shard_state.slice_start_time)
    self.assertFalse(shard_state.slice_request_id)
    stub = apiproxy_stub_map.apiproxy.GetStub("taskqueue")
    self.assertEqual(1, len(stub.GetTasks("default")))

  def testLeaseFreedOnSliceRetry(self):
    # Reinitialize with faulty map function.
    self._init_job(__name__ + "." + test_handler_raise_exception.__name__)
    self._init_shard()
    handler, _ = self._create_handler()
    handler.post()
    self.assertEqual(httplib.SERVICE_UNAVAILABLE, handler.response.status)

    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    self.assertTrue(shard_state.active)
    # Slice stays the same.
    self.assertEquals(self.CURRENT_SLICE_ID, shard_state.slice_id)
    # Lease is freed.
    self.assertFalse(shard_state.slice_start_time)
    self.assertFalse(shard_state.slice_request_id)
    # Slice retry is increased.
    self.assertEqual(self.shard_state.slice_retries + 1,
                     shard_state.slice_retries)

  def testLeaseFreedOnTaskqueueUnavailable(self):
    handler, _ = self._create_handler()
    with mock.patch("mapreduce"
                    ".handlers.MapperWorkerCallbackHandler._add_task") as add:
      add.side_effect = taskqueue.Error
      self.assertRaises(taskqueue.Error, handler.post)

    # No new task in taskqueue.
    stub = apiproxy_stub_map.apiproxy.GetStub("taskqueue")
    self.assertEqual(0, len(stub.GetTasks("default")))

    shard_state = model.ShardState.get_by_shard_id(self.shard_state.shard_id)
    self.assertTrue(shard_state.acquired_once)
    # Besides these fields, all other fields should be the same.
    shard_state.acquired_once = self.shard_state.acquired_once
    shard_state.update_time = self.shard_state.update_time
    self.assertEqual(str(self.shard_state), str(shard_state))


class MapperWorkerCallbackHandlerTest(MapreduceHandlerTestBase):
  """Test handlers.MapperWorkerCallbackHandler."""

  def setUp(self):
    """Sets up the test harness."""
    MapreduceHandlerTestBase.setUp(self)
    self.original_task_add = taskqueue.Task.add
    self.original_slice_duration = handlers._SLICE_DURATION_SEC
    self.init()

  def tearDown(self):
    handlers._TEST_INJECTED_FAULTS.clear()
    taskqueue.Task.add = self.original_task_add
    handlers._SLICE_DURATION_SEC = self.original_slice_duration
    MapreduceHandlerTestBase.tearDown(self)

  def init(self,
           mapper_handler_spec=MAPPER_HANDLER_SPEC,
           mapper_parameters=None,
           hooks_class_name=None,
           output_writer_spec=None,
           shard_count=8):
    """Init everything needed for testing worker callbacks.

    Args:
      mapper_handler_spec: handler specification to use in test.
      mapper_params: mapper specification to use in test.
      hooks_class_name: fully qualified name of the hooks class to use in test.
    """
    InputReader.reset()
    self.handler = handlers.MapperWorkerCallbackHandler()
    self.handler._time = MockTime.time
    request = mock_webapp.MockRequest()
    request.headers[model.HugeTask.PAYLOAD_VERSION_HEADER] = (
        model.HugeTask.PAYLOAD_VERSION)
    self.handler.initialize(request,
                            mock_webapp.MockResponse())
    self.handler.request.path = "/mapreduce/worker_callback"

    self.mapreduce_id = "mapreduce0"
    self.mapreduce_spec = self.create_mapreduce_spec(
        self.mapreduce_id,
        shard_count,
        mapper_handler_spec=mapper_handler_spec,
        hooks_class_name=hooks_class_name,
        output_writer_spec=output_writer_spec,
        mapper_parameters=mapper_parameters)
    self.shard_number = 1
    self.slice_id = 0

    self.shard_state = self.create_and_store_shard_state(
        self.mapreduce_id, self.shard_number)
    self.shard_id = self.shard_state.shard_id

    output_writer = None
    if self.mapreduce_spec.mapper.output_writer_class():
      output_writer = self.mapreduce_spec.mapper.output_writer_class()()

    reader_iter = db_iters.RangeIteratorFactory.create_key_ranges_iterator(
        key_ranges.KeyRangesFactory.create_from_list([key_range.KeyRange()]),
        model.QuerySpec("ENTITY_KIND", model_class_path=ENTITY_KIND),
        db_iters.KeyRangeModelIterator)
    self.transient_state = model.TransientShardState(
        "/mapreduce",
        self.mapreduce_spec,
        self.shard_id,
        self.slice_id,
        InputReader(reader_iter),
        InputReader(reader_iter),
        output_writer=output_writer
        )

    worker_params = self.transient_state.to_dict()
    for param_name in worker_params:
      self.handler.request.set(param_name, worker_params[param_name])

    self.handler.request.headers["X-AppEngine-QueueName"] = "default"

  def testCSRF(self):
    """Tests that that handler only accepts requests from the task queue."""
    del self.handler.request.headers["X-AppEngine-QueueName"]
    self.handler.post()
    self.assertEquals(403, self.handler.response.status)

  def testSmoke(self):
    """Test main execution path of entity scanning.

    No processing rate limit.
    """
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

    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(0, len(tasks))

  def testCompletedState(self):
    self.shard_state.active = False
    self.shard_state.put()

    e1 = TestEntity()
    e1.put()

    self.handler.post()

    # completed state => no data processed
    self.assertEquals([], TestHandler.processed_keys)
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id), active=False)
    self.assertEquals(0, len(self.taskqueue.GetTasks("default")))

  def testShardStateCollision(self):
    handlers._TEST_INJECTED_FAULTS.add("worker_active_state_collision")

    e1 = TestEntity()
    e1.put()

    self.handler.post()

    # Data will still be processed
    self.assertEquals([str(e1.key())], TestHandler.processed_keys)
    # Shard state should not be overriden, i.e. left active.
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id), active=True)
    self.assertEquals(0, len(self.taskqueue.GetTasks("default")))

  def testNoShardState(self):
    """Correct handling of missing shard state."""
    self.shard_state.delete()
    e1 = TestEntity()
    e1.put()

    self.handler.post()

    # no state => no data processed
    self.assertEquals([], TestHandler.processed_keys)
    self.assertEquals(0, len(self.taskqueue.GetTasks("default")))

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
    # Be sure to have an output writer for the abort step so we can confirm
    # that the finalize() method is never called.
    self.init(__name__ + ".test_handler_yield_keys",
              output_writer_spec=__name__ + ".UnfinalizableTestOutputWriter")

    model.MapreduceControl.abort(self.mapreduce_id, force_writes=True)
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

    # slice should be still active
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        processed=1)

    # new task should be spawned
    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    self.verify_shard_task(tasks[0], self.shard_id, self.slice_id + 1)
    self.assertEquals(tasks[0]['eta_delta'], '0:00:00 ago')

  def testLimitingRate(self):
    """Test not enough quota to process everything in this slice."""
    e1 = TestEntity()
    e1.put()

    e2 = TestEntity()
    e2.put()

    e3 = TestEntity()
    e3.put()

    # Everytime the handler is called, it increases time by this amount.
    TestHandler.delay = handlers._SLICE_DURATION_SEC/2 - 1
    # handler should be called twice.
    self.init(mapper_parameters={"processing_rate":
        10.0/handlers._SLICE_DURATION_SEC}, shard_count=5)

    self.handler.post()

    self.assertEquals(2, len(TestHandler.processed_keys))
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=True, processed=2,
        result_status=None)

    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    self.assertEquals(tasks[0]['eta_delta'], '0:00:02 from now')

  def testLongProcessDataWithAllowCheckpoint(self):
    """Tests that process_data works with input_readers.ALLOW_CHECKPOINT."""
    self.handler._start_time = 0
    self.assertFalse(self.handler.process_data(input_readers.ALLOW_CHECKPOINT,
                                               None,
                                               None,
                                               None))

  def testScheduleSlice(self):
    """Test _schedule_slice method."""
    self.handler._schedule_slice(
        self.shard_state,
        model.TransientShardState(
            "/mapreduce", self.mapreduce_spec,
            self.shard_id, 123, mock.Mock(), mock.Mock()))

    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    self.verify_shard_task(tasks[0], self.shard_id, 123)

  def testScheduleSlice_Eta(self):
    """Test _schedule_slice method."""
    eta = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
    self.handler._schedule_slice(
        self.shard_state,
        model.TransientShardState(
            "/mapreduce", self.mapreduce_spec,
            self.shard_id, 123, mock.Mock(), mock.Mock()),
        eta=eta)

    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    self.verify_shard_task(tasks[0], self.shard_id, 123, eta=eta)

  def testScheduleSlice_Countdown(self):
    """Test _schedule_slice method."""
    countdown = 60 * 60
    self.handler._schedule_slice(
        self.shard_state,
        model.TransientShardState(
            "/mapreduce", self.mapreduce_spec,
            self.shard_id, 123, mock.Mock(), mock.Mock()),
        countdown=countdown)

    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    self.verify_shard_task(tasks[0], self.shard_id, 123, countdown=countdown)

  def testScheduleSlice_QueuePreserved(self):
    """Tests that _schedule_slice will enqueue tasks on the calling queue."""
    os.environ["HTTP_X_APPENGINE_QUEUENAME"] = "crazy-queue"
    try:
      self.handler._schedule_slice(
          self.shard_state,
          model.TransientShardState(
              "/mapreduce", self.mapreduce_spec,
              self.shard_id, 123, mock.Mock(), mock.Mock()))

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

    self.assertEquals(1, len(self.taskqueue.GetTasks("default")))
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

    self.handler._schedule_slice(
        self.shard_state,
        model.TransientShardState(
            "/mapreduce", self.mapreduce_spec,
            self.shard_id, 123, mock.Mock(), mock.Mock()))

    tasks = self.taskqueue.GetTasks("default")
    self.assertEquals(1, len(tasks))
    self.verify_shard_task(tasks[0], self.shard_id, 123,
                           hooks_class_name=hooks_class_name)

  def testDatastoreExceptionInHandler(self):
    """Test when a handler can't save state to datastore."""
    self.init(__name__ + ".test_handler_yield_keys")
    TestEntity().put()
    original_method = datastore.PutAsync
    datastore.PutAsync = mock.MagicMock(side_effect=datastore_errors.Timeout())

    # Tests that handler doesn't abort task for datastore errors.
    # Unfornately they still increase TaskExecutionCount.
    for _ in range(handlers._RETRY_SLICE_ERROR_MAX_RETRIES + 1):
      self.assertRaises(datastore_errors.Timeout, self.handler.post)
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=True,
        processed=0)

    datastore.PutAsync = original_method
    self.handler.post()

    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=False,
        result_status=model.ShardState.RESULT_SUCCESS,
        processed=1)

  def testTaskqueueExceptionInHandler(self):
    """Test when a handler can't reach taskqueue."""
    self.init(__name__ + ".test_handler_yield_keys")
    # Force enqueue another task.
    handlers._SLICE_DURATION_SEC = 0
    TestEntity().put()
    taskqueue.Task.add = mock.MagicMock(side_effect=taskqueue.TransientError)

    # Tests that handler doesn't abort task for taskqueue errors.
    # Unfornately they still increase TaskExecutionCount.
    for _ in range(handlers._RETRY_SLICE_ERROR_MAX_RETRIES + 1):
      self.assertRaises(taskqueue.TransientError, self.handler.post)
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=True,
        processed=0)

    taskqueue.Task.add = self.original_task_add
    self.handler.post()

    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=True,
        result_status=None,
        processed=1)

  def testSliceRetryExceptionInHandler(self):
    """Test when a handler throws a fatal exception."""
    self.init(__name__ + ".test_handler_raise_slice_retry_exception")
    TestEntity().put()

    # First time, the task gets retried.
    self.handler.post()
    self.assertEqual(httplib.SERVICE_UNAVAILABLE, self.handler.response.status)
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=True,
        processed=0,
        slice_retries=1)

    # After the Nth attempt, we abort the whole job.
    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    shard_state.slice_retries = handlers._RETRY_SLICE_ERROR_MAX_RETRIES + 1
    shard_state.put()

    try:
      self.handler.post()
    finally:
      pass

    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=False,
        result_status=model.ShardState.RESULT_FAILED,
        processed=1,
        slice_retries=shard_state.slice_retries)

  def testSuccessfulSliceRetryClearsSliceRetriesCount(self):
    self.init(__name__ + ".test_handler_yield_op")
    TestEntity().put()
    TestEntity().put()
    # Force enqueue another task.
    handlers._SLICE_DURATION_SEC = 0

    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    shard_state.slice_retries = handlers._RETRY_SLICE_ERROR_MAX_RETRIES
    shard_state.put()

    self.handler.post()
    self.verify_shard_state(
        model.ShardState.get_by_shard_id(self.shard_id),
        active=True,
        processed=1,
        slice_retries=0)

  def testShardRetryExceptionInHandler(self):
    """Test when a handler throws a fatal exception."""
    self.init(__name__ + ".test_handler_raise_shard_retry_exception")
    TestEntity().put()

    self.handler.post()
    shard_state = model.ShardState.get_by_shard_id(self.shard_id)
    self.verify_shard_state(shard_state)
    self.assertEquals(1, shard_state.retries)
    self.assertEquals("", shard_state.last_work_item)

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
      self.handler.post()
      self.assertEqual(httplib.SERVICE_UNAVAILABLE,
                       self.handler.response.status)

      # slice should be still active
      shard_state = model.ShardState.get_by_shard_id(self.shard_id)
      self.verify_shard_state(shard_state, processed=0, slice_retries=1)
      # mapper calls counter should not be incremented
      self.assertEquals(0, shard_state.counters_map.get(
          context.COUNTER_MAPPER_CALLS))

      # new task should not be spawned
      tasks = self.taskqueue.GetTasks("default")
      self.assertEquals(0, len(tasks))

      m.VerifyAll()
    finally:
      m.UnsetStubs()

  def testFailJobExceptionInHandler(self):
    """Test behavior when handler throws exception."""
    self.init(__name__ + ".test_handler_raise_fail_job_exception")
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
      self.handler.post()

      # slice should not be active
      shard_state = model.ShardState.get_by_shard_id(self.shard_id)
      self.verify_shard_state(
          shard_state,
          processed=1,
          active=False,
          result_status = model.ShardState.RESULT_FAILED)
      self.assertEquals(1, shard_state.counters_map.get(
          context.COUNTER_MAPPER_CALLS))

      # new task should not be spawned
      tasks = self.taskqueue.GetTasks("default")
      self.assertEquals(0, len(tasks))

      m.VerifyAll()
    finally:
      m.UnsetStubs()

  def testContext(self):
    """Test proper context initialization."""
    self.handler.request.headers["X-AppEngine-TaskExecutionCount"] = 5
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
    self.handler = handlers.ControllerCallbackHandler()
    self.handler._time = MockTime.time
    request = mock_webapp.MockRequest()
    request.headers[model.HugeTask.PAYLOAD_VERSION_HEADER] = (
        model.HugeTask.PAYLOAD_VERSION)
    self.handler.initialize(request,
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

  def testStateUpdateIsCmpAndSet(self):
    """Verify updating model.MapreduceState is cmp and set."""
    # Create shard states for 3 finished shards.
    shard_states = []
    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      shard_state.active = False
      shard_state.put()
      shard_states.append(shard_state)

    # MapreduceState.active is changed to False by another duplicated running
    # controller task.
    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    mapreduce_state.active = False
    mapreduce_state.result_status = model.MapreduceState.RESULT_SUCCESS
    mapreduce_state.put()

    # Invoke controller handler with stale mapreduce_state and shard_states.
    mapreduce_state.active = True
    mapreduce_state.result_status = None
    for s in shard_states:
      s.active = True
    self.handler._update_state_from_shard_states(mapreduce_state,
                                                 shard_states,
                                                 None)

    # Make sure we did't overwrite active or result_status.
    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    self.verify_mapreduce_state(
        mapreduce_state,
        shard_count=3,
        active=False,
        result_status=model.MapreduceState.RESULT_SUCCESS)

    # New controller task will drop itself because it detected that
    # mapreduce_state.active is False.
    # It will enqueue a finalizejob callback to cleanup garbage.
    self.handler.post()
    tasks = self.taskqueue.GetTasks("default")
    self.assertEqual(1, len(tasks))
    self.assertEqual("/mapreduce/finalizejob_callback", tasks[0]["url"])

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
        3, len(model.ShardState.find_by_mapreduce_state(mapreduce_state)))

  def testShardsDoneFinalizeOutputWriter(self):
    self.mapreduce_state.mapreduce_spec.mapper.output_writer_spec = (
        __name__ + '.' + TestOutputWriter.__name__)
    self.mapreduce_state.put()
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
    self.mapreduce_state.put()
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

  def testShardFailure(self):
    """Tests that when one shard fails the job will be aborted."""
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

    # Abort signal should be present.
    self.assertEquals(
        model.MapreduceControl.ABORT,
        db.get(model.MapreduceControl.get_key_by_job_id(
            self.mapreduce_id)).command)

  def testShardFailureAllDone(self):
    """Tests that individual shard failure affects the job outcome."""
    for i in range(3):
      shard_state = self.create_shard_state(self.mapreduce_id, i)
      shard_state.active = False
      if i == 0:
        shard_state.result_status = model.ShardState.RESULT_FAILED
      elif i == 1:
        shard_state.result_status = model.ShardState.RESULT_ABORTED
      else:
        shard_state.result_status = model.ShardState.RESULT_SUCCESS
      shard_state.put()

    self.handler.post()

    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    self.verify_mapreduce_state(
        mapreduce_state, active=False, shard_count=3,
        result_status=model.ShardState.RESULT_FAILED)
    self.assertEquals(1, mapreduce_state.failed_shards)
    self.assertEquals(1, mapreduce_state.aborted_shards)

    tasks = self.taskqueue.GetTasks("default")
    # Finalize task should be spawned.
    self.assertEquals(1, len(tasks))
    self.assertEquals("/mapreduce/finalizejob_callback", tasks[0]["url"])

    # Done Callback task should be spawned
    self.verify_done_task()

    self.assertEquals(
        3, len(model.ShardState.find_by_mapreduce_state(mapreduce_state)))

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

    # Force all shards to completion state (success, success, or abort).
    shard_state_list = model.ShardState.find_by_mapreduce_state(mapreduce_state)
    self.assertEquals(3, len(shard_state_list))
    shard_state_list[0].active = False
    shard_state_list[0].result_status = model.ShardState.RESULT_SUCCESS
    shard_state_list[1].active = False
    shard_state_list[1].result_status = model.ShardState.RESULT_SUCCESS
    shard_state_list[2].active = False
    shard_state_list[2].result_status = model.ShardState.RESULT_ABORTED
    db.put(shard_state_list)

    self.handler.post()
    mapreduce_state = model.MapreduceState.get_by_key_name(self.mapreduce_id)
    self.verify_mapreduce_state(
        mapreduce_state, active=False, shard_count=3,
        result_status=model.MapreduceState.RESULT_ABORTED)
    self.assertEquals(1, mapreduce_state.aborted_shards)

    tasks = self.taskqueue.GetTasks("default")
    # Finalize task should be spawned.
    self.assertEquals(1, len(tasks))
    self.assertEquals("/mapreduce/finalizejob_callback", tasks[0]["url"])

    # Done Callback task should be spawned
    self.verify_done_task()

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
    test_support.execute_all_tasks(self.taskqueue)

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
    self.assertFalse(model.ShardState.find_by_mapreduce_id(self.mapreduce_id))
    self.assertFalse(db.get(key))


if __name__ == "__main__":
  unittest.main()
