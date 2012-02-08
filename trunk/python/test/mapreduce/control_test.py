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




import datetime
import os
import random
import string
import time
import unittest

from google.appengine.ext import db
from mapreduce import control
from mapreduce import hooks
from mapreduce import model
from mapreduce import test_support
from testlib import testutil


def random_string(length):
  """Generate a random string of given length."""
  return "".join(
      random.choice(string.letters + string.digits) for _ in range(length))


class TestEntity(db.Model):
  """Test entity class."""


class TestHooks(hooks.Hooks):
  """Test hooks class."""

  enqueue_kickoff_task_calls = []

  def enqueue_kickoff_task(self, task, queue_name):
    TestHooks.enqueue_kickoff_task_calls.append((task, queue_name))


def test_handler(entity):
  """Test handler function."""
  pass


class ControlTest(testutil.HandlerTestBase):
  """Tests for control module."""

  QUEUE_NAME = "crazy-queue"

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    TestHooks.enqueue_kickoff_task_calls = []

  def get_mapreduce_spec(self, task):
    """Get mapreduce spec form kickoff task payload."""
    payload = test_support.decode_task_payload(task)
    return model.MapreduceSpec.from_json_str(payload["mapreduce_spec"])

  def validate_map_started(self, mapreduce_id, queue_name=None):
    """Tests that the map has been started."""
    queue_name = queue_name or self.QUEUE_NAME
    self.assertTrue(mapreduce_id)

    # Note: only a kickoff job is pending at this stage, shards come later.
    tasks = self.taskqueue.GetTasks(queue_name)
    self.assertEquals(1, len(tasks))
    # Checks that tasks are scheduled into the future.
    task = tasks[0]
    self.assertEquals("/mapreduce_base_path/kickoffjob_callback", task["url"])

    mapreduce_spec =self.get_mapreduce_spec(task)
    self.assertTrue(mapreduce_spec)
    self.assertEquals(mapreduce_id, mapreduce_spec.mapreduce_id)
    self.assertEquals({"foo": "bar"}, mapreduce_spec.params)

    return task["eta"]

  def testStartMap(self):
    """Test start_map function.

    Most of start_map functionality is already tested by handlers_test.
    Just a smoke test is enough.
    """
    TestEntity().put()

    shard_count = 4
    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".test_handler",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
        },
        shard_count,
        mapreduce_parameters={"foo": "bar"},
        base_path="/mapreduce_base_path",
        queue_name=self.QUEUE_NAME)

    self.validate_map_started(mapreduce_id)

  def testStartMap_QueueEnvironment(self):
    """Test that the start_map inherits its queue from the enviornment."""
    TestEntity().put()

    shard_count = 4
    os.environ["HTTP_X_APPENGINE_QUEUENAME"] = self.QUEUE_NAME
    try:
      mapreduce_id = control.start_map(
          "test_map",
          __name__ + ".test_handler",
          "mapreduce.input_readers.DatastoreInputReader",
          {
              "entity_kind": __name__ + "." + TestEntity.__name__,
          },
          shard_count,
          mapreduce_parameters={"foo": "bar"},
          base_path="/mapreduce_base_path")
    finally:
      del os.environ["HTTP_X_APPENGINE_QUEUENAME"]

    self.validate_map_started(mapreduce_id)

  def testStartMap_Cron(self):
    """Test that the start_map works from cron."""
    TestEntity().put()

    shard_count = 4
    os.environ["HTTP_X_APPENGINE_QUEUENAME"] = "__cron"
    try:
      mapreduce_id = control.start_map(
          "test_map",
          __name__ + ".test_handler",
          "mapreduce.input_readers.DatastoreInputReader",
          {
              "entity_kind": __name__ + "." + TestEntity.__name__,
          },
          shard_count,
          mapreduce_parameters={"foo": "bar"},
          base_path="/mapreduce_base_path")
    finally:
      del os.environ["HTTP_X_APPENGINE_QUEUENAME"]

    self.validate_map_started(mapreduce_id, queue_name="default")

  def testStartMap_Countdown(self):
    """Test that MR can be scheduled into the future.

    Most of start_map functionality is already tested by handlers_test.
    Just a smoke test is enough.
    """
    TestEntity().put()

    # MR should be scheduled into the future.
    now_sec = long(time.time())

    shard_count = 4
    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".test_handler",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
        },
        shard_count,
        mapreduce_parameters={"foo": "bar"},
        base_path="/mapreduce_base_path",
        queue_name=self.QUEUE_NAME,
        countdown=1000)

    task_eta = self.validate_map_started(mapreduce_id)
    eta_sec = time.mktime(time.strptime(task_eta, "%Y/%m/%d %H:%M:%S"))
    self.assertTrue(now_sec + 1000 <= eta_sec)

  def testStartMap_Eta(self):
    """Test that MR can be scheduled into the future.

    Most of start_map functionality is already tested by handlers_test.
    Just a smoke test is enough.
    """
    TestEntity().put()

    # MR should be scheduled into the future.
    eta = datetime.datetime.utcnow() + datetime.timedelta(hours=1)

    shard_count = 4
    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".test_handler",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
        },
        shard_count,
        mapreduce_parameters={"foo": "bar"},
        base_path="/mapreduce_base_path",
        queue_name=self.QUEUE_NAME,
        eta=eta)

    task_eta = self.validate_map_started(mapreduce_id)
    self.assertEquals(eta.strftime("%Y/%m/%d %H:%M:%S"), task_eta)

  def testStartMap_Hooks(self):
    """Tests that MR can be scheduled with a hook class installed.

    Most of start_map functionality is already tested by handlers_test.
    Just a smoke test is enough.
    """
    TestEntity().put()

    shard_count = 4
    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".test_handler",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
        },
        shard_count,
        mapreduce_parameters={"foo": "bar"},
        base_path="/mapreduce_base_path",
        queue_name="crazy-queue",
        hooks_class_name=__name__+"."+TestHooks.__name__)

    self.assertTrue(mapreduce_id)
    task, queue_name = TestHooks.enqueue_kickoff_task_calls[0]
    self.assertEquals("/mapreduce_base_path/kickoffjob_callback", task.url)
    self.assertEquals("crazy-queue", queue_name)

  def testStartMap_RaisingHooks(self):
    """Tests that MR can be scheduled with a dummy hook class installed.

    The dummy hook class raises NotImplementedError for all method calls so the
    default scheduling logic should be used.

    Most of start_map functionality is already tested by handlers_test.
    Just a smoke test is enough.
    """
    TestEntity().put()

    shard_count = 4
    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".test_handler",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
        },
        shard_count,
        mapreduce_parameters={"foo": "bar"},
        base_path="/mapreduce_base_path",
        queue_name="crazy-queue",
        hooks_class_name=hooks.__name__+"."+hooks.Hooks.__name__)

    self.validate_map_started(mapreduce_id)

  def testStartMap_HugePayload(self):
    """Test start_map function.

    Most of start_map functionality is already tested by handlers_test.
    Just a smoke test is enough.
    """
    TestEntity().put()

    shard_count = 4
    mapreduce_id = ""

    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".test_handler",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
            "huge_parameter": random_string(900000)
        },
        shard_count,
        mapreduce_parameters={"foo": "bar"},
        base_path="/mapreduce_base_path",
        queue_name=self.QUEUE_NAME)
    self.validate_map_started(mapreduce_id)

  def testStartMapTransactional(self):
    """Test start_map function.

    Most of start_map functionality is already tested by handlers_test.
    Just a smoke test is enough.
    """
    TestEntity().put()

    shard_count = 4
    mapreduce_id = ""

    def tx():
      return control.start_map(
          "test_map",
          __name__ + ".test_handler",
          "mapreduce.input_readers.DatastoreInputReader",
          {
              "entity_kind": __name__ + "." + TestEntity.__name__,
          },
          shard_count,
          mapreduce_parameters={"foo": "bar"},
          base_path="/mapreduce_base_path",
          queue_name=self.QUEUE_NAME,
          transactional=True)
    mapreduce_id = db.run_in_transaction(tx)
    self.validate_map_started(mapreduce_id)

  def testStartMapTransactional_HugePayload(self):
    """Test start_map function.

    Most of start_map functionality is already tested by handlers_test.
    Just a smoke test is enough.
    """
    TestEntity().put()

    shard_count = 4
    mapreduce_id = ""

    def tx():
      root_entity = TestEntity()
      root_entity.put()
      return control.start_map(
          "test_map",
          __name__ + ".test_handler",
          "mapreduce.input_readers.DatastoreInputReader",
          {
              "entity_kind": __name__ + "." + TestEntity.__name__,
              "huge_parameter": random_string(900000)
          },
          shard_count,
          mapreduce_parameters={"foo": "bar"},
          base_path="/mapreduce_base_path",
          queue_name=self.QUEUE_NAME,
          transactional=True,
          transactional_parent=root_entity)
    mapreduce_id = db.run_in_transaction(tx)
    self.validate_map_started(mapreduce_id)


if __name__ == "__main__":
  unittest.main()
