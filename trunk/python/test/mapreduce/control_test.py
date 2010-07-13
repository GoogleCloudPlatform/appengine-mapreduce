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





import unittest

from google.appengine.ext import db
from mapreduce import control
from mapreduce import model
from testlib import testutil


class TestEntity(db.Model):
  """Test entity class."""


def test_handler(entity):
  """Test handler function."""
  pass


class ControlTest(testutil.HandlerTestBase):
  """Tests for control module."""

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
        queue_name="crazy-queue")

    self.assertTrue(mapreduce_id)
    self.assertEquals(shard_count + 1,
                      len(self.taskqueue.GetTasks("crazy-queue")))
    mapreduce_state = model.MapreduceState.all().fetch(limit=1)[0]
    self.assertTrue(mapreduce_state)
    self.assertEquals(mapreduce_id, mapreduce_state.key().id_or_name())

    mapreduce_spec = mapreduce_state.mapreduce_spec
    self.assertEquals({"foo": "bar"}, mapreduce_spec.params)

if __name__ == "__main__":
  unittest.main()
