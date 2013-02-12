#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.




import unittest


from mapreduce.lib import pipeline
from google.appengine.api import files
from google.appengine.ext import db
from mapreduce import input_readers
from mapreduce import mapreduce_pipeline
from mapreduce import operation
from mapreduce import output_writers
from mapreduce import shuffler
from mapreduce import test_support
from testlib import testutil


class TestEntity(db.Model):
  """Test entity class."""
  data = db.TextProperty()


def test_combiner_map(entity):
  """Tests map handler for use with the Combiner test."""
  yield str(int(entity.data) % 4), entity.data


class TestCombiner(object):
  """Test combine handler."""
  invocations = []

  def __call__(self, key, values, combiner_values):
    self.invocations.append((key, values, combiner_values))

    value_ints = [int(x) for x in values]
    combiner_values_int = [int(x) for x in combiner_values]
    yield sum(value_ints + combiner_values_int)
    yield operation.counters.Increment("combiner-call")

  @classmethod
  def reset(cls):
    cls.invocations = []


def test_combiner_reduce(key, values):
  yield repr((key, sum([int(x) for x in values]))) + "\n"


class CombinerTest(testutil.HandlerTestBase):
  """Tests for combiners."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

    self.old_max_values_count = shuffler._MergePipeline._MAX_VALUES_COUNT
    shuffler._MergePipeline._MAX_VALUES_COUNT = 1

    TestCombiner.reset()

  def tearDown(self):
    shuffler._MergePipeline._MAX_VALUES_COUNT = self.old_max_values_count
    testutil.HandlerTestBase.tearDown(self)

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testNoCombiner(self):
    """Test running with low values count but without combiner."""
    # Even though this test doesn't have combiner specified, it's still
    # interesting to run. It forces MergePipeline to produce partial
    # key values and we verify that they are combined correctly in reader.

    # Prepare test data
    entity_count = 200

    for i in range(entity_count):
      TestEntity(data=str(i)).put()
      TestEntity(data=str(i)).put()

    p = mapreduce_pipeline.MapreducePipeline(
        "test",
        __name__ + ".test_combiner_map",
        __name__ + ".test_combiner_reduce",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=
        output_writers.__name__ + ".BlobstoreOutputWriter",
        mapper_params={
            "entity_kind": __name__ + ".TestEntity",
            },
        shards=4)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    p = mapreduce_pipeline.MapreducePipeline.from_id(p.pipeline_id)
    self.assertEquals(1, len(p.outputs.default.value))
    output_file = p.outputs.default.value[0]

    file_content = []
    with files.open(output_file, "r") as f:
      file_content = sorted(f.read(10000000).strip().split("\n"))

    self.assertEquals(
        ["('0', 9800)", "('1', 9900)", "('2', 10000)", "('3', 10100)"],
        file_content)

  def testCombiner(self):
    """Test running with low values count but with combiner."""
    # Prepare test data
    entity_count = 200

    for i in range(entity_count):
      TestEntity(data=str(i)).put()
      TestEntity(data=str(i)).put()

    p = mapreduce_pipeline.MapreducePipeline(
        "test",
        __name__ + ".test_combiner_map",
        __name__ + ".test_combiner_reduce",
        combiner_spec=__name__ + ".TestCombiner",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=
        output_writers.__name__ + ".BlobstoreOutputWriter",
        mapper_params={
            "entity_kind": __name__ + ".TestEntity",
            },
        shards=4)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    p = mapreduce_pipeline.MapreducePipeline.from_id(p.pipeline_id)
    self.assertEquals(1, len(p.outputs.default.value))
    output_file = p.outputs.default.value[0]

    file_content = []
    with files.open(output_file, "r") as f:
      file_content = sorted(f.read(10000000).strip().split("\n"))
    self.assertEquals(
        ["('0', 9800)", "('1', 9900)", "('2', 10000)", "('3', 10100)"],
        file_content)

    self.assertTrue(TestCombiner.invocations)

    for invocation in TestCombiner.invocations:
      key = invocation[0]
      values = invocation[1]
      combiner_values = invocation[2]
      self.assertTrue(key)
      self.assertTrue(values)
      self.assertEquals(1, len(values))
      self.assertTrue(int(values[0]) % 4 == int(key))


if __name__ == "__main__":
  unittest.main()

