#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.




# pylint: disable=g-bad-name

import datetime
import unittest


from mapreduce.lib import pipeline
from google.appengine.api import files
from google.appengine.ext import db
from mapreduce import context
from mapreduce import input_readers
from mapreduce import mapper_pipeline
from mapreduce import model
from mapreduce import output_writers
from mapreduce import test_support
from testlib import testutil


class TestEntity(db.Model):
  """Test entity class."""
  data = db.StringProperty()
  dt = db.DateTimeProperty(default=datetime.datetime(2000, 1, 1))


class TestOutputEntity(db.Model):
  """TestOutput entity class."""
  data = db.StringProperty()


class RetryCount(db.Model):
  """Use to keep track of slice/shard retries."""
  retries = db.IntegerProperty()


def test_slice_retry_map(entity):
  """Raise exception for 9 times when data is 100."""
  if entity.data == "100":
    retry_count = RetryCount.get_by_key_name(entity.data)
    if not retry_count:
      retry_count = RetryCount(key_name=entity.data, retries=0)
    if retry_count.retries < 9:
      retry_count.retries += 1
      retry_count.put()
      raise Exception()
  TestOutputEntity(key_name=entity.data, data=entity.data).put()


def test_shard_retry_map(entity):
  """Raise shard retry exception 3 times when data is 100."""
  if entity.data == "100":
    retry_count = RetryCount.get_by_key_name(entity.data)
    if not retry_count:
      retry_count = RetryCount(key_name=entity.data, retries=0)
    if retry_count.retries < 3:
      retry_count.retries += 1
      retry_count.put()
      raise files.FinalizationError()
  TestOutputEntity(key_name=entity.data, data=entity.data).put()


def test_shard_retry_too_many_map(entity):
  """Raise shard retry exception 4 times when data is 100."""
  if entity.data == "100":
    retry_count = RetryCount.get_by_key_name(entity.data)
    if not retry_count:
      retry_count = RetryCount(key_name=entity.data, retries=0)
    if retry_count.retries < 4:
      retry_count.retries += 1
      retry_count.put()
      raise files.FinalizationError()
  TestOutputEntity(key_name=entity.data, data=entity.data).put()


def test_map(entity):
  """Test map handler."""
  yield (entity.data, "")


def test_empty_handler(entity):
  """Test handler that does nothing."""
  pass


class CleanupPipelineTest(testutil.HandlerTestBase):
  """Tests for the CleanupPipeline class."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testCleanup_Flat(self):
    """Tests cleaning up a flat list of files."""
    # Prepare test data
    entity_count = 200

    for i in range(entity_count):
      TestEntity(data=str(i)).put()
      TestEntity(data=str(i)).put()

    # Run map
    p = mapper_pipeline.MapperPipeline(
        "test",
        handler_spec=__name__ + ".test_map",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=
            output_writers.__name__ + ".KeyValueBlobstoreOutputWriter",
        params={
            "input_reader": {
                "entity_kind": __name__ + ".TestEntity",
                },
            },
        )
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    finished_map = mapper_pipeline.MapperPipeline.from_id(p.pipeline_id)

    # Can open files
    file_list = finished_map.outputs.default.value
    self.assertTrue(len(file_list) > 0)
    for name in file_list:
      files.open(name, "r").read(0)

    # Cleanup
    cleanup = mapper_pipeline._CleanupPipeline(file_list)
    cleanup.start()
    test_support.execute_until_empty(self.taskqueue)

    # Cannot open files
    for name in file_list:
      self.assertRaises(files.Error, files.open, name, "r")

  def testCleanup_ListOfLists(self):
    """Tests cleaning up a list of file lists."""
    # Prepare test data
    entity_count = 200

    for i in range(entity_count):
      TestEntity(data=str(i)).put()
      TestEntity(data=str(i)).put()

    # Run map
    p = mapper_pipeline.MapperPipeline(
        "test",
        handler_spec=__name__ + ".test_map",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=
            output_writers.__name__ + ".KeyValueBlobstoreOutputWriter",
        params={
            "input_reader": {
                "entity_kind": __name__ + ".TestEntity",
                },
            },
        )
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    finished_map = mapper_pipeline.MapperPipeline.from_id(p.pipeline_id)

    # Can open files
    file_list = finished_map.outputs.default.value
    self.assertTrue(len(file_list) > 0)
    for name in file_list:
      files.open(name, "r").read(0)

    grouped_list = [file_list]

    # Cleanup
    cleanup = mapper_pipeline._CleanupPipeline(grouped_list)
    cleanup.start()
    test_support.execute_until_empty(self.taskqueue)

    # Cannot open files
    for name in file_list:
      self.assertRaises(files.Error, files.open, name, "r")


class MapperPipelineTest(testutil.HandlerTestBase):
  """Tests for MapperPipeline."""


  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testEmptyMapper(self):
    """Test empty mapper over empty dataset."""
    p = mapper_pipeline.MapperPipeline(
        "empty_map",
        handler_spec=__name__ + ".test_empty_handler",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        params={
            "input_reader": {
                "entity_kind": __name__ + ".TestEntity",
                # Test datetime can be json serialized.
                "filters": [("dt", "=", datetime.datetime(2000, 1, 1))],
                },
            },
        )
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertEquals(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith(
        "Pipeline successful:"))

    p = mapper_pipeline.MapperPipeline.from_id(p.pipeline_id)
    self.assertTrue(p.outputs.job_id.value)

    counters = p.outputs.counters.value
    self.assertTrue(counters)
    self.assertTrue(context.COUNTER_MAPPER_WALLTIME_MS in counters)

  def testProcessEntites(self):
    """Test empty mapper over non-empty dataset."""
    for _ in range(100):
      TestEntity().put()

    p = mapper_pipeline.MapperPipeline(
        "empty_map",
        handler_spec=__name__ + ".test_empty_handler",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        params={
            "input_reader": {
                "entity_kind": __name__ + ".TestEntity",
                },
            },
        )
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertEquals(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith(
        "Pipeline successful:"))

    p = mapper_pipeline.MapperPipeline.from_id(p.pipeline_id)
    self.assertTrue(p.outputs.job_id.value)

    counters = p.outputs.counters.value
    self.assertTrue(counters)
    self.assertTrue(context.COUNTER_MAPPER_WALLTIME_MS in counters)
    self.assertEquals(100, counters[context.COUNTER_MAPPER_CALLS])

  def testSliceRetry(self):
    entity_count = 200
    db.delete(TestOutputEntity.all())
    db.delete(RetryCount.all())

    for i in range(entity_count):
      TestEntity(data=str(i)).put()

    p = mapper_pipeline.MapperPipeline(
        "test",
        handler_spec=__name__ + ".test_slice_retry_map",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        params={
            "input_reader": {
                "entity_kind": __name__ + "." + TestEntity.__name__,
            },
        },
        shards=5)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertEquals(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith(
        "Pipeline successful:"))

    p = mapper_pipeline.MapperPipeline.from_id(p.pipeline_id)
    outputs = []
    for output in TestOutputEntity.all():
      outputs.append(int(output.data))
    outputs.sort()

    expected_outputs = [i for i in range(entity_count)]
    expected_outputs.sort()
    self.assertEquals(expected_outputs, outputs)

  def testShardRetry(self):
    entity_count = 200
    db.delete(TestOutputEntity.all())
    db.delete(RetryCount.all())

    for i in range(entity_count):
      TestEntity(data=str(i)).put()

    p = mapper_pipeline.MapperPipeline(
        "test",
        handler_spec=__name__ + ".test_shard_retry_map",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        params={
            "input_reader": {
                "entity_kind": __name__ + "." + TestEntity.__name__,
            },
        },
        shards=5)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertEquals(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith(
        "Pipeline successful:"))

    p = mapper_pipeline.MapperPipeline.from_id(p.pipeline_id)
    outputs = []
    for output in TestOutputEntity.all():
      outputs.append(int(output.data))
    outputs.sort()

    expected_outputs = [i for i in range(entity_count)]
    expected_outputs.sort()
    self.assertEquals(expected_outputs, outputs)

  def testShardRetryTooMany(self):
    entity_count = 200
    db.delete(TestOutputEntity.all())
    db.delete(RetryCount.all())

    for i in range(entity_count):
      TestEntity(data=str(i)).put()

    p = mapper_pipeline.MapperPipeline(
        "test",
        handler_spec=__name__ + ".test_shard_retry_too_many_map",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        params={
            "input_reader": {
                "entity_kind": __name__ + "." + TestEntity.__name__,
            },
        },
        shards=5)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertEquals(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith(
        "Pipeline successful:"))
    state = model.MapreduceState.all().get()
    self.assertEqual(model.MapreduceState.RESULT_FAILED, state.result_status)


if __name__ == "__main__":
  unittest.main()
