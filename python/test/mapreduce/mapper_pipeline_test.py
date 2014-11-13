#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.




# pylint: disable=g-bad-name

import datetime
import unittest


import pipeline
from google.appengine.ext import db
from mapreduce import context
from mapreduce import errors
from mapreduce import input_readers
from mapreduce import mapper_pipeline
from mapreduce import model
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


def test_fail_map(_):
  """Always fail job immediately."""
  raise errors.FailJobError()


def test_slice_retry_map(entity):
  """Raise exception for 11 times when data is 100."""
  if entity.data == "100":
    retry_count = RetryCount.get_by_key_name(entity.data)
    if not retry_count:
      retry_count = RetryCount(key_name=entity.data, retries=0)
    if retry_count.retries < 11:
      retry_count.retries += 1
      retry_count.put()
      raise Exception()
  TestOutputEntity(key_name=entity.data, data=entity.data).put()


def test_shard_retry_map(entity):
  """Raise exception 12 times when data is 100."""
  if entity.data == "100":
    retry_count = RetryCount.get_by_key_name(entity.data)
    if not retry_count:
      retry_count = RetryCount(key_name=entity.data, retries=0)
    if retry_count.retries < 12:
      retry_count.retries += 1
      retry_count.put()
      raise Exception()
  TestOutputEntity(key_name=entity.data, data=entity.data).put()


def test_shard_retry_too_many_map(entity):
  """Raise shard retry exception 45 times when data is 100."""
  if entity.data == "100":
    retry_count = RetryCount.get_by_key_name(entity.data)
    if not retry_count:
      retry_count = RetryCount(key_name=entity.data, retries=0)
    if retry_count.retries < 45:
      retry_count.retries += 1
      retry_count.put()
      raise Exception()
  TestOutputEntity(key_name=entity.data, data=entity.data).put()


def test_map(entity):
  """Test map handler."""
  yield (entity.data, "")


# pylint: disable=unused-argument
def test_empty_handler(entity):
  """Test handler that does nothing."""
  pass


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
    # Verify outputs.
    # Counter output
    counters = p.outputs.counters.value
    self.assertTrue(counters)
    self.assertTrue(context.COUNTER_MAPPER_WALLTIME_MS in counters)
    # Default output.
    self.assertEqual([], p.outputs.default.value)
    # Job id output.
    self.assertTrue(p.outputs.job_id.filled)
    state = model.MapreduceState.get_by_job_id(p.outputs.job_id.value)
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS, state.result_status)
    # Result status output.
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS,
                     p.outputs.result_status.value)

  def testFailedMap(self):
    for i in range(1):
      TestEntity(data=str(i)).put()

    pipeline.pipeline._DEFAULT_MAX_ATTEMPTS = 1

    p = mapper_pipeline.MapperPipeline(
        "test",
        handler_spec=__name__ + ".test_fail_map",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        params={
            "input_reader": {
                "entity_kind": __name__ + "." + TestEntity.__name__,
            },
        },
        shards=5)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    p = mapper_pipeline.MapperPipeline.from_id(p.pipeline_id)
    self.assertTrue(p.was_aborted)

    self.assertTrue(p.outputs.job_id.filled)
    state = model.MapreduceState.get_by_job_id(p.outputs.job_id.value)
    self.assertEqual(model.MapreduceState.RESULT_FAILED, state.result_status)
    self.assertFalse(p.outputs.result_status.filled)
    self.assertFalse(p.outputs.default.filled)

    self.assertEquals(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith(
        "Pipeline aborted:"))

  def testProcessEntities(self):
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

    self.assertTrue(p.outputs.job_id.filled)
    counters = p.outputs.counters.value
    self.assertTrue(counters)
    self.assertTrue(context.COUNTER_MAPPER_WALLTIME_MS in counters)
    self.assertEquals(100, counters[context.COUNTER_MAPPER_CALLS])
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS,
                     p.outputs.result_status.value)
    self.assertEqual([], p.outputs.default.value)

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
    p.max_attempts = 1
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    state = model.MapreduceState.all().get()
    self.assertEqual(model.MapreduceState.RESULT_FAILED, state.result_status)

    self.assertEquals(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith(
        "Pipeline aborted:"))


if __name__ == "__main__":
  unittest.main()
