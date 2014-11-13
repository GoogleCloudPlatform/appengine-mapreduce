#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.




# pylint: disable=g-bad-name

import unittest


import pipeline
import cloudstorage
from google.appengine.ext import db
from mapreduce import errors
from mapreduce import input_readers
from mapreduce import mapreduce_pipeline
from mapreduce import model
from mapreduce import output_writers
from mapreduce import records
from mapreduce import test_support
from testlib import testutil


class TestEntity(db.Model):
  """Test entity class."""
  data = db.StringProperty()


class TestOutputEntity(db.Model):
  """TestOutput entity class."""
  data = db.StringProperty()


class RetryCount(db.Model):
  """Use to keep track of slice/shard retries."""
  retries = db.IntegerProperty()


# Map or reduce functions.
def test_mapreduce_map(entity):
  """Test map handler."""
  yield (entity.data, "")


def test_mapreduce_reduce(key, values):
  """Test reduce handler."""
  yield str((key, values))


def test_failed_map(_):
  """Always fail the map immediately."""
  raise errors.FailJobError()


class TestFileRecordsOutputWriter(
    output_writers._GoogleCloudStorageRecordOutputWriter):

  RETRIES = 11

  def finalize(self, ctx, shard_state):
    """Simulate output writer finalization Error."""
    retry_count = RetryCount.get_by_key_name(__name__)
    if not retry_count:
      retry_count = RetryCount(key_name=__name__, retries=0)
    if retry_count.retries < self.RETRIES:
      retry_count.retries += 1
      retry_count.put()
      raise cloudstorage.TransientError("output writer finalize failed.")
    super(TestFileRecordsOutputWriter, self).finalize(ctx, shard_state)


class MapreducePipelineTest(testutil.HandlerTestBase):
  """Tests for MapreducePipeline."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testFailedMapReduce(self):
    bucket_name = "testbucket"
    max_attempts_before = pipeline.pipeline._DEFAULT_MAX_ATTEMPTS
    try:
      pipeline.pipeline._DEFAULT_MAX_ATTEMPTS = 1

      # Add some random data.
      entity_count = 200

      print dir(pipeline.pipeline)

      for i in range(entity_count):
        TestEntity(data=str(i)).put()
        TestEntity(data=str(i)).put()

      p = mapreduce_pipeline.MapreducePipeline(
          "test",
          __name__ + ".test_failed_map",
          __name__ + ".test_mapreduce_reduce",
          input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
          output_writer_spec=(output_writers.__name__ +
                              "._GoogleCloudStorageRecordOutputWriter"),
          mapper_params={
              "entity_kind": __name__ + "." + TestEntity.__name__,
          },
          reducer_params={
              "output_writer": {
                  "bucket_name": bucket_name
              },
          },
          shards=3)
      p.max_attempts = 1
      p.start()
      test_support.execute_until_empty(self.taskqueue)

      p = mapreduce_pipeline.MapreducePipeline.from_id(p.pipeline_id)
      self.assertTrue(p.was_aborted)
    finally:
      pipeline.pipeline._DEFAULT_MAX_ATTEMPTS = max_attempts_before

  def testMapReduce(self):
    # Prepare test data
    bucket_name = "testbucket"
    job_name = "test_job"
    entity_count = 200

    for i in range(entity_count):
      TestEntity(data=str(i)).put()
      TestEntity(data=str(i)).put()

    # Run Mapreduce
    p = mapreduce_pipeline.MapreducePipeline(
        job_name,
        __name__ + ".test_mapreduce_map",
        __name__ + ".test_mapreduce_reduce",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=(
            output_writers.__name__ + "._GoogleCloudStorageRecordOutputWriter"),
        mapper_params={
            "entity_kind": __name__ + "." + TestEntity.__name__,
            "bucket_name": bucket_name
        },
        reducer_params={
            "output_writer": {
                "bucket_name": bucket_name
            },
        },
        shards=16)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertEquals(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith(
        "Pipeline successful:"))

    # Verify reduce output.
    p = mapreduce_pipeline.MapreducePipeline.from_id(p.pipeline_id)
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS,
                     p.outputs.result_status.value)
    output_data = []
    for output_file in p.outputs.default.value:
      with cloudstorage.open(output_file) as f:
        for record in records.RecordsReader(f):
          output_data.append(record)

    expected_data = [
        str((str(d), ["", ""])) for d in range(entity_count)]
    expected_data.sort()
    output_data.sort()
    self.assertEquals(expected_data, output_data)

    # Verify that mapreduce doesn't leave intermediate files behind.
    temp_file_stats = cloudstorage.listbucket("/" + bucket_name)
    for stat in temp_file_stats:
      if stat.filename:
        self.assertFalse(
            stat.filename.startswith("/%s/%s-shuffle-" %
                                     (bucket_name, job_name)))

  def testMapReduceWithShardRetry(self):
    # Prepare test data
    bucket_name = "testbucket"
    entity_count = 200
    db.delete(RetryCount.all())

    for i in range(entity_count):
      TestEntity(data=str(i)).put()
      TestEntity(data=str(i)).put()

    # Run Mapreduce
    p = mapreduce_pipeline.MapreducePipeline(
        "test",
        __name__ + ".test_mapreduce_map",
        __name__ + ".test_mapreduce_reduce",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=(
            __name__ + ".TestFileRecordsOutputWriter"),
        mapper_params={
            "input_reader": {
                "entity_kind": __name__ + "." + TestEntity.__name__,
            },
        },
        reducer_params={
            "output_writer": {
                "bucket_name": bucket_name
            },
        },
        shards=16)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertEquals(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith(
        "Pipeline successful:"))

    # Verify reduce output.
    p = mapreduce_pipeline.MapreducePipeline.from_id(p.pipeline_id)
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS,
                     p.outputs.result_status.value)
    output_data = []
    retries = 0
    for output_file in p.outputs.default.value:
      # Get the number of shard retries by parsing filename.
      retries += (int(output_file[-1]) - 1)
      with cloudstorage.open(output_file) as f:
        for record in records.RecordsReader(f):
          output_data.append(record)

    # Assert file names also suggest the right number of retries.
    self.assertEquals(44, retries)
    expected_data = [
        str((str(d), ["", ""])) for d in range(entity_count)]
    expected_data.sort()
    output_data.sort()
    self.assertEquals(expected_data, output_data)


if __name__ == "__main__":
  unittest.main()
