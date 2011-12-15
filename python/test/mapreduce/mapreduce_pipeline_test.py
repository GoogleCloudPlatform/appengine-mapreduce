#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.




import re
import unittest


from mapreduce.lib import pipeline
from mapreduce.lib import files
from mapreduce.lib.files import file_service_pb
from mapreduce.lib.files import records
from google.appengine.ext import blobstore
from google.appengine.ext import db
from mapreduce import input_readers
from mapreduce import mapreduce_pipeline
from mapreduce import output_writers
from mapreduce import test_support
from testlib import testutil


class TestEntity(db.Model):
  """Test entity class."""
  data = db.TextProperty()


def test_handler_yield_key(entity):
  """Test handler that yields entity key."""
  yield entity.key()


def test_handler_yield_str(o):
  """Test handler that yields parameter converted to string."""
  yield str(o)


def test_map(entity):
  """Test map handler."""
  yield (entity.data, "")


def test_combiner_map(entity):
  """Tests map handler for use with the Combiner test."""
  yield str(int(entity.data) % 4), entity.data


def test_combine(key, values, combiner_values):
  """Test combine handler."""
  assert not combiner_values
  value_ints = [int(x) for x in values]
  value_ints.sort()
  yield str(value_ints)


def test_reduce(key, values):
  """Test reduce handler."""
  yield str((key, values))


class MapreducePipelineTest(testutil.HandlerTestBase):
  """Tests for MapreducePipeline."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testMapReduce(self):
    # Prepare test data
    entity_count = 200

    for i in range(entity_count):
      TestEntity(data=str(i)).put()
      TestEntity(data=str(i)).put()

    # Run Mapreduce
    p = mapreduce_pipeline.MapreducePipeline(
        "test",
        __name__ + ".test_map",
        __name__ + ".test_reduce",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=
            output_writers.__name__ + ".BlobstoreRecordsOutputWriter",
        mapper_params={
            "entity_kind": __name__ + "." + TestEntity.__name__,
            },
        shards=16)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertEquals(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith(
        "Pipeline successful:"))

    # Verify reduce output.
    p = mapreduce_pipeline.MapreducePipeline.from_id(p.pipeline_id)
    output_data = []
    for output_file in p.outputs.default.value:
      with files.open(output_file, "r") as f:
        for record in records.RecordsReader(f):
          output_data.append(record)

    expected_data = [
        str((str(d), ["", ""])) for d in range(entity_count)]
    expected_data.sort()
    output_data.sort()
    self.assertEquals(expected_data, output_data)

    # Verify that mapreduce doesn't leave intermediate files behind.
    blobInfos = blobstore.BlobInfo.all().fetch(limit=1000)
    for blobinfo in blobInfos:
      self.assertTrue(
          "Bad filename: %s" % blobinfo.filename,
          re.match("test-reduce-.*-output-\d+", blobinfo.filename))


class ShufflerCombinePipelineTest(testutil.HandlerTestBase):
  """Tests for combiners in the shuffle pipeline."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testCombine(self):
    # Prepare test data
    entity_count = 200

    for i in range(entity_count):
      TestEntity(data=str(i)).put()
      TestEntity(data=str(i)).put()

    # Run map
    p = mapreduce_pipeline.MapperPipeline(
        "test",
        handler_spec=__name__ + ".test_combiner_map",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=
            output_writers.__name__ + ".KeyValueBlobstoreOutputWriter",
        params={
            "entity_kind": __name__ + ".TestEntity",
            },
        shards=4)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    finished_map = mapreduce_pipeline.MapperPipeline.from_id(p.pipeline_id)

    # Run Combine
    p = mapreduce_pipeline.ShufflePipeline(
        "test",
        finished_map.outputs.default.value,
        combine_spec=__name__ + ".test_combine")
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertEquals(2, len(self.emails))
    for i in xrange(2):
      self.assertTrue(self.emails[i][1].startswith(
          "Pipeline successful:"))

    # Verify combine output. A filename for each processing shard.
    finished_combine = mapreduce_pipeline.ShufflePipeline.from_id(p.pipeline_id)
    self.assertEquals(4, len(finished_combine.outputs.default.value))

    seen_keys = set()
    repeated_keys = []
    output_data = []
    for output_file in finished_combine.outputs.default.value:
      with files.open(output_file, "r") as f:
        for record in records.RecordsReader(f):
          proto = file_service_pb.KeyValue()
          proto.ParseFromString(record)
          key = proto.key()
          output_data.append(eval(proto.value()))

    # Validate the combined data.
    seen_numbers = set()
    for shard in output_data:
      # Each sub-shard should be in order.
      self.assertEquals(shard, sorted(shard))

      output_set = set(shard)
      for number in output_set:
        # Within each shard, each number should appear twice.
        self.assertEquals(2, shard.count(number))
        # And should not appear in other shards.
        self.assertFalse(number in seen_numbers)

      seen_numbers.update(output_set)

    self.assertEquals(set(xrange(200)), seen_numbers)


if __name__ == "__main__":
  unittest.main()
