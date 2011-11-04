#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.




import re
import unittest


from mapreduce.lib import pipeline
from mapreduce.lib import files
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


def test_empty_handler(entity):
  """Test handler that does nothing."""
  pass


def test_handler_yield_key(entity):
  """Test handler that yields entity key."""
  yield entity.key()


def test_handler_yield_str(o):
  """Test handler that yields parameter converted to string."""
  yield str(o)


def test_map(entity):
  """Test map handler."""
  yield (entity.data, "")


def test_reduce(key, values):
  """Test reduce handler."""
  yield str((key, values))


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
    p = mapreduce_pipeline.MapperPipeline(
        "empty_map",
        handler_spec=__name__ + ".test_empty_handler",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        params={
            "entity_kind": __name__ + ".TestEntity",
            },
        )
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertEquals(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith(
        "Pipeline successful:"))

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
        mapper_params= {
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


if __name__ == "__main__":
  unittest.main()
