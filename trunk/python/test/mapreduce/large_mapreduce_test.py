#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""Testing large mapreduce jobs."""



import unittest


from mapreduce.lib import pipeline
from google.appengine.api import files
from google.appengine.api.files import records
from google.appengine.ext import db
from mapreduce import input_readers
from mapreduce import mapreduce_pipeline
from mapreduce import output_writers
from mapreduce import test_support
from testlib import testutil


class TestEntity(db.Model):
  """Test entity class."""
  data = db.TextProperty()


def map_yield_lots_of_values(entity):
  """Test map handler that yields lots of pairs."""
  for i in range(50000):
    yield (1, " " * 100)


def reduce_length(key, values):
  """Reduce function yielding a string with key and values length."""
  yield str((key, len(values)))



class LargeMapreduceTest(testutil.HandlerTestBase):
  """Large tests for MapperPipeline."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testLotsOfValuesForSingleKey(self):
    TestEntity(data=str(1)).put()
    # Run Mapreduce
    p = mapreduce_pipeline.MapreducePipeline(
        "test",
        __name__ + ".map_yield_lots_of_values",
        __name__ + ".reduce_length",
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

    expected_data = ["('1', 50000)"]
    expected_data.sort()
    output_data.sort()
    self.assertEquals(expected_data, output_data)


if __name__ == "__main__":
  unittest.main()

