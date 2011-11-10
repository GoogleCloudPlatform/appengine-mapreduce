#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.




import unittest


from mapreduce.lib import pipeline
from mapreduce.lib import files
from google.appengine.ext import db
from mapreduce import input_readers
from mapreduce import mapper_pipeline
from mapreduce import mapreduce_pipeline
from mapreduce import output_writers
from mapreduce import test_support
from testlib import testutil


class TestEntity(db.Model):
  """Test entity class."""
  data = db.TextProperty()


def test_map(entity):
  """Test map handler."""
  yield (entity.data, "")


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
    p = mapreduce_pipeline.MapperPipeline(
        "test",
        handler_spec=__name__ + ".test_map",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=
            output_writers.__name__ + ".KeyValueBlobstoreOutputWriter",
        params={
            "entity_kind": __name__ + ".TestEntity",
            },
        )
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    finished_map = mapreduce_pipeline.MapperPipeline.from_id(p.pipeline_id)

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
    p = mapreduce_pipeline.MapperPipeline(
        "test",
        handler_spec=__name__ + ".test_map",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=
            output_writers.__name__ + ".KeyValueBlobstoreOutputWriter",
        params={
            "entity_kind": __name__ + ".TestEntity",
            },
        )
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    finished_map = mapreduce_pipeline.MapperPipeline.from_id(p.pipeline_id)

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


if __name__ == "__main__":
  unittest.main()
