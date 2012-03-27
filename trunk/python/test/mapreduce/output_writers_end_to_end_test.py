#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.





import unittest


from mapreduce.lib import files
from google.appengine.ext import db
from mapreduce import control
from mapreduce import model
from mapreduce import output_writers
from mapreduce import test_support
from testlib import testutil


BLOBSTORE_WRITER_NAME = (output_writers.__name__ + "." +
                         output_writers.BlobstoreOutputWriter.__name__)
FILE_WRITER_NAME = (output_writers.__name__ + "." +
                    output_writers.FileOutputWriter.__name__)


class TestEntity(db.Model):
  """Test entity class."""


def test_handler_yield_key_str(entity):
  """Test handler which yields entity key."""
  yield str(entity.key()) + "\n"


class FileOutputWriterEndToEndTest(testutil.HandlerTestBase):
  """End-to-end tests for FileOutputWriter using googlestore."""

  def testSingleShard(self):
    entity_count = 1000

    for _ in range(entity_count):
      TestEntity().put()

    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".test_handler_yield_key_str",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
            "filesystem": "gs",
            "gs_bucket_name": "bucket"
        },
        shard_count=4,
        base_path="/mapreduce_base_path",
        output_writer_spec=FILE_WRITER_NAME)

    test_support.execute_until_empty(self.taskqueue)

    mapreduce_state = model.MapreduceState.get_by_job_id(mapreduce_id)
    filenames = output_writers.FileOutputWriter.get_filenames(mapreduce_state)
    self.assertEqual(1, len(filenames))
    self.assertTrue(filenames[0].startswith("/gs/bucket/"))

    with files.open(filenames[0], "r") as f:
      data = f.read(10000000)
      self.assertEquals(1000, len(data.strip().split("\n")))

  def testDedicatedParams(self):
    entity_count = 1000

    for _ in range(entity_count):
      TestEntity().put()

    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".test_handler_yield_key_str",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "input_reader": {
                "entity_kind": __name__ + "." + TestEntity.__name__,
            },
            "output_writer": {
                "filesystem": "gs",
                "gs_bucket_name": "bucket",
            },
        },
        shard_count=4,
        base_path="/mapreduce_base_path",
        output_writer_spec=FILE_WRITER_NAME)

    test_support.execute_until_empty(self.taskqueue)

    mapreduce_state = model.MapreduceState.get_by_job_id(mapreduce_id)
    filenames = output_writers.FileOutputWriter.get_filenames(mapreduce_state)
    self.assertEqual(1, len(filenames))
    self.assertTrue(filenames[0].startswith("/gs/bucket/"))

    with files.open(filenames[0], "r") as f:
      data = f.read(10000000)
      self.assertEquals(1000, len(data.strip().split("\n")))

  def testMultipleShards(self):
    entity_count = 1000

    for _ in range(entity_count):
      TestEntity().put()

    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".test_handler_yield_key_str",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
            "output_sharding": "input",
            "filesystem": "gs",
        },
        shard_count=4,
        base_path="/mapreduce_base_path",
        output_writer_spec=BLOBSTORE_WRITER_NAME)

    test_support.execute_until_empty(self.taskqueue)

    mapreduce_state = model.MapreduceState.get_by_job_id(mapreduce_id)
    filenames = output_writers.BlobstoreOutputWriter.get_filenames(
        mapreduce_state)
    self.assertEqual(4, len(filenames))

    file_lengths = []
    for filename in filenames:
      self.assertTrue(filename.startswith("/blobstore/"))
      self.assertFalse(filename.startswith("/blobstore/writable:"))

      with files.open(filename, "r") as f:
        data = f.read(10000000)
        file_lengths.append(len(data.strip().split("\n")))

    # these numbers are totally random and depend on our sharding,
    # which is quite deterministic.
    expected_lengths = [199, 210, 275, 316]
    self.assertEqual(1000, sum(expected_lengths))
    self.assertEquals(expected_lengths, file_lengths)


class BlobstoreOutputWriterEndToEndTest(testutil.HandlerTestBase):
  """End-to-end tests for BlobstoreOutputWriter.

  BlobstoreOutputWriter isn't complex enough yet to do extensive
  unit tests. Do end-to-end tests just to check that it works.
  """

  def testSingleShard(self):
    entity_count = 1000

    for _ in range(entity_count):
      TestEntity().put()

    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".test_handler_yield_key_str",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
        },
        shard_count=4,
        base_path="/mapreduce_base_path",
        output_writer_spec=BLOBSTORE_WRITER_NAME)

    test_support.execute_until_empty(self.taskqueue)

    mapreduce_state = model.MapreduceState.get_by_job_id(mapreduce_id)
    filenames = output_writers.BlobstoreOutputWriter.get_filenames(
        mapreduce_state)
    self.assertEqual(1, len(filenames))
    blob_name = filenames[0]
    self.assertTrue(blob_name.startswith("/blobstore/"))
    self.assertFalse(blob_name.startswith("/blobstore/writable:"))

    with files.open(blob_name, "r") as f:
      data = f.read(10000000)
      self.assertEquals(1000, len(data.strip().split("\n")))

  def testMultipleShards(self):
    entity_count = 1000

    for _ in range(entity_count):
      TestEntity().put()

    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".test_handler_yield_key_str",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
            "output_sharding": "input",
        },
        shard_count=4,
        base_path="/mapreduce_base_path",
        output_writer_spec=BLOBSTORE_WRITER_NAME)

    test_support.execute_until_empty(self.taskqueue)

    mapreduce_state = model.MapreduceState.get_by_job_id(mapreduce_id)
    filenames = output_writers.BlobstoreOutputWriter.get_filenames(
        mapreduce_state)
    self.assertEqual(4, len(filenames))

    file_lengths = []
    for filename in filenames:
      self.assertTrue(filename.startswith("/blobstore/"))
      self.assertFalse(filename.startswith("/blobstore/writable:"))

      with files.open(filename, "r") as f:
        data = f.read(10000000)
        file_lengths.append(len(data.strip().split("\n")))

    # these numbers are totally random and depend on our sharding,
    # which is quite deterministic.
    expected_lengths = [199, 210, 275, 316]
    self.assertEqual(1000, sum(expected_lengths))
    self.assertEquals(expected_lengths, file_lengths)


if __name__ == "__main__":
  unittest.main()
