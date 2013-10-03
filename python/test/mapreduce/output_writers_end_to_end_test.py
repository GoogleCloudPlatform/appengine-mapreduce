#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.




# Using opensource naming conventions, pylint: disable=g-bad-name

import unittest


from google.appengine.api import files
from google.appengine.ext import db
from mapreduce import control
from mapreduce import input_readers
from mapreduce import model
from mapreduce import output_writers
from mapreduce import records
from mapreduce import test_support
from testlib import testutil

# pylint: disable=g-import-not-at-top
# TODO(user): Cleanup imports if/when cloudstorage becomes part of runtime.
try:
  import cloudstorage
  enable_cloudstorage_tests = True
except ImportError:
  enable_cloudstorage_tests = False


BLOBSTORE_WRITER_NAME = (output_writers.__name__ + "." +
                         output_writers.BlobstoreOutputWriter.__name__)
FILE_WRITER_NAME = (output_writers.__name__ + "." +
                    output_writers.FileOutputWriter.__name__)
DATASTORE_READER_NAME = (input_readers.__name__ + "." +
                         input_readers.DatastoreInputReader.__name__)


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
        DATASTORE_READER_NAME,
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
        DATASTORE_READER_NAME,
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
        DATASTORE_READER_NAME,
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
            "output_sharding": "input",
            "filesystem": "gs",
            "gs_bucket_name": "bucket"
        },
        shard_count=4,
        base_path="/mapreduce_base_path",
        output_writer_spec=FILE_WRITER_NAME)

    test_support.execute_until_empty(self.taskqueue)

    mapreduce_state = model.MapreduceState.get_by_job_id(mapreduce_id)
    filenames = output_writers.BlobstoreOutputWriter.get_filenames(
        mapreduce_state)
    self.assertEqual(4, len(set(filenames)))

    file_lengths = []
    for filename in filenames:
      self.assertTrue(filenames[0].startswith("/gs/bucket/"))

      with files.open(filename, "r") as f:
        data = f.read(10000000)
        file_lengths.append(len(data.strip().split("\n")))

    self.assertEquals(1000, sum(file_lengths))

  # For 183 compat.
  # TODO(user): Remove after 184 is out.
  def testMultipleShards183Compat(self):
    entity_count = 1000

    for _ in range(entity_count):
      TestEntity().put()

    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".test_handler_yield_key_str",
        DATASTORE_READER_NAME,
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
            "output_sharding": "input",
            "filesystem": "gs",
            "gs_bucket_name": "bucket"
        },
        shard_count=4,
        base_path="/mapreduce_base_path",
        output_writer_spec="__main__.TestFileOutputWriter")

    # Kickoff job task.
    test_support.execute_all_tasks(self.taskqueue)
    # Verify all writer_states are set.
    mapreduce_state = model.MapreduceState.get_by_job_id(mapreduce_id)
    shard_states = model.ShardState.find_by_mapreduce_state(mapreduce_state)
    for s in shard_states:
      writer_state = output_writers.FileOutputWriterBase._State.from_json(
          s.writer_state)
      self.assertTrue(writer_state.filenames)
      self.assertTrue(writer_state.request_filenames)
    # Worker tasks.
    test_support.execute_until_empty(self.taskqueue)

    mapreduce_state = model.MapreduceState.get_by_job_id(mapreduce_id)
    filenames = output_writers.BlobstoreOutputWriter.get_filenames(
        mapreduce_state)
    self.assertEqual(4, len(set(filenames)))

    writer_state = output_writers.FileOutputWriterBase._State.from_json(
        mapreduce_state.writer_state)
    self.assertEqual(filenames, writer_state.filenames)

    file_lengths = []
    for filename in filenames:
      self.assertTrue(filenames[0].startswith("/gs/bucket/"))

      with files.open(filename, "r") as f:
        data = f.read(10000000)
        file_lengths.append(len(data.strip().split("\n")))

    self.assertEquals(1000, sum(file_lengths))


# For 183 compat.
# TODO(user): Remove after 184 is out.
class TestFileOutputWriter(output_writers.FileOutputWriter):

  def __init__(self, filenames, request_filenames):
    super(TestFileOutputWriter, self).__init__(filenames, request_filenames)
    self._183_test = True


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
        DATASTORE_READER_NAME,
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
        DATASTORE_READER_NAME,
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
    self.assertEqual(4, len(set(filenames)))

    file_lengths = []
    for filename in filenames:
      self.assertTrue(filename.startswith("/blobstore/"))
      self.assertFalse(filename.startswith("/blobstore/writable:"))

      with files.open(filename, "r") as f:
        data = f.read(10000000)
        file_lengths.append(len(data.strip().split("\n")))

    self.assertEqual(1000, sum(file_lengths))


class GoogleCloudStorageOutputWriterEndToEndTest(testutil.CloudStorageTestBase):
  """End-to-end tests for CloudStorageOutputWriter."""

  WRITER_CLS = output_writers._GoogleCloudStorageOutputWriter
  WRITER_NAME = output_writers.__name__ + "." + WRITER_CLS.__name__

  def _runTest(self, num_shards):
    entity_count = 1000
    bucket_name = "bucket"
    job_name = "test_map"

    for _ in range(entity_count):
      TestEntity().put()

    mapreduce_id = control.start_map(
        job_name,
        __name__ + ".test_handler_yield_key_str",
        DATASTORE_READER_NAME,
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
            "output_writer": {
                "bucket_name": bucket_name,
            },
        },
        shard_count=num_shards,
        output_writer_spec=self.WRITER_NAME)

    test_support.execute_until_empty(self.taskqueue)
    mapreduce_state = model.MapreduceState.get_by_job_id(mapreduce_id)
    filenames = self.WRITER_CLS.get_filenames(mapreduce_state)

    self.assertEqual(num_shards, len(set(filenames)))
    total_entries = 0
    for shard in range(num_shards):
      self.assertTrue(filenames[shard].startswith("/%s/%s" % (bucket_name,
                                                              job_name)))
      data = cloudstorage.open(filenames[shard]).read()
      # strip() is used to remove the last newline of each file so that split()
      # does not retrun extraneous empty entries.
      total_entries += len(data.strip().split("\n"))
    self.assertEqual(entity_count, total_entries)

  def testSingleShard(self):
    self._runTest(num_shards=1)

  def testMultipleShards(self):
    self._runTest(num_shards=4)


class GoogleCloudStorageRecordOutputWriterEndToEndTest(
    testutil.CloudStorageTestBase):
  """End-to-end tests for CloudStorageRecordOutputWriter."""

  WRITER_CLS = output_writers._GoogleCloudStorageRecordOutputWriter
  WRITER_NAME = output_writers.__name__ + "." + WRITER_CLS.__name__

  def _runTest(self, num_shards):
    entity_count = 1000
    bucket_name = "bucket"
    job_name = "test_map"

    for _ in range(entity_count):
      TestEntity().put()

    mapreduce_id = control.start_map(
        job_name,
        __name__ + ".test_handler_yield_key_str",
        DATASTORE_READER_NAME,
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
            "output_writer": {
                "bucket_name": bucket_name,
            },
        },
        shard_count=num_shards,
        output_writer_spec=self.WRITER_NAME)

    test_support.execute_until_empty(self.taskqueue)
    mapreduce_state = model.MapreduceState.get_by_job_id(mapreduce_id)
    filenames = self.WRITER_CLS.get_filenames(mapreduce_state)

    self.assertEqual(num_shards, len(set(filenames)))
    total_entries = 0
    for shard in range(num_shards):
      self.assertTrue(filenames[shard].startswith("/%s/%s" % (bucket_name,
                                                              job_name)))
      data = "".join([_ for _ in records.RecordsReader(
          cloudstorage.open(filenames[shard]))])
      # strip() is used to remove the last newline of each file so that split()
      # does not return extraneous empty entries.
      total_entries += len(data.strip().split("\n"))
    self.assertEqual(entity_count, total_entries)

  def testSingleShard(self):
    self._runTest(num_shards=1)

  def testMultipleShards(self):
    self._runTest(num_shards=4)


if __name__ == "__main__":
  unittest.main()
