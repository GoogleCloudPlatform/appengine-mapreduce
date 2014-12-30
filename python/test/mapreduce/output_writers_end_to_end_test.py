#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.




# Using opensource naming conventions, pylint: disable=g-bad-name

import unittest


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


DATASTORE_READER_NAME = (input_readers.__name__ + "." +
                         input_readers.DatastoreInputReader.__name__)


class TestEntity(db.Model):
  """Test entity class."""


def test_handler_yield_key_str(entity):
  """Test handler which yields entity key."""
  yield str(entity.key()) + "\n"


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


class GCSRecordOutputWriterEndToEndTestBase(object):

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


class GoogleCloudStorageRecordOutputWriterEndToEndTest(
    GCSRecordOutputWriterEndToEndTestBase,
    testutil.CloudStorageTestBase):
  """End-to-end tests for CloudStorageRecordOutputWriter."""

  WRITER_CLS = output_writers._GoogleCloudStorageRecordOutputWriter
  WRITER_NAME = output_writers.__name__ + "." + WRITER_CLS.__name__


class GoogleCloudStorageConsistentRecordOutputWriterEndToEndTest(
    GCSRecordOutputWriterEndToEndTestBase,
    testutil.CloudStorageTestBase):
  """End-to-end tests for CloudStorageConsistentRecordOutputWriter."""

  WRITER_CLS = output_writers.GoogleCloudStorageConsistentRecordOutputWriter
  WRITER_NAME = output_writers.__name__ + "." + WRITER_CLS.__name__


class GoogleCloudStorageConsistentOutputWriterEndToEndTest(
    testutil.CloudStorageTestBase):
  """End-to-end tests for CloudStorageOutputWriter."""

  WRITER_CLS = output_writers.GoogleCloudStorageConsistentOutputWriter
  WRITER_NAME = output_writers.__name__ + "." + WRITER_CLS.__name__

  def _runTest(self, num_shards):
    entity_count = 1000
    bucket_name = "bucket"
    tmp_bucket_name = "tmp_bucket"
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
                "tmp_bucket_name": tmp_bucket_name,
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

    # no files left in tmpbucket
    self.assertFalse(list(cloudstorage.listbucket("/%s" % tmp_bucket_name)))
    # and only expected files in regular bucket
    files_in_bucket = [
        f.filename for f in cloudstorage.listbucket("/%s" % bucket_name)]
    self.assertEquals(filenames, files_in_bucket)

  def testSingleShard(self):
    self._runTest(num_shards=1)

  def testMultipleShards(self):
    self._runTest(num_shards=4)


if __name__ == "__main__":
  unittest.main()
