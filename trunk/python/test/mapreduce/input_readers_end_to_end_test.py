#!/usr/bin/env python




# Using opensource naming conventions, pylint: disable=g-bad-name

import unittest


from mapreduce import control
from mapreduce import input_readers
from mapreduce import test_support
from testlib import testutil

# pylint: disable=g-import-not-at-top
# TODO(user): Cleanup imports if/when cloudstorage becomes part of runtime.
try:
  import cloudstorage
  enable_cloudstorage_tests = True
except ImportError:
  enable_cloudstorage_tests = False

# Global for collecting data across all map shards
_memory_mapper_data = []


def _input_reader_memory_mapper(data):
  _memory_mapper_data.append(data.read())


class GoogleCloudStorageInputReaderEndToEndTest(testutil.CloudStorageTestBase):
  """End-to-end tests for GoogleCloudStorageInputReader."""

  def setUp(self):
    super(GoogleCloudStorageInputReaderEndToEndTest, self).setUp()
    # clear global list of mapped data
    global _memory_mapper_data
    _memory_mapper_data = []

  def create_test_content(self, bucket_name, object_prefix, num_files):
    """Create a file in Google Cloud Storage with a small amount of content.

    Args:
      bucket_name: the name of the bucket, with no delimiters.
      object_prefix: a string prefix for each object/file that will be created.
        A suffix with a file number will automatically be appended.
      num_files: the number of files to create.

    Returns:
      A list with each element containing the data in one of the created files.
    """
    created_content = []
    for file_num in range(num_files):
      content = "Dummy Content %d" % file_num
      created_content.append(content)
      test_file = cloudstorage.open(
          "/%s/%s%03d" % (bucket_name, object_prefix, file_num),
          mode="w")
      test_file.write(content)
      test_file.close()
    return created_content

  def _run_test(self, num_shards, num_files):
    bucket_name = "testing"
    object_prefix = "file-"
    job_name = "test_map"
    input_class = (input_readers.__name__ + "." +
                   input_readers._GoogleCloudStorageInputReader.__name__)

    expected_content = self.create_test_content(bucket_name,
                                                object_prefix,
                                                num_files)

    control.start_map(
        job_name,
        __name__ + "." + "_input_reader_memory_mapper",
        input_class,
        {
            "input_reader": {
                "bucket_name": bucket_name,
                "objects": [object_prefix + "*"]
            },
        },
        shard_count=num_shards)

    test_support.execute_until_empty(self.taskqueue)
    self.assertEqual(expected_content.sort(), _memory_mapper_data.sort())

  def testSingleShard(self):
    self._run_test(num_shards=1, num_files=10)

  def testMultipleShards(self):
    self._run_test(num_shards=4, num_files=10)

if __name__ == "__main__":
  unittest.main()
