#!/usr/bin/env python
"""Tests for _gcs.py."""

# Disable "Invalid method name"
# pylint: disable=g-bad-name

import math
import unittest


from mapreduce import errors

from mapreduce import map_job_context
from mapreduce import records
from testlib import testutil
from mapreduce.api import map_job
from mapreduce.lib import input_reader


# pylint: disable=g-import-not-at-top
# TODO(user): Cleanup imports if/when cloudstorage becomes part of runtime.
try:
  import cloudstorage
  enable_cloudstorage_tests = True
except ImportError:
  enable_cloudstorage_tests = False


class GCSInputTestBase(testutil.CloudStorageTestBase):
  """Base class for running input tests with Google Cloud Storage.

  Subclasses must define READER_CLS.
  """

  # Defaults
  READER_CLS = input_reader.GCSInputReader
  TEST_BUCKET = "testing"

  def create_job_config(self, num_shards=None, input_params=None):
    """Create a JobConfig using GCSInputReader.

    Args:
      num_shards: optionally specify the number of shards.
      input_params: parameters for the input reader.

    Returns:
      a JobConfig with default settings and specified input_params.
    """
    job_config = map_job.JobConfig(
        job_name="TestJob",
        mapper=map_job.Mapper,
        input_reader_cls=self.READER_CLS,
        input_reader_params=input_params,
        shard_count=num_shards)
    return job_config

  def create_ctx(self, job_config, shard_state, tstate):
    """Creates contexts.

    Args:
      job_config: JobConfig instance.
      shard_state: model.ShardState instance.
      tstate: model.TransientShardState instance.

    Returns:
      A (ShardContext, SliceContext) pair.
    """
    job_ctx = map_job_context.JobContext(job_config)
    shard_ctx = map_job_context.ShardContext(job_ctx, shard_state)
    slice_ctx = map_job_context.SliceContext(shard_ctx, shard_state, tstate)
    self.slice_ctx = slice_ctx
    self.shard_ctx = shard_ctx
    return shard_ctx, slice_ctx

  def create_test_file(self, filename, content):
    """Create a test file with minimal content.

    Args:
      filename: the name of the file in the form "/bucket/object".
      content: the content to put in the file or if None a dummy string
        containing the filename will be used.
    """
    test_file = cloudstorage.open(filename, mode="w")
    test_file.write(content)
    test_file.close()

  def create_single_reader(self, filenames, contents=None, delimiter=None,
                           path_filter=None):
    """Create a test reader.

    Args:
      filenames: a list of filenames without bucket prefix.
      contents: a list of list of strings. If provided, the files will be
        created with these contents.
      delimiter: delimiter.
      path_filter: path filter.

    Returns:
      a single initialized reader object.
    """
    if contents is not None:
      for f, c in zip(filenames, contents):
        fullname = "/%s/%s" % (self.TEST_BUCKET, f)
        self.create_test_file(fullname, c)

    job_config = self.create_job_config(
        num_shards=1,
        input_params={"bucket_name": self.TEST_BUCKET,
                      "objects": filenames,
                      "delimiter": delimiter,
                      "path_filter": path_filter})
    shard_state = self.create_shard_state(0)
    _, slice_ctx = self.create_ctx(job_config, shard_state, None)

    readers = self.READER_CLS.split_input(job_config)
    self.assertEqual(1, len(readers))
    reader = readers[0]
    reader.begin_slice(slice_ctx)
    return reader


class GCSInputReaderTest(GCSInputTestBase):
  """Tests for GoogleCloudStorageInputReader."""

  def setUp(self):
    super(GCSInputReaderTest, self).setUp()

    # create test content
    self.test_bucket = self.TEST_BUCKET
    self.test_content = []
    self.test_num_files = 20
    self.test_filenames = []
    for file_num in range(self.test_num_files):
      content = "Dummy Content %03d" % file_num
      self.test_content.append(content)
      filename = "/%s/file-%03d" % (self.test_bucket, file_num)
      self.test_filenames.append(filename)
      self.create_test_file(filename, content)

  def testValidate_NoParams(self):
    self.assertRaises(
        errors.BadReaderParamsError,
        self.READER_CLS.validate,
        self.create_job_config())

  def testValidate_NoBucket(self):
    self.assertRaises(
        errors.BadReaderParamsError,
        self.READER_CLS.validate,
        self.create_job_config(input_params={"objects": ["1", "2", "3"]}))

  def testValidate_NoObjects(self):
    self.assertRaises(
        errors.BadReaderParamsError,
        self.READER_CLS.validate,
        self.create_job_config(input_params={"bucket_name": self.test_bucket}))

  def testValidate_PathFilterWrongType(self):
    self.assertRaises(
        errors.BadReaderParamsError,
        self.READER_CLS.validate,
        self.create_job_config(input_params={"bucket_name": self.test_bucket,
                                             "objects": ["1"],
                                             "path_filter": object()}))

  def testVlidate_PathFilter(self):
    # expect no errors are raised.
    self.READER_CLS.validate(
        self.create_job_config(
            input_params={"bucket_name": self.test_bucket,
                          "objects": ["1"],
                          "path_filter": input_reader.PathFilter()}))

  def testValidate_NonList(self):
    self.assertRaises(
        errors.BadReaderParamsError,
        self.READER_CLS.validate,
        self.create_job_config(input_params={"bucket_name": self.test_bucket,
                                             "objects": "1"}))

  def testValidate_SingleObject(self):
    # expect no errors are raised
    self.READER_CLS.validate(
        self.create_job_config(input_params={"bucket_name": self.test_bucket,
                                             "objects": ["1"]}))

  def testValidate_ObjectList(self):
    # expect no errors are raised
    self.READER_CLS.validate(
        self.create_job_config(input_params={"bucket_name": self.test_bucket,
                                             "objects": ["1", "2", "3"]}))

  def testValidate_ObjectListNonString(self):
    self.assertRaises(
        errors.BadReaderParamsError,
        self.READER_CLS.validate,
        self.create_job_config(input_params={"bucket_name": self.test_bucket,
                                             "objects": ["1", ["2", "3"]]}))

  def testSplit_NoObjectSingleShard(self):
    readers = self.READER_CLS.split_input(
        self.create_job_config(num_shards=1,
                               input_params={"bucket_name": self.test_bucket,
                                             "objects": []}))
    self.assertFalse(readers)

  def testSplit_SingleObjectSingleShard(self):
    readers = self.READER_CLS.split_input(
        self.create_job_config(num_shards=1,
                               input_params={"bucket_name": self.test_bucket,
                                             "objects": ["1"]}))
    self.assertEqual(1, len(readers))
    self.assertEqual(1, len(readers[0]._filenames))

  def testSplit_SingleObjectManyShards(self):
    readers = self.READER_CLS.split_input(
        self.create_job_config(num_shards=10,
                               input_params={"bucket_name": self.test_bucket,
                                             "objects": ["1"]}))
    self.assertEqual(1, len(readers))
    self.assertEqual(1, len(readers[0]._filenames))

  def testSplit_ManyObjectEvenlySplitManyShards(self):
    num_shards = 10
    files_per_shard = 3
    filenames = ["f-%d" % f for f in range(num_shards * files_per_shard)]
    readers = self.READER_CLS.split_input(
        self.create_job_config(num_shards=num_shards,
                               input_params={"bucket_name": self.test_bucket,
                                             "objects": filenames}))
    self.assertEqual(num_shards, len(readers))
    for reader in readers:
      self.assertEqual(files_per_shard, len(reader._filenames))

  def testSplit_ManyObjectUnevenlySplitManyShards(self):
    num_shards = 10
    total_files = int(10 * 2.33)
    filenames = ["f-%d" % f for f in range(total_files)]
    readers = self.READER_CLS.split_input(
        self.create_job_config(num_shards=num_shards,
                               input_params={"bucket_name": self.test_bucket,
                                             "objects": filenames}))
    self.assertEqual(num_shards, len(readers))
    found_files = 0
    for reader in readers:
      shard_num_files = len(reader._filenames)
      found_files += shard_num_files
      # ensure per-shard distribution is even
      min_files = math.floor(total_files / num_shards)
      max_files = min_files + 1
      self.assertTrue(min_files <= shard_num_files,
                      msg="Too few files (%d > %d) in reader: %s" %
                      (min_files, shard_num_files, reader))
      self.assertTrue(max_files >= shard_num_files,
                      msg="Too many files (%d < %d) in reader: %s" %
                      (max_files, shard_num_files, reader))
    self.assertEqual(total_files, found_files)

  def testSplit_Wildcard(self):
    # test prefix matching all files
    reader = self.create_single_reader(["file-*"])
    self.assertEqual(self.test_num_files, len(reader._filenames))

    # test prefix the first 10 (those with 00 prefix)
    self.assertTrue(self.test_num_files > 10,
                    msg="More than 10 files required for testing")
    reader = self.create_single_reader(["file-00*"])
    self.assertEqual(10, len(reader._filenames))

    # test prefix matching no files
    readers = self.READER_CLS.split_input(
        self.create_job_config(num_shards=1,
                               input_params={"bucket_name": self.test_bucket,
                                             "objects": ["badprefix*"]}))
    self.assertEqual(0, len(readers))

  def testNext(self):
    reader = self.create_single_reader(["file-*"])

    # Counter should be 0.
    self.assertEqual(
        0, self.slice_ctx.counter(map_job.InputReader.COUNTER_IO_READ_MSEC))

    reader_files = list(reader)
    self.assertEqual(self.test_num_files, len(reader_files))
    found_content = []
    for reader_file in reader_files:
      found_content.append(reader_file.read())
    self.assertEqual(len(self.test_content), len(found_content))

    for content in self.test_content:
      self.assertTrue(content in found_content)
    # Check that the counter was incremented.
    self.assertEqual(
        self.test_num_files,
        self.slice_ctx.counter(input_reader.GCSInputReader.COUNTER_FILE_READ))
    self.assertEqual(
        0,
        self.slice_ctx.counter(
            input_reader.GCSInputReader.COUNTER_FILE_MISSING))

  def testNextWithMissingFiles(self):
    reader = self.create_single_reader(["file-*"])

    # Remove the first and second to last files.
    cloudstorage.delete(self.test_filenames[0])
    cloudstorage.delete(self.test_filenames[-2])
    del self.test_filenames[0]
    del self.test_filenames[-2]

    reader_files = list(reader)
    self.assertEqual(len(self.test_filenames), len(reader_files))
    self.assertEqual(self.test_filenames, [f.name for f in reader_files])
    self.assertEqual(
        self.test_num_files - 2,
        self.slice_ctx.counter(input_reader.GCSInputReader.COUNTER_FILE_READ))
    self.assertEqual(
        2,
        self.slice_ctx.counter(
            input_reader.GCSInputReader.COUNTER_FILE_MISSING))

  def testPathFilter(self):
    seen_paths = []

    class MyFilter(input_reader.PathFilter):

      def accept(self, slice_ctx, path):
        seen_paths.append(path)
        if int(path.rsplit("-")[-1]) < 10:
          return True
        return False

    reader = self.create_single_reader(["file-*"], None, None, MyFilter())
    found_filenames = [f.name for f in reader]

    self.assertEqual(self.test_filenames, seen_paths)
    self.assertEqual(10, len(found_filenames))
    self.assertEqual(
        10,
        self.slice_ctx.counter(input_reader.GCSInputReader.COUNTER_FILE_READ))

  def testSerialization(self):
    reader = self.create_single_reader(["file-*"])

    # serialize/deserialize unused reader
    reader = self.READER_CLS.from_json_str(reader.to_json_str())
    reader.begin_slice(self.slice_ctx)

    found_content = []
    for _ in range(self.test_num_files):
      reader_file = reader.next()
      found_content.append(reader_file.read())
      # serialize/deserialize after each file is read
      reader = self.READER_CLS.from_json(reader.to_json())
      reader.begin_slice(self.slice_ctx)

    self.assertEqual(len(self.test_content), len(found_content))
    for content in self.test_content:
      self.assertTrue(content in found_content)

    # verify a reader at EOF still raises EOF after serialization
    self.assertRaises(StopIteration, reader.next)


class GCSInputReaderWithDelimiterTest(GCSInputTestBase):
  """Tests for GoogleCloudStorageInputReader."""

  def setUp(self):
    super(GCSInputReaderWithDelimiterTest, self).setUp()

    # create some test content
    self.test_bucket = "testing"
    self.test_num_files = 20
    self.test_filenames = []
    for file_num in range(self.test_num_files):
      filename = "/%s/file-%03d" % (self.test_bucket, file_num)
      self.test_filenames.append(filename)
      self.create_test_file(filename, "foo")

    # Set up more directories for testing.
    self.file_per_dir = 2
    self.dirs = 20
    self.filenames_in_first_10_dirs = []
    for d in range(self.dirs):
      for file_num in range(self.file_per_dir):
        filename = "/%s/dir-%02d/file-%03d" % (self.test_bucket, d, file_num)
        if d < 10:
          self.filenames_in_first_10_dirs.append(filename)
        self.create_test_file(filename, "foo")

  def testValidate_InvalidDelimiter(self):
    self.assertRaises(
        errors.BadReaderParamsError,
        self.READER_CLS.validate,
        self.create_job_config(input_params={"bucket_name": self.test_bucket,
                                             "delimiter": 1,
                                             "objects": ["file"]}))

  def testSerialization(self):
    # Grab all files in the first 10 directories and all other files.
    reader = self.create_single_reader(["dir-0*", "file*"], None, "/")
    self.assertEqual(10 + self.test_num_files, len(reader._filenames))
    result_filenames = []
    while True:
      # Read one file and immediately serialize.
      reader = self.READER_CLS.from_json_str(reader.to_json_str())
      reader.begin_slice(self.slice_ctx)
      try:
        result_filenames.append(reader.next().name)
      except StopIteration:
        break
    self.assertEqual(self.file_per_dir * 10 + self.test_num_files,
                     len(result_filenames))
    self.assertEqual(self.filenames_in_first_10_dirs + self.test_filenames,
                     result_filenames)

  def testSplit(self):
    # Grab all files in the first 10 directories.
    reader = self.create_single_reader(["dir-0*"], None, "/")
    self.assertEqual(10, len(reader._filenames))
    result_filenames = [f.name for f in reader]
    self.assertEqual(self.file_per_dir * 10, len(result_filenames))
    self.assertEqual(self.filenames_in_first_10_dirs, result_filenames)

    # Grab all files.
    reader = self.create_single_reader(["*"], None, "/")
    self.assertEqual(self.dirs + self.test_num_files, len(reader._filenames))
    self.assertEqual(self.file_per_dir * self.dirs + self.test_num_files,
                     len([f for f in reader]))


class GCSRecordInputReaderTest(GCSInputTestBase):
  """Tests for GoogleCloudStorageInputReader."""

  READER_CLS = input_reader.GCSRecordInputReader

  def create_test_file(self, filename, content):
    """Create a test LevelDB file with a RecordWriter.

    Args:
      filename: the name of the file in the form "/bucket/object".
      content: list of content to put in file in LevelDB format.
    """
    test_file = cloudstorage.open(filename, mode="w")
    with records.RecordsWriter(test_file) as w:
      for c in content:
        w.write(c)
    test_file.close()

  def testSingleFileNoRecord(self):
    filename = "empty-file"
    reader = self.create_single_reader([filename], [[]])

    self.assertRaises(StopIteration, reader.next)

  def testSingleFileOneRecord(self):
    filename = "single-record-file"
    reader = self.create_single_reader([filename], [["foobardata"]])

    self.assertEqual("foobardata", reader.next())
    self.assertRaises(StopIteration, reader.next)

  def testSingleFileManyRecords(self):
    filename = "many-records-file"
    data = []
    for record_num in range(100):  # Make 100 records
      data.append(("%03d" % record_num) * 10)  # Make each record 30 chars long
    reader = self.create_single_reader([filename], [data])

    for record in data:
      self.assertEqual(record, reader.next())
    self.assertRaises(StopIteration, reader.next)
    # ensure StopIteration is still raised after its first encountered
    self.assertRaises(StopIteration, reader.next)

  def testManyFilesManyRecords(self):
    filenames = []
    all_data = []
    for file_num in range(10):  # Make 10 files
      filename = "file-%03d" % file_num
      fullname = "/%s/%s" % (self.TEST_BUCKET, filename)
      data_set = []
      for record_num in range(10):  # Make 10 records, each 30 chars long
        data_set.append(("%03d" % record_num) * 10)
      self.create_test_file(fullname, data_set)
      filenames.append(filename)
      all_data.append(data_set)
    reader = self.create_single_reader(filenames, all_data)

    for data_set in all_data:
      for record in data_set:
        self.assertEqual(record, reader.next())
    self.assertRaises(StopIteration, reader.next)

  def testManyFilesSomeEmpty(self):
    filenames = []
    all_data = []
    for file_num in range(50):  # Make 10 files
      filename = "file-%03d" % file_num
      fullname = "/%s/%s" % (self.TEST_BUCKET, filename)
      data_set = []
      for record_num in range(file_num % 5):  # Make up to 4 records
        data_set.append(("%03d" % record_num) * 10)
      self.create_test_file(fullname, data_set)
      filenames.append(filename)
      all_data.append(data_set)
    reader = self.create_single_reader(filenames, all_data)

    for data_set in all_data:
      for record in data_set:
        self.assertEqual(record, reader.next())
    self.assertRaises(StopIteration, reader.next)

  def testSerialization(self):
    filenames = []
    all_data = []
    for file_num in range(50):  # Make 10 files
      filename = "file-%03d" % file_num
      fullname = "/%s/%s" % (self.TEST_BUCKET, filename)
      data_set = []
      for record_num in range(file_num % 5):  # Make up to 4 records
        data_set.append(("%03d" % record_num) * 10)
      self.create_test_file(fullname, data_set)
      filenames.append(filename)
      all_data.append(data_set)
    reader = self.create_single_reader(filenames, all_data)

    # Serialize before using
    reader = self.READER_CLS.from_json_str(reader.to_json_str())
    reader.begin_slice(self.slice_ctx)

    for data_set in all_data:
      for record in data_set:
        self.assertEqual(record, reader.next())
        # Serialize after each read
        reader = self.READER_CLS.from_json_str(reader.to_json_str())
        reader.begin_slice(self.slice_ctx)

    self.assertRaises(StopIteration, reader.next)

    # Serialize after StopIteration reached
    reader = self.READER_CLS.from_json_str(reader.to_json_str())
    reader.begin_slice(self.slice_ctx)
    self.assertRaises(StopIteration, reader.next)

  def testCounters(self):
    filenames = []
    all_data = []
    for file_num in range(10):  # Make 10 files
      filename = "file-%03d" % file_num
      fullname = "/%s/%s" % (self.TEST_BUCKET, filename)
      data_set = []
      for record_num in range(10):  # Make 10 records, each 30 chars long
        data_set.append(("%03d" % record_num) * 10)
      self.create_test_file(fullname, data_set)
      filenames.append(filename)
      all_data.append(data_set)
    reader = self.create_single_reader(filenames, all_data)

    # Counters should be 0.
    self.assertEqual(0, self.slice_ctx.counter(
        map_job.InputReader.COUNTER_IO_READ_MSEC))
    self.assertEqual(0, self.slice_ctx.counter(
        map_job.InputReader.COUNTER_IO_READ_BYTE))

    for data_set in all_data:
      for record in data_set:
        self.assertEqual(record, reader.next())
    self.assertRaises(StopIteration, reader.next)

    # Check counters.
    # Check for 10 files each with 10 records each 30 chars long.
    self.assertEqual(10 * 10 * 30, self.slice_ctx.counter(
        map_job.InputReader.COUNTER_IO_READ_BYTE))


if __name__ == "__main__":
  unittest.main()
