#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.




# Using opensource naming conventions, pylint: disable=g-bad-name


import os
import math
from testlib import mox
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import files
from google.appengine.api.files import testutil as files_testutil
from google.appengine.api.files import records
from mapreduce import context
from mapreduce import errors
from mapreduce import model
from mapreduce import output_writers
from testlib import testutil

# pylint: disable=g-import-not-at-top
# TODO(user): Cleanup imports if/when cloudstorage becomes part of runtime.
try:
  import cloudstorage
  enable_cloudstorage_tests = True
except ImportError:
  enable_cloudstorage_tests = False


FILE_WRITER_NAME = (output_writers.__name__ + "." +
                    output_writers.FileOutputWriter.__name__)


class FilePoolTest(unittest.TestCase):
  """Tests for _FilePool class."""

  def setUp(self):
    self.file_service = files_testutil.TestFileServiceStub()
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub(
        "file", self.file_service)

    self.pool = output_writers._FilePool(flush_size_chars=10)

  def testAppendAndFlush(self):
    self.pool.append("foo", "a")
    self.assertEquals("", self.file_service.get_content("foo"))
    self.pool.append("foo", "b")
    self.assertEquals("", self.file_service.get_content("foo"))
    self.pool.flush()
    self.assertEquals("ab", self.file_service.get_content("foo"))

  def testAutoFlush(self):
    self.pool.append("foo", "a"*10)
    self.pool.append("foo", "b")
    self.assertEquals("a"*10, self.file_service.get_content("foo"))
    self.pool.flush()
    self.assertEquals("a"*10 + "b", self.file_service.get_content("foo"))

  def testAppendTooMuchData(self):
    """Test appending too much data."""
    self.assertRaises(errors.Error, self.pool.append, "foo", "a"*1024*1024*2)

  def testAppendLargeData(self):
    """Test appending large amount of data.

    See b/6827293.
    """
    self.pool.append("foo", "a"*output_writers._FILES_API_FLUSH_SIZE + "a")
    self.assertEquals("a"*output_writers._FILES_API_FLUSH_SIZE + "a",
                      self.file_service.get_content("foo"))

  def testAppendMultipleFiles(self):
    self.pool.append("foo", "a")
    self.pool.append("bar", "b")
    self.pool.append("foo", "a")
    self.pool.append("bar", "b")

    self.assertEquals("", self.file_service.get_content("foo"))
    self.assertEquals("", self.file_service.get_content("bar"))
    self.pool.flush()
    self.assertEquals("aa", self.file_service.get_content("foo"))
    self.assertEquals("bb", self.file_service.get_content("bar"))


class RecordsPoolTest(unittest.TestCase):
  """Tests for RecordsPool."""

  def setUp(self):
    self.file_service = files_testutil.TestFileServiceStub()
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub(
        "file", self.file_service)

    self.pool = output_writers.RecordsPool("tempfile", flush_size_chars=30)

  def testAppendAndFlush(self):
    self.pool.append("a")
    self.assertEquals("", self.file_service.get_content("tempfile"))
    self.pool.append("b")
    self.assertEquals("", self.file_service.get_content("tempfile"))
    self.pool.flush()
    self.assertEquals(
        ["a", "b"],
        list(records.RecordsReader(files.open("tempfile", "r"))))


class FileOutputWriterTest(testutil.HandlerTestBase):

  def create_mapper_spec(self,
                         output_writer_spec=FILE_WRITER_NAME,
                         params=None):
    params = params or {}
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params,
        10,
        output_writer_spec=output_writer_spec)
    return mapper_spec

  def create_mapreduce_state(self, params=None):
    mapreduce_spec = model.MapreduceSpec(
        "mapreduce0",
        "mapreduce0",
        self.create_mapper_spec(params=params).to_json())
    mapreduce_state = model.MapreduceState.create_new("mapreduce0")
    mapreduce_state.mapreduce_spec = mapreduce_spec
    return mapreduce_state

  def testValidate_Passes(self):
    output_writers.FileOutputWriter.validate(
        self.create_mapper_spec(params={"filesystem": "blobstore"}))

  def testValidate_WriterNotSet(self):
    self.assertRaises(
        errors.BadWriterParamsError,
        output_writers.FileOutputWriter.validate,
        self.create_mapper_spec(output_writer_spec=None))

  def testValidate_ShardingNone(self):
    output_writers.FileOutputWriter.validate(self.create_mapper_spec(
        params={"output_sharding": "NONE", "filesystem": "blobstore"}))

  def testValidate_ShardingInput(self):
    output_writers.FileOutputWriter.validate(self.create_mapper_spec(
        params={"output_sharding": "input", "filesystem": "blobstore"}))

  def testValidate_ShardingIncorrect(self):
    self.assertRaises(
        errors.BadWriterParamsError,
        output_writers.FileOutputWriter.validate,
        self.create_mapper_spec(
            params={"output_sharding": "foo", "filesystem": "blobstore"}))

  def testInitJob_NoSharding(self):
    mapreduce_state = self.create_mapreduce_state(
        params={"filesystem": "blobstore"})
    output_writers.FileOutputWriter.init_job(mapreduce_state)
    self.assertTrue(mapreduce_state.writer_state)
    filenames = output_writers.FileOutputWriter.get_filenames(mapreduce_state)
    self.assertEqual(1, len(filenames))
    self.assertTrue(filenames[0].startswith("/blobstore/writable:"))

  def testInitJob_ShardingNone(self):
    mapreduce_state = self.create_mapreduce_state(
        params={"output_sharding": "none", "filesystem": "blobstore"})
    output_writers.FileOutputWriter.init_job(mapreduce_state)
    self.assertTrue(mapreduce_state.writer_state)
    filenames = output_writers.FileOutputWriter.get_filenames(mapreduce_state)
    self.assertEqual(1, len(filenames))
    self.assertTrue(filenames[0].startswith("/blobstore/writable:"))

  def testInitJob_ShardingInput(self):
    mapreduce_state = self.create_mapreduce_state(
        params={"output_sharding": "input", "filesystem": "blobstore"})
    output_writers.FileOutputWriter.init_job(mapreduce_state)
    self.assertTrue(mapreduce_state.writer_state)
    filenames = output_writers.FileOutputWriter.get_filenames(mapreduce_state)
    self.assertEqual(0, len(filenames))

  def testInitJob_GoogleStorage(self):
    mapreduce_state = self.create_mapreduce_state(
        params={"filesystem": "gs", "gs_bucket_name": "foo", "gs_acl": "public"})
    m = mox.Mox()
    m.StubOutWithMock(files.gs, "create")
    files.gs.create(mox.StrContains('/gs/foo'),
                    mox.IgnoreArg(),
                    acl="public")
    m.ReplayAll()
    output_writers.FileOutputWriter.init_job(mapreduce_state)
    m.UnsetStubs()
    m.VerifyAll()
    self.assertTrue(mapreduce_state.writer_state)

  def testValidate_MissingBucketParam(self):
    self.assertRaises(
        errors.BadWriterParamsError,
        output_writers.FileOutputWriter.validate,
        self.create_mapper_spec(
            params={"filesystem": "gs", "bucket_name": "foo"}))


class GoogleCloudStorageOutputTestBase(testutil.CloudStorageTestBase):
  """Base class for running output tests with Google Cloud Storage.

  Subclasses must define WRITER_NAME and may redefine NUM_SHARDS.
  """

  # Defaults
  NUM_SHARDS = 10

  def create_mapper_spec(self, output_params=None):
    """Create a Mapper specification using the GoogleCloudStorageOutputWriter.

    The specification generated uses a dummy handler and input reader. The
    number of shards is 10 (some number greater than 1).

    Args:
      output_params: parameters for the output writer.

    Returns:
      a model.MapperSpec with default settings and specified output_params.
    """
    return model.MapperSpec(
        "DummyHandler",
        "DummyInputReader",
        {"output_writer": output_params or {}},
        self.NUM_SHARDS,
        output_writer_spec=self.WRITER_NAME)

  def create_mapreduce_state(self, output_params=None):
    """Create a model.MapreduceState including MapreduceSpec and MapperSpec.

    Args:
      output_params: parameters for the output writer.

    Returns:
      a model.MapreduceSpec with default settings and specified output_params.
    """
    mapreduce_spec = model.MapreduceSpec(
        "DummyMapReduceJobName",
        "DummyMapReduceJobId",
        self.create_mapper_spec(output_params=output_params).to_json())
    mapreduce_state = model.MapreduceState.create_new("DummyMapReduceJobId")
    mapreduce_state.mapreduce_spec = mapreduce_spec
    mapreduce_state.put()
    return mapreduce_state

  def create_shard_state(self, shard_number):
    """Create a model.ShardState.

    Args:
      shard_number: The index for this shard (zero-indexed).

    Returns:
      a model.ShardState for the given shard.
    """
    shard_state = model.ShardState.create_new("DummyMapReduceJobId",
                                              shard_number)
    shard_state.put()
    return shard_state


class GoogleCloudStorageOutputWriterTest(GoogleCloudStorageOutputTestBase):

  WRITER_CLS = output_writers._GoogleCloudStorageOutputWriter
  WRITER_NAME = output_writers.__name__ + "." + WRITER_CLS.__name__

  def testValidate_PassesBasic(self):
    self.WRITER_CLS.validate(self.create_mapper_spec(
        output_params=
        {self.WRITER_CLS.BUCKET_NAME_PARAM: "test"}))

  def testValidate_PassesAllOptions(self):
    self.WRITER_CLS.validate(
        self.create_mapper_spec(
            output_params=
            {self.WRITER_CLS.BUCKET_NAME_PARAM: "test",
             self.WRITER_CLS.ACL_PARAM: "test-acl",
             self.WRITER_CLS.NAMING_FORMAT_PARAM:
             "fname",
             self.WRITER_CLS.CONTENT_TYPE_PARAM:
             "mime"}))

  def testValidate_NoBucket(self):
    self.assertRaises(
        errors.BadWriterParamsError,
        self.WRITER_CLS.validate,
        self.create_mapper_spec())

  def testValidate_BadBucket(self):
    # Only test a single bad name to ensure that the validator is called.
    # Full testing of the validation is in cloudstorage component.
    self.assertRaises(
        errors.BadWriterParamsError,
        self.WRITER_CLS.validate,
        self.create_mapper_spec(
            output_params=
            {self.WRITER_CLS.BUCKET_NAME_PARAM: "#"}))

  def testValidate_BadNamingTemplate(self):
    # Send a naming format that includes an unknown subsitution: $bad
    self.assertRaises(
        errors.BadWriterParamsError,
        self.WRITER_CLS.validate,
        self.create_mapper_spec(
            output_params=
            {self.WRITER_CLS.BUCKET_NAME_PARAM: "test",
             self.WRITER_CLS.NAMING_FORMAT_PARAM:
             "$bad"}))

  def testCreateWriters(self):
    mapreduce_state = self.create_mapreduce_state(
        output_params=
        {self.WRITER_CLS.BUCKET_NAME_PARAM: "test"})
    for shard_num in range(self.NUM_SHARDS):
      shard = self.create_shard_state(shard_num)
      writer = self.WRITER_CLS.create(mapreduce_state, shard)
      shard.put()
      writer.finalize(None, shard)
    filenames = self.WRITER_CLS.get_filenames(mapreduce_state)
    # Verify we have the correct number of filenames
    self.assertEqual(self.NUM_SHARDS, len(filenames))

    # Verify each has a unique filename
    self.assertEqual(self.NUM_SHARDS, len(set(filenames)))

  def testWriter(self):
    mapreduce_state = self.create_mapreduce_state(
        output_params=
        {self.WRITER_CLS.BUCKET_NAME_PARAM: "test"})
    shard_state = self.create_shard_state(0)
    writer = self.WRITER_CLS.create(mapreduce_state, shard_state)
    data = "fakedata"
    writer.write(data, None)
    writer.finalize(None, shard_state)
    filename = self.WRITER_CLS._get_filename(shard_state)

    self.assertNotEquals(None, filename)
    self.assertEqual(data, cloudstorage.open(filename).read())

  def testCreateWritersWithRetries(self):
    mapreduce_state = self.create_mapreduce_state(
        output_params=
        {self.WRITER_CLS.BUCKET_NAME_PARAM: "test"})
    shard_state = self.create_shard_state(0)

    # Create the writer for the 1st attempt
    writer = self.WRITER_CLS.create(mapreduce_state, shard_state)
    filename = self.WRITER_CLS._get_filename(
        shard_state)
    writer.write("badData", None)

    # Test re-creating the writer for a retry
    shard_state.reset_for_retry()
    writer = self.WRITER_CLS.create(mapreduce_state, shard_state)
    new_filename = self.WRITER_CLS._get_filename(shard_state)
    good_data = "goodData"
    writer.write(good_data, None)
    writer.finalize(None, shard_state)

    # Verify the retry has a different filename
    self.assertNotEqual(filename, new_filename)

    # Verify the badData is not in the final file
    self.assertEqual(good_data, cloudstorage.open(new_filename).read())

  def testWriterMetadata(self):
    test_acl = "test-acl"
    test_content_type = "test-mime"
    mapreduce_state = self.create_mapreduce_state(
        output_params=
        {self.WRITER_CLS.BUCKET_NAME_PARAM: "test",
         self.WRITER_CLS.ACL_PARAM: test_acl,
         self.WRITER_CLS.CONTENT_TYPE_PARAM:
         test_content_type})
    shard_state = self.create_shard_state(0)

    writer = self.WRITER_CLS.create(mapreduce_state, shard_state)
    writer.finalize(None, shard_state)

    filename = self.WRITER_CLS._get_filename(
        shard_state)

    file_stat = cloudstorage.stat(filename)
    self.assertEqual(test_content_type, file_stat.content_type)
    # TODO(user) Add support in the stub to retrieve acl metadata

  def testWriterSerialization(self):
    mapreduce_state = self.create_mapreduce_state(
        output_params=
        {self.WRITER_CLS.BUCKET_NAME_PARAM: "test"})
    shard_state = self.create_shard_state(0)
    writer = self.WRITER_CLS.create(mapreduce_state, shard_state)
    # data expliclity contains binary data
    data = "\"fake\"\tdatathatishardtoencode"
    writer.write(data, None)

    # Serialize/deserialize writer after some data written
    writer = self.WRITER_CLS.from_json(writer.to_json())
    writer.write(data, None)

    # Serialize/deserialize writer after more data written
    writer = self.WRITER_CLS.from_json(writer.to_json())
    writer.finalize(None, shard_state)

    # Serialize/deserialize writer after finalization
    writer = self.WRITER_CLS.from_json(writer.to_json())
    self.assertRaises(IOError, writer.write, data, None)

    filename = self.WRITER_CLS._get_filename(shard_state)

    self.assertNotEquals(None, filename)
    self.assertEqual(data + data, cloudstorage.open(filename).read())

  def testWriterCounters(self):
    mapreduce_state = self.create_mapreduce_state(
        output_params=
        {self.WRITER_CLS.BUCKET_NAME_PARAM: "test"})
    shard_state = self.create_shard_state(0)
    writer = self.WRITER_CLS.create(mapreduce_state, shard_state)
    ctx = context.Context(mapreduce_state.mapreduce_spec, shard_state)
    # Write large amount of data to ensure measurable time passes during write.
    data = "d" * 1024 * 1024 * 10
    writer.write(data, ctx)
    self.assertEqual(len(data), shard_state.counters_map.get(
        output_writers.COUNTER_IO_WRITE_BYTES))
    self.assertTrue(shard_state.counters_map.get(
        output_writers.COUNTER_IO_WRITE_MSEC) > 0)


class GoogleCloudStorageRecordOutputWriterTest(
    GoogleCloudStorageOutputTestBase):

  BUCKET_NAME = "test"
  WRITER_CLS = output_writers._GoogleCloudStorageRecordOutputWriter
  WRITER_NAME = output_writers.__name__ + "." + WRITER_CLS.__name__

  def create_mapreduce_state(self, output_params=None):
    """Create a model.MapreduceState including MapreduceSpec and MapperSpec.

    Args:
      output_params: parameters for the output writer.

    Returns:
      a model.MapreduceSpec with default settings and specified output_params.
    """
    all_params = {self.WRITER_CLS.BUCKET_NAME_PARAM: self.BUCKET_NAME}
    all_params.update(output_params or {})
    return super(GoogleCloudStorageRecordOutputWriterTest,
                 self).create_mapreduce_state(all_params)

  def setupWriter(self, flush_size=None):
    """Create an Google Cloud Storage LevelDB record output writer.

    Args:
      flush_size: optional setting for the buffer size before data is flushed
        to the cloudstorage library.

    Returns:
      a model.MapreduceSpec.
    """
    self.mapreduce_state = self.create_mapreduce_state(
        output_params={self.WRITER_CLS.FLUSH_SIZE_PARAM: flush_size})
    self.shard_state = self.create_shard_state(0)
    self.writer = self.WRITER_CLS.create(self.mapreduce_state, self.shard_state)
    self.ctx = context.Context(self.mapreduce_state.mapreduce_spec,
                               self.shard_state)

  def testWrite_RecordSizeSmallerThanFlush(self):
    record_size = 10
    flush_size = record_size + 2 * records.HEADER_LENGTH
    self.setupWriter(flush_size=flush_size)

    self.writer.write("d" * record_size, self.ctx)

    # No data should have been flushed
    self.assertEqual(1, len(self.writer._buffer))
    self.assertEqual(record_size, len(self.writer._buffer[0]))
    self.assertEqual(0, self.shard_state.counters_map.get(
        output_writers.COUNTER_IO_WRITE_BYTES))

    # Flush
    self.writer._flush(self.ctx)

    # No data should be in the record writer buffer
    self.assertEqual(0, len(self.writer._buffer))
    # All data should be written cloudstorage library
    self.assertEqual(records.BLOCK_SIZE, self.shard_state.counters_map.get(
        output_writers.COUNTER_IO_WRITE_BYTES))

  def testWrite_RecordSizeLargerThanFlush(self):
    record_size = 10
    flush_size = 1
    self.setupWriter(flush_size=flush_size)

    self.writer.write("d" * record_size, self.ctx)

    # No data should be in the record writer buffer
    self.assertEqual(0, len(self.writer._buffer))
    # All data should be written cloudstorage library
    self.assertEqual(records.BLOCK_SIZE, self.shard_state.counters_map.get(
        output_writers.COUNTER_IO_WRITE_BYTES))

  def testWrite_FlushLargerThanBlock(self):
    record_size = int(records.BLOCK_SIZE / 10)
    flush_size = records.BLOCK_SIZE * 2
    num_records = int(math.ceil(float(flush_size) / record_size))
    self.setupWriter(flush_size=flush_size)

    # Write all but the last recrod
    for _ in range(num_records - 1):
      self.writer.write("d" * record_size, self.ctx)

    # No data should have been flushed
    self.assertEqual(num_records - 1, len(self.writer._buffer))
    self.assertEqual(0, self.shard_state.counters_map.get(
        output_writers.COUNTER_IO_WRITE_BYTES))

    # Write the last record that should put the buffer over flush size
    self.writer.write("d" * record_size, self.ctx)

    # No data should be in the record writer buffer
    self.assertEqual(0, len(self.writer._buffer))
    # All data should be written cloudstorage library (exact size will
    # dependent on how many records were split across multiple blocks
    min_expected_size = (record_size + records.HEADER_LENGTH) * num_records
    self.assertTrue(min_expected_size < self.shard_state.counters_map.get(
        output_writers.COUNTER_IO_WRITE_BYTES))

  def testSerialization(self):
    record_size = records.BLOCK_SIZE / 3
    # Approximate, does not account for headers
    records_per_flush = 5
    flush_size = record_size * records_per_flush - 1

    self.setupWriter(flush_size=flush_size)

    # Serialize un-used writer
    self.writer = self.WRITER_CLS.from_json(self.writer.to_json())

    # Write single record
    self.writer.write("d" * record_size, self.ctx)

    # No data should have been flushed
    self.assertEqual(1, len(self.writer._buffer))
    self.assertEqual(0, self.shard_state.counters_map.get(
        output_writers.COUNTER_IO_WRITE_BYTES))

    # Serialize
    self.writer = self.WRITER_CLS.from_json(self.writer.to_json())

    # No data should be in the record writer buffer
    self.assertEqual(0, len(self.writer._buffer))
    # A full (padded) block should have been flushed
    self.assertEqual(records.BLOCK_SIZE, self.shard_state.counters_map.get(
        output_writers.COUNTER_IO_WRITE_BYTES))

    # Write enough records to trigger self-flush
    for _ in range(records_per_flush):
      self.writer.write("d" * record_size, self.ctx)

    # No data should be in the record writer buffer
    self.assertEqual(0, len(self.writer._buffer))
    data_written = self.shard_state.counters_map.get(
        output_writers.COUNTER_IO_WRITE_BYTES)
    # At least 2 blocks
    self.assertTrue(records.BLOCK_SIZE * 2 < data_written)
    # Only full blocks writen
    self.assertEqual(0, data_written % records.BLOCK_SIZE)

    # Serialize
    self.writer = self.WRITER_CLS.from_json(self.writer.to_json())


if __name__ == "__main__":
  unittest.main()
