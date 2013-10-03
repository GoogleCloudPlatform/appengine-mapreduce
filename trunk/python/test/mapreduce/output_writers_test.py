#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.




# Using opensource naming conventions, pylint: disable=g-bad-name


from testlib import mox
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import files
from google.appengine.api.files import testutil as files_testutil
from mapreduce import context
from mapreduce import errors
from mapreduce import model
from mapreduce import output_writers
from mapreduce import records
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

  def testNoShardingSpecified(self):
    """Test default output_sharding is one shared output file."""
    self.assertEqual(
        output_writers.FileOutputWriter.OUTPUT_SHARDING_NONE,
        output_writers.FileOutputWriter._get_output_sharding(
            mapper_spec=self.create_mapper_spec(params={"filesystem": "gs"})))

  def testInitJob_ShardingNone(self):
    mapreduce_state = self.create_mapreduce_state(
        params={"output_sharding": "none", "filesystem": "blobstore"})
    output_writers.FileOutputWriter.init_job(mapreduce_state)
    self.assertTrue(mapreduce_state.writer_state)
    filenames = output_writers.FileOutputWriter._State.from_json(
        mapreduce_state.writer_state).filenames
    self.assertEqual(1, len(filenames))
    self.assertTrue(filenames[0].startswith("/blobstore/writable:"))

  def testInitJob_ShardingInput(self):
    mapreduce_state = self.create_mapreduce_state(
        params={"output_sharding": "input", "filesystem": "blobstore"})
    output_writers.FileOutputWriter.init_job(mapreduce_state)
    self.assertTrue(mapreduce_state.writer_state)
    filenames = output_writers.FileOutputWriter._State.from_json(
        mapreduce_state.writer_state).filenames
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

  def testGetFilenamesNoInput(self):
    """Tests get_filenames when no other writer's methods are called.

    Emulates the zero input case.

    Other tests on get_filenames see output_writers_end_to_end_test.
    """
    self.assertEqual(
        [], output_writers.FileOutputWriter.get_filenames(
            self.create_mapreduce_state(params={"filesystem": "blobstore"})))

  def testValidate_MissingBucketParam(self):
    self.assertRaises(
        errors.BadWriterParamsError,
        output_writers.FileOutputWriter.validate,
        self.create_mapper_spec(
            params={"filesystem": "gs", "bucket_name": "foo"}))

  def testFromJson183Compat(self):
    writer = output_writers.FileOutputWriter.from_json({"filename": "foo"})
    self.assertEqual("foo", writer._filename)
    self.assertEqual(None, writer._request_filename)


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
      shard.result_status = model.ShardState.RESULT_SUCCESS
      writer.finalize(None, shard)
      shard.put()
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
    ctx = context.Context(mapreduce_state.mapreduce_spec, shard_state)
    context.Context._set(ctx)

    writer = self.WRITER_CLS.create(mapreduce_state, shard_state)
    data = "fakedata"
    writer.write(data)
    writer.finalize(None, shard_state)
    filename = self.WRITER_CLS._get_filename(shard_state)

    self.assertNotEquals(None, filename)
    self.assertEqual(data, cloudstorage.open(filename).read())

  def testCreateWritersWithRetries(self):
    mapreduce_state = self.create_mapreduce_state(
        output_params=
        {self.WRITER_CLS.BUCKET_NAME_PARAM: "test"})
    shard_state = self.create_shard_state(0)
    ctx = context.Context(mapreduce_state.mapreduce_spec, shard_state)
    context.Context._set(ctx)

    # Create the writer for the 1st attempt
    writer = self.WRITER_CLS.create(mapreduce_state, shard_state)
    filename = writer._filename
    writer.write("badData")

    # Test re-creating the writer for a retry
    shard_state.reset_for_retry()
    writer = self.WRITER_CLS.create(mapreduce_state, shard_state)
    new_filename = writer._filename
    good_data = "goodData"
    writer.write(good_data)
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
    ctx = context.Context(mapreduce_state.mapreduce_spec, shard_state)
    context.Context._set(ctx)

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
    ctx = context.Context(mapreduce_state.mapreduce_spec, shard_state)
    context.Context._set(ctx)

    writer = self.WRITER_CLS.create(mapreduce_state, shard_state)
    # data expliclity contains binary data
    data = "\"fake\"\tdatathatishardtoencode"
    writer.write(data)

    # Serialize/deserialize writer after some data written
    writer = self.WRITER_CLS.from_json(writer.to_json())
    writer.write(data)

    # Serialize/deserialize writer after more data written
    writer = self.WRITER_CLS.from_json(writer.to_json())
    writer.finalize(None, shard_state)

    # Serialize/deserialize writer after finalization
    writer = self.WRITER_CLS.from_json(writer.to_json())
    self.assertRaises(IOError, writer.write, data)

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
    context.Context._set(ctx)

    # Write large amount of data to ensure measurable time passes during write.
    data = "d" * 1024 * 1024 * 10
    writer.write(data)
    self.assertEqual(len(data), shard_state.counters_map.get(
        output_writers.COUNTER_IO_WRITE_BYTES))
    self.assertTrue(shard_state.counters_map.get(
        output_writers.COUNTER_IO_WRITE_MSEC) > 0)

  def testGetFilenamesNoInput(self):
    """Tests get_filenames when no other writer's methods are called.

    Emulates the zero input case.

    Other tests on get_filenames see output_writers_end_to_end_test.
    """
    mapreduce_state = self.create_mapreduce_state(
        output_params={self.WRITER_CLS.BUCKET_NAME_PARAM: "test"})
    self.assertEqual([], self.WRITER_CLS.get_filenames(mapreduce_state))


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

  def setupWriter(self):
    """Create an Google Cloud Storage LevelDB record output writer.

    Returns:
      a model.MapreduceSpec.
    """
    self.mapreduce_state = self.create_mapreduce_state()
    self.shard_state = self.create_shard_state(0)
    self.writer = self.WRITER_CLS.create(self.mapreduce_state, self.shard_state)
    self.ctx = context.Context(self.mapreduce_state.mapreduce_spec,
                               self.shard_state)
    context.Context._set(self.ctx)

  def testSmoke(self):
    data_size = 10
    self.setupWriter()

    # Serialize un-used writer
    self.writer = self.WRITER_CLS.from_json_str(self.writer.to_json_str())

    # Write single record
    self.writer.write("d" * data_size)

    self.assertEqual(data_size + records._HEADER_LENGTH,
                     self.shard_state.counters_map.get(
                         output_writers.COUNTER_IO_WRITE_BYTES))

    # Serialize
    self.writer = self.WRITER_CLS.from_json_str(self.writer.to_json_str())

    # A full (padded) block should have been flushed
    self.assertEqual(records._BLOCK_SIZE, self.shard_state.counters_map.get(
        output_writers.COUNTER_IO_WRITE_BYTES))

    # Writer a large record.
    self.writer.write("d" * records._BLOCK_SIZE)

    self.assertEqual(records._BLOCK_SIZE + records._BLOCK_SIZE +
                     2 * records._HEADER_LENGTH,
                     self.shard_state.counters_map.get(
                         output_writers.COUNTER_IO_WRITE_BYTES))

    self.writer.finalize(self.ctx, self.shard_state)
    self.writer = self.WRITER_CLS.from_json_str(self.writer.to_json_str())


if __name__ == "__main__":
  unittest.main()
