#!/usr/bin/env python
# Copyright 2010 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.




# Using opensource naming conventions, pylint: disable=g-bad-name

import unittest


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


class GCSRecordsPoolTest(testutil.CloudStorageTestBase):
  """Tests for GCSRecordsPool."""

  def setUp(self):
    super(GCSRecordsPoolTest, self).setUp()
    bucket_name = "testbucket"
    test_filename = "testfile"

    self.filename = "/%s/%s" % (bucket_name, test_filename)
    self.filehandle = cloudstorage.open(self.filename, mode="w")
    self.pool = output_writers.GCSRecordsPool(self.filehandle,
                                              flush_size_chars=30)

  def testAppendAndFlush(self):
    self.pool.append("a")
    self.assertRaises(cloudstorage.errors.NotFoundError, cloudstorage.open,
                      self.filename)
    self.pool.append("b")
    self.assertRaises(cloudstorage.errors.NotFoundError, cloudstorage.open,
                      self.filename)
    self.pool.flush()
    self.assertRaises(cloudstorage.errors.NotFoundError, cloudstorage.open,
                      self.filename)
    # File handle does need to be explicitly closed.
    self.filehandle.close()
    self.assertEquals(32 * 1024, cloudstorage.stat(self.filename).st_size)
    self.assertEquals(
        ["a", "b"],
        list(records.RecordsReader(cloudstorage.open(self.filename))))

  def testAppendAndForceFlush(self):
    self.pool.append("a")
    self.assertRaises(cloudstorage.errors.NotFoundError, cloudstorage.open,
                      self.filename)
    self.pool.append("b")
    self.assertRaises(cloudstorage.errors.NotFoundError, cloudstorage.open,
                      self.filename)
    self.pool.flush(True)
    self.assertRaises(cloudstorage.errors.NotFoundError, cloudstorage.open,
                      self.filename)
    # File handle does need to be explicitly closed.
    self.filehandle.close()
    # Check the file size contains the padding.
    self.assertEquals(256 * 1024, cloudstorage.stat(self.filename).st_size)
    self.assertEquals(
        ["a", "b"],
        list(records.RecordsReader(cloudstorage.open(self.filename))))


class GCSOutputTestBase(object):
  """Base class for running output tests with Google Cloud Storage.

  Subclasses must define WRITER_NAME and may redefine NUM_SHARDS.
  """

  # Defaults
  NUM_SHARDS = 10
  WRITER_CLS = None

  def _serialize_and_deserialize(self, writer):
    writer.end_slice(None)
    writer = self.WRITER_CLS.from_json(writer.to_json())
    writer.begin_slice(None)
    return writer

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


class GCSOutputWriterNoDupModeTest(GCSOutputTestBase,
                                   testutil.CloudStorageTestBase):

  WRITER_CLS = output_writers.GoogleCloudStorageOutputWriter
  WRITER_NAME = output_writers.__name__ + "." + WRITER_CLS.__name__

  def setUp(self):
    super(GCSOutputWriterNoDupModeTest, self).setUp()
    self.mr_state = self.create_mapreduce_state(
        output_params=
        {self.WRITER_CLS.BUCKET_NAME_PARAM: "test",
         self.WRITER_CLS._NO_DUPLICATE: True})

  def testValidate_NoDuplicateParam(self):
    # Good.
    self.WRITER_CLS.validate(self.create_mapper_spec(
        output_params={self.WRITER_CLS.BUCKET_NAME_PARAM: "test",
                       self.WRITER_CLS._NO_DUPLICATE: True}))

    # Bad. Expect a boolean.
    self.assertRaises(
        errors.BadWriterParamsError,
        self.WRITER_CLS.validate,
        self.create_mapper_spec(
            output_params=
            {self.WRITER_CLS.BUCKET_NAME_PARAM: "test",
             self.WRITER_CLS._NO_DUPLICATE: "False"}))

  def testSmoke(self):
    tmp_files = set()
    final_files = set()
    for shard_num in range(self.NUM_SHARDS):
      shard = self.create_shard_state(shard_num)
      writer = self.WRITER_CLS.create(self.mr_state.mapreduce_spec,
                                      shard.shard_number, 0)
      # Verify files are created under tmp dir.
      tmp_file = writer._streaming_buffer.name
      self.assertTrue(self.WRITER_CLS._MR_TMP in tmp_file)
      tmp_files.add(tmp_file)
      cxt = context.Context(self.mr_state.mapreduce_spec, shard)
      writer.finalize(cxt, shard)
      # Verify the integrity of writer state.
      self.assertEqual(
          writer._streaming_buffer.name,
          (shard.writer_state[self.WRITER_CLS._SEG_PREFIX] +
           str(shard.writer_state[self.WRITER_CLS._LAST_SEG_INDEX])))
      final_file = shard.writer_state["filename"]
      self.assertFalse(self.WRITER_CLS._MR_TMP in final_file)
      final_files.add(final_file)

    # Verify all filenames are different.
    self.assertEqual(self.NUM_SHARDS, len(tmp_files))
    self.assertEqual(self.NUM_SHARDS, len(final_files))

  def testSerialization(self):
    mr_spec = self.mr_state.mapreduce_spec
    shard_state = self.create_shard_state(0)
    ctx = context.Context(mr_spec, shard_state)
    context.Context._set(ctx)

    writer = self.WRITER_CLS.create(mr_spec, 0, 0)
    writer._seg_index = 1
    writer.write("abcde")

    writer = self.WRITER_CLS.from_json_str(writer.to_json_str())
    # _seg_index doesn't change.
    self.assertEqual(1, writer._seg_index)
    # _seg_valid_length is updated to what was in the buffer.
    self.assertEqual(len("abcde"), writer._seg_valid_length)

  def testRecoverNothingWrittenInFailedInstance(self):
    mr_spec = self.mr_state.mapreduce_spec
    shard_state = self.create_shard_state(0)
    ctx = context.Context(mr_spec, shard_state)
    context.Context._set(ctx)

    writer = self.WRITER_CLS.create(mr_spec, 0, 0)
    self.assertEqual(0, writer._seg_index)
    new_writer = writer._recover(mr_spec, 0, 0)
    # Old instance is not finalized.
    self.assertFalse(0, writer._streaming_buffer.closed)
    # seg index is not incremented.
    self.assertEqual(0, new_writer._seg_index)

  def testRecoverSomethingWrittenInFailedInstance(self):
    mr_spec = self.mr_state.mapreduce_spec
    shard_state = self.create_shard_state(0)
    ctx = context.Context(mr_spec, shard_state)
    context.Context._set(ctx)

    writer = self.WRITER_CLS.create(mr_spec, 0, 0)
    writer.write("123")
    writer = self.WRITER_CLS.from_json(writer.to_json())
    writer.write("4")

    new_writer = writer._recover(mr_spec, 0, 0)
    # Old instance is finalized and valid offset saved.
    old_stat = cloudstorage.stat(writer._streaming_buffer.name)
    self.assertEqual(
        len("123"),
        int(old_stat.metadata[self.WRITER_CLS._VALID_LENGTH]))
    # New instance is created with an incremented seg index.
    self.assertEqual(writer._seg_index + 1, new_writer._seg_index)

    # Verify filenames.
    self.assertTrue(
        writer._streaming_buffer.name.endswith(str(writer._seg_index)))
    self.assertTrue(
        new_writer._streaming_buffer.name.endswith(str(new_writer._seg_index)))


class GCSOutputWriterTestCommon(GCSOutputTestBase):

  # GoogleCloudStorageOutputWriter and
  # GoogleCloudStorageConsistentOutputWriter both run all of these tests.

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
      writer = self.WRITER_CLS.create(mapreduce_state.mapreduce_spec,
                                      shard.shard_number, 0)
      cxt = context.Context(mapreduce_state.mapreduce_spec,
                            shard)
      shard.result_status = model.ShardState.RESULT_SUCCESS
      writer.finalize(cxt, shard)
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

    writer = self.WRITER_CLS.create(mapreduce_state.mapreduce_spec,
                                    shard_state.shard_number, 0)
    writer.begin_slice(None)
    data = "fakedata"
    writer.write(data)
    writer = self._serialize_and_deserialize(writer)
    writer.finalize(ctx, shard_state)
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
    writer = self.WRITER_CLS.create(mapreduce_state.mapreduce_spec,
                                    shard_state.shard_number,
                                    shard_state.retries + 1)
    new_filename = writer._get_filename_for_test()
    writer.begin_slice(None)
    writer.write("initData")
    writer.end_slice(None)

    orig_json = writer.to_json()

    writer = self.WRITER_CLS.from_json(orig_json)
    writer.begin_slice(None)
    writer.write("badData")  # we fail here so this data should be discarded

    # Recreate the same rewrite (simulates a slice retry).
    writer = self.WRITER_CLS.from_json(orig_json)
    writer.begin_slice(None)
    writer.write("goodData")
    writer.end_slice(None)
    writer = self._serialize_and_deserialize(writer)
    writer.finalize(ctx, shard_state)

    # Verify the badData is not in the final file
    self.assertEqual("initDatagoodData", cloudstorage.open(new_filename).read())

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

    writer = self.WRITER_CLS.create(mapreduce_state.mapreduce_spec,
                                    shard_state.shard_number,
                                    0)
    writer = self.WRITER_CLS.from_json(writer.to_json())
    writer.finalize(ctx, shard_state)

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

    writer = self.WRITER_CLS.create(mapreduce_state.mapreduce_spec,
                                    shard_state.shard_number, 0)
    writer.begin_slice(None)
    # data expliclity contains binary data
    data = "\"fake\"\tdatathatishardtoencode"
    writer.write(data)

    # Serialize/deserialize writer after some data written
    writer = self._serialize_and_deserialize(writer)
    writer.write(data)

    # Serialize/deserialize writer after more data written
    writer = self._serialize_and_deserialize(writer)
    writer.finalize(ctx, shard_state)

    # Serialize/deserialize writer after finalization
    writer = self._serialize_and_deserialize(writer)
    self.assertRaises(IOError, writer.write, data)

    filename = self.WRITER_CLS._get_filename(shard_state)

    self.assertNotEquals(None, filename)
    self.assertEqual(data + data, cloudstorage.open(filename).read())

  def testWriterCounters(self):
    mapreduce_state = self.create_mapreduce_state(
        output_params=
        {self.WRITER_CLS.BUCKET_NAME_PARAM: "test"})
    shard_state = self.create_shard_state(0)
    writer = self.WRITER_CLS.create(mapreduce_state.mapreduce_spec,
                                    shard_state.shard_number, 0)
    writer.begin_slice(None)
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


class GCSRecordOutputWriterTestBase(GCSOutputTestBase):

  WRITER_CLS = None
  WRITER_NAME = None
  BUCKET_NAME = "test"

  def create_mapreduce_state(self, output_params=None):
    """Create a model.MapreduceState including MapreduceSpec and MapperSpec.

    Args:
      output_params: parameters for the output writer.

    Returns:
      a model.MapreduceSpec with default settings and specified output_params.
    """
    all_params = {self.WRITER_CLS.BUCKET_NAME_PARAM: self.BUCKET_NAME}
    all_params.update(output_params or {})
    return super(GCSRecordOutputWriterTestBase, self).create_mapreduce_state(
        all_params)

  def setupWriter(self):
    """Create an Google Cloud Storage LevelDB record output writer.

    Returns:
      a model.MapreduceSpec.
    """
    self.mapreduce_state = self.create_mapreduce_state()
    self.shard_state = self.create_shard_state(0)
    self.writer = self.WRITER_CLS.create(self.mapreduce_state.mapreduce_spec,
                                         self.shard_state.shard_number,
                                         self.shard_state.retries + 1)
    self.writer.begin_slice(None)
    self.ctx = context.Context(self.mapreduce_state.mapreduce_spec,
                               self.shard_state)
    context.Context._set(self.ctx)

  def testSmoke(self):
    data_size = 10
    self.setupWriter()

    # Serialize un-used writer
    self.writer = self._serialize_and_deserialize(self.writer)

    # Write single record
    self.writer.write("d" * data_size)

    self.assertEqual(data_size + records._HEADER_LENGTH,
                     self.shard_state.counters_map.get(
                         output_writers.COUNTER_IO_WRITE_BYTES))

    # Serialize
    self.writer = self._serialize_and_deserialize(self.writer)

    # A full (padded) block should have been flushed
    self.assertEqual(records._BLOCK_SIZE, self.shard_state.counters_map.get(
        output_writers.COUNTER_IO_WRITE_BYTES))

    # Writer a large record.
    self.writer.write("d" * records._BLOCK_SIZE)

    self.assertEqual(records._BLOCK_SIZE + records._BLOCK_SIZE +
                     2 * records._HEADER_LENGTH,
                     self.shard_state.counters_map.get(
                         output_writers.COUNTER_IO_WRITE_BYTES))

    self.writer = self._serialize_and_deserialize(self.writer)
    self.writer.finalize(self.ctx, self.shard_state)


class GCSRecordOutputWriterTest(GCSRecordOutputWriterTestBase,
                                testutil.CloudStorageTestBase):

  WRITER_CLS = output_writers.GoogleCloudStorageRecordOutputWriter
  WRITER_NAME = output_writers.__name__ + "." + WRITER_CLS.__name__


class GCSConsistentRecordOutputWriterTest(GCSRecordOutputWriterTestBase,
                                          testutil.CloudStorageTestBase):

  WRITER_CLS = output_writers.GoogleCloudStorageConsistentRecordOutputWriter
  WRITER_NAME = output_writers.__name__ + "." + WRITER_CLS.__name__


class GCSOutputWriterTest(GCSOutputWriterTestCommon,
                          testutil.CloudStorageTestBase):

  WRITER_CLS = output_writers.GoogleCloudStorageOutputWriter
  WRITER_NAME = output_writers.__name__ + "." + WRITER_CLS.__name__


class GCSOutputConsistentOutputWriterTest(GCSOutputWriterTestCommon,
                                          testutil.CloudStorageTestBase):

  WRITER_CLS = output_writers.GoogleCloudStorageConsistentOutputWriter
  WRITER_NAME = output_writers.__name__ + "." + WRITER_CLS.__name__

  def testFinalizeChecksForErrors(self):
    """Just make sure finalize is never called after processing data."""
    mapreduce_state = self.create_mapreduce_state(
        output_params=
        {self.WRITER_CLS.BUCKET_NAME_PARAM: "test"})
    shard_state = self.create_shard_state(0)
    ctx = context.Context(mapreduce_state.mapreduce_spec, shard_state)
    context.Context._set(ctx)

    writer = self.WRITER_CLS.create(mapreduce_state.mapreduce_spec,
                                    shard_state.shard_number, 0)
    writer.begin_slice(None)
    writer.write("foobar")
    # We wrote something, finalize must fail (sanity check).
    self.assertRaises(errors.FailJobError, writer.finalize, ctx, shard_state)

  def testTemporaryFilesGetCleanedUp(self):
    mapreduce_state = self.create_mapreduce_state(
        output_params=
        {self.WRITER_CLS.BUCKET_NAME_PARAM: "test"})
    shard_state = self.create_shard_state(0)
    ctx = context.Context(mapreduce_state.mapreduce_spec, shard_state)
    context.Context._set(ctx)

    writer = self.WRITER_CLS.create(mapreduce_state.mapreduce_spec,
                                    shard_state.shard_number, 0)
    writer.begin_slice(None)
    writer.write("foo")
    writer = self.WRITER_CLS.from_json(writer.to_json())
    writer.write("bar")
    writer = self.WRITER_CLS.from_json(writer.to_json())
    writer.write("foo again")
    writer = self.WRITER_CLS.from_json(writer.to_json())
    writer.finalize(ctx, shard_state)

    names = [l.filename for l in cloudstorage.listbucket("/test")]
    self.assertEquals(
        ["/test/DummyMapReduceJobName/DummyMapReduceJobId/output-0"], names)

  def testRemovingIgnoredNonExistent(self):
    writer_spec = {self.WRITER_CLS.BUCKET_NAME_PARAM: "test"}
    mapreduce_state = self.create_mapreduce_state(output_params=writer_spec)
    shard_state = self.create_shard_state(0)
    ctx = context.Context(mapreduce_state.mapreduce_spec, shard_state)
    context.Context._set(ctx)

    writer = self.WRITER_CLS.create(mapreduce_state.mapreduce_spec,
                                    shard_state.shard_number, 0)
    writer._remove_tmpfile(None, writer_spec)  # no exceptions
    writer._remove_tmpfile("/test/i_dont_exist", writer_spec)

  def testTmpfileName(self):
    writer_spec = {self.WRITER_CLS.BUCKET_NAME_PARAM: "test"}
    mapreduce_state = self.create_mapreduce_state(output_params=writer_spec)
    shard_state = self.create_shard_state(19)
    ctx = context.Context(mapreduce_state.mapreduce_spec, shard_state)
    context.Context._set(ctx)

    writer = self.WRITER_CLS.create(mapreduce_state.mapreduce_spec,
                                    shard_state.shard_number, 0)
    writer.begin_slice(None)

    prefix = "/test/gae_mr_tmp/DummyMapReduceJobId-tmp-19-"
    tmpfile_name = writer.status.tmpfile.name
    self.assertTrue(tmpfile_name.startswith(prefix),
                    "Test file name is: %s" % tmpfile_name)

  def testTmpDefaultsToMain(self):
    writer_spec = {self.WRITER_CLS.BUCKET_NAME_PARAM: "bucket",
                   self.WRITER_CLS._ACCOUNT_ID_PARAM: "account"}
    mapreduce_state = self.create_mapreduce_state(output_params=writer_spec)
    shard_state = self.create_shard_state(1)
    ctx = context.Context(mapreduce_state.mapreduce_spec, shard_state)
    context.Context._set(ctx)

    writer = self.WRITER_CLS.create(mapreduce_state.mapreduce_spec,
                                    shard_state.shard_number, 0)

    self.assertEquals("bucket", writer._get_tmp_gcs_bucket(writer_spec))
    self.assertEquals("account", writer._get_tmp_account_id(writer_spec))

  def testTmpTakesPrecedence(self):
    writer_spec = {self.WRITER_CLS.BUCKET_NAME_PARAM: "bucket",
                   self.WRITER_CLS._ACCOUNT_ID_PARAM: "account",
                   self.WRITER_CLS.TMP_BUCKET_NAME_PARAM: "tmp_bucket",
                   self.WRITER_CLS._TMP_ACCOUNT_ID_PARAM: None}
    mapreduce_state = self.create_mapreduce_state(output_params=writer_spec)
    shard_state = self.create_shard_state(1)
    ctx = context.Context(mapreduce_state.mapreduce_spec, shard_state)
    context.Context._set(ctx)

    writer = self.WRITER_CLS.create(mapreduce_state.mapreduce_spec,
                                    shard_state.shard_number, 0)

    self.assertEquals("tmp_bucket", writer._get_tmp_gcs_bucket(writer_spec))
    self.assertEquals(None, writer._get_tmp_account_id(writer_spec))

  def testRemoveGarbage(self):
    """Make sure abandoned files get removed."""
    writer_spec = {self.WRITER_CLS.BUCKET_NAME_PARAM: "unused",
                   self.WRITER_CLS.TMP_BUCKET_NAME_PARAM: "test"}
    mapreduce_state = self.create_mapreduce_state(output_params=writer_spec)
    shard_state = self.create_shard_state(1)
    ctx = context.Context(mapreduce_state.mapreduce_spec, shard_state)
    context.Context._set(ctx)

    writer = self.WRITER_CLS.create(mapreduce_state.mapreduce_spec,
                                    shard_state.shard_number, 0)
    writer.begin_slice(None)

    # our shard
    our_file = "/test/gae_mr_tmp/DummyMapReduceJobId-tmp-1-very-random"
    f = cloudstorage.open(our_file, "w")
    f.write("foo?")
    f.close()

    # not our shard
    their_file = "/test/gae_mr_tmp/DummyMapReduceJobId-tmp-3-very-random"
    f = cloudstorage.open(their_file, "w")
    f.write("bar?")
    f.close()

    # unrelated file
    real_file = "/test/this_things_should_survive"
    f = cloudstorage.open(real_file, "w")
    f.write("yes, foobar!")
    f.close()

    # Make sure bogus file still exists
    names = [l.filename for l in cloudstorage.listbucket("/test")]
    self.assertTrue(our_file in names)
    self.assertTrue(their_file in names)
    self.assertTrue(real_file in names)

    # slice end should clean up the garbage
    writer = self._serialize_and_deserialize(writer)

    names = [l.filename for l in cloudstorage.listbucket("/test")]
    self.assertFalse(our_file in names)
    self.assertTrue(their_file in names)
    self.assertTrue(real_file in names)

    # finalize shouldn't change anything
    writer.finalize(ctx, shard_state)
    self.assertFalse(our_file in names)
    self.assertTrue(their_file in names)
    self.assertTrue(real_file in names)


if __name__ == "__main__":
  unittest.main()
