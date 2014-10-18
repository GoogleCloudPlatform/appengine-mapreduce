#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.





import unittest

from mapreduce.third_party import pipeline

from google.appengine.api.files import file_service_pb
import cloudstorage
from mapreduce import base_handler
from mapreduce import mapreduce_pipeline
from mapreduce import output_writers
from mapreduce import records
from mapreduce import shuffler
from mapreduce import test_support
from testlib import testutil


class HashEndToEndTest(testutil.HandlerTestBase):
  """End-to-end test for _HashPipeline."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  # pylint: disable=invalid-name
  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testHashingMultipleFiles(self):
    """Test hashing files."""
    input_data = [(str(i), str(i)) for i in range(100)]
    input_data.sort()

    bucket_name = "testbucket"
    test_filename = "testfile"
    full_filename = "/%s/%s" % (bucket_name, test_filename)

    with cloudstorage.open(full_filename, mode="w") as f:
      with records.RecordsWriter(f) as w:
        for (k, v) in input_data:
          proto = file_service_pb.KeyValue()
          proto.set_key(k)
          proto.set_value(v)
          w.write(proto.Encode())

    p = shuffler._HashPipeline("testjob", bucket_name,
                               [full_filename, full_filename, full_filename])
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    p = shuffler._HashPipeline.from_id(p.pipeline_id)

    list_of_output_files = p.outputs.default.value
    output_data = []
    for output_files in list_of_output_files:
      for output_file in output_files:
        with cloudstorage.open(output_file) as f:
          for binary_record in records.RecordsReader(f):
            proto = file_service_pb.KeyValue()
            proto.ParseFromString(binary_record)
            output_data.append((proto.key(), proto.value()))

    output_data.sort()
    self.assertEquals(300, len(output_data))
    for i in range(len(input_data)):
      self.assertEquals(input_data[i], output_data[(3 * i)])
      self.assertEquals(input_data[i], output_data[(3 * i) + 1])
      self.assertEquals(input_data[i], output_data[(3 * i) + 2])
    self.assertEquals(1, len(self.emails))


class SortFileEndToEndTest(testutil.HandlerTestBase):
  """End-to-end test for _SortFilePipeline."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  # pylint: disable=invalid-name
  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testSortFile(self):
    """Test sorting a file."""
    bucket_name = "testbucket"
    test_filename = "testfile"
    full_filename = "/%s/%s" % (bucket_name, test_filename)

    input_data = [
        (str(i), "_" + str(i)) for i in range(100)]

    with cloudstorage.open(full_filename, mode="w") as f:
      with records.RecordsWriter(f) as w:
        for (k, v) in input_data:
          proto = file_service_pb.KeyValue()
          proto.set_key(k)
          proto.set_value(v)
          w.write(proto.Encode())

    p = shuffler._SortChunksPipeline("testjob", bucket_name, [[full_filename]])
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    p = shuffler._SortChunksPipeline.from_id(p.pipeline_id)

    input_data.sort()
    output_files = p.outputs.default.value[0]
    output_data = []
    for output_file in output_files:
      with cloudstorage.open(output_file) as f:
        for binary_record in records.RecordsReader(f):
          proto = file_service_pb.KeyValue()
          proto.ParseFromString(binary_record)
          output_data.append((proto.key(), proto.value()))

    self.assertEquals(input_data, output_data)
    self.assertEquals(1, len(self.emails))


# pylint: disable=invalid-name
def test_handler_yield_str(key, value, partial):
  """Test handler that yields parameters converted to string."""
  yield str((key, value, partial))


class TestMergePipeline(base_handler.PipelineBase):
  """A pipeline to merge-sort multiple sorted files.

  Args:
    bucket_name: The name of the Google Cloud Storage bucket.
    filenames: list of input file names as string. Each file is of records
    format with file_service_pb.KeyValue protocol messages. All files should
    be sorted by key value.

  Returns:
    The list of filenames as string. Resulting files contain records with
    str((key, values)) obtained from MergingReader.
  """

  def run(self, bucket_name, filenames):
    yield mapreduce_pipeline.MapperPipeline(
        "sort",
        __name__ + ".test_handler_yield_str",
        shuffler.__name__ + "._MergingReader",
        output_writers.__name__ + "._GoogleCloudStorageRecordOutputWriter",
        params={
            shuffler._MergingReader.FILES_PARAM: [filenames],
            shuffler._MergingReader.MAX_VALUES_COUNT_PARAM:
                shuffler._MergePipeline._MAX_VALUES_COUNT,
            shuffler._MergingReader.MAX_VALUES_SIZE_PARAM:
                shuffler._MergePipeline._MAX_VALUES_SIZE,
            "output_writer": {
                "bucket_name": bucket_name,
            },
        },
        )


class MergingReaderEndToEndTest(testutil.HandlerTestBase):
  """End-to-end test for MergingReader."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  # pylint: disable=invalid-name
  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testMergeFiles(self):
    """Test merging multiple files."""
    input_data = [(str(i), "_" + str(i)) for i in range(100)]
    input_data.sort()

    bucket_name = "testbucket"
    test_filename = "testfile"
    full_filename = "/%s/%s" % (bucket_name, test_filename)

    with cloudstorage.open(full_filename, mode="w") as f:
      with records.RecordsWriter(f) as w:
        for (k, v) in input_data:
          proto = file_service_pb.KeyValue()
          proto.set_key(k)
          proto.set_value(v)
          w.write(proto.Encode())

    p = TestMergePipeline(bucket_name,
                          [full_filename, full_filename, full_filename])
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    p = TestMergePipeline.from_id(p.pipeline_id)

    output_file = p.outputs.default.value[0]
    output_data = []
    with cloudstorage.open(output_file) as f:
      for record in records.RecordsReader(f):
        output_data.append(record)

    expected_data = [
        str((k, [v, v, v], False)) for (k, v) in input_data]
    self.assertEquals(expected_data, output_data)
    self.assertEquals(1, len(self.emails))

  def testPartialRecords(self):
    """Test merging into partial key values."""
    try:
      self._prev_max_values_count = shuffler._MergePipeline._MAX_VALUES_COUNT
      # force max values count to extremely low value.
      shuffler._MergePipeline._MAX_VALUES_COUNT = 1

      input_data = [("1", "a"), ("2", "b"), ("3", "c")]
      input_data.sort()

      bucket_name = "testbucket"
      test_filename = "testfile"
      full_filename = "/%s/%s" % (bucket_name, test_filename)

      with cloudstorage.open(full_filename, mode="w") as f:
        with records.RecordsWriter(f) as w:
          for (k, v) in input_data:
            proto = file_service_pb.KeyValue()
            proto.set_key(k)
            proto.set_value(v)
            w.write(proto.Encode())

      p = TestMergePipeline(bucket_name,
                            [full_filename, full_filename, full_filename])
      p.start()
      test_support.execute_until_empty(self.taskqueue)
      p = TestMergePipeline.from_id(p.pipeline_id)

      output_file = p.outputs.default.value[0]
      output_data = []
      with cloudstorage.open(output_file) as f:
        for record in records.RecordsReader(f):
          output_data.append(record)

      expected_data = [
          ("1", ["a"], True),
          ("1", ["a"], True),
          ("1", ["a"], False),
          ("2", ["b"], True),
          ("2", ["b"], True),
          ("2", ["b"], False),
          ("3", ["c"], True),
          ("3", ["c"], True),
          ("3", ["c"], False),
          ]
      self.assertEquals([str(e) for e in expected_data], output_data)
    finally:
      shuffler._MergePipeline._MAX_VALUES_COUNT = self._prev_max_values_count
    self.assertEquals(1, len(self.emails))


class ShuffleEndToEndTest(testutil.HandlerTestBase):
  """End-to-end test for ShufflePipeline."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  # pylint: disable=invalid-name
  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testShuffleNoData(self):
    bucket_name = "testbucket"
    test_filename = "testfile"
    full_filename = "/%s/%s" % (bucket_name, test_filename)

    gcs_file = cloudstorage.open(full_filename, mode="w")
    gcs_file.close()

    p = shuffler.ShufflePipeline("testjob", {"bucket_name": bucket_name},
                                 [full_filename, full_filename, full_filename])
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    p = shuffler.ShufflePipeline.from_id(p.pipeline_id)
    for filename in p.outputs.default.value:
      self.assertEqual(0, cloudstorage.stat(filename).st_size)
    self.assertEquals(1, len(self.emails))

  def testShuffleNoFile(self):
    bucket_name = "testbucket"
    p = shuffler.ShufflePipeline("testjob", {"bucket_name": bucket_name}, [])
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    p = shuffler.ShufflePipeline.from_id(p.pipeline_id)
    for filename in p.outputs.default.value:
      self.assertEqual(0, cloudstorage.stat(filename).st_size)
    self.assertEquals(1, len(self.emails))

  def testShuffleFiles(self):
    """Test shuffling multiple files."""
    input_data = [(str(i), str(i)) for i in range(100)]
    input_data.sort()

    bucket_name = "testbucket"
    test_filename = "testfile"
    full_filename = "/%s/%s" % (bucket_name, test_filename)

    with cloudstorage.open(full_filename, mode="w") as f:
      with records.RecordsWriter(f) as w:
        for (k, v) in input_data:
          proto = file_service_pb.KeyValue()
          proto.set_key(k)
          proto.set_value(v)
          w.write(proto.Encode())

    p = shuffler.ShufflePipeline("testjob", {"bucket_name": bucket_name},
                                 [full_filename, full_filename, full_filename])
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    p = shuffler.ShufflePipeline.from_id(p.pipeline_id)

    output_files = p.outputs.default.value
    output_data = []
    for output_file in output_files:
      with cloudstorage.open(output_file) as f:
        for record in records.RecordsReader(f):
          proto = file_service_pb.KeyValues()
          proto.ParseFromString(record)
          output_data.append((proto.key(), proto.value_list()))
    output_data.sort()

    expected_data = sorted([
        (str(k), [str(v), str(v), str(v)]) for (k, v) in input_data])
    self.assertEquals(expected_data, output_data)
    self.assertEquals(1, len(self.emails))


if __name__ == "__main__":
  unittest.main()
