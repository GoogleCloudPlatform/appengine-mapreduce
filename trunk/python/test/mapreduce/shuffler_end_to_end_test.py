#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.





import unittest

from mapreduce.lib import pipeline
from google.appengine.api import files
from google.appengine.api.files import file_service_pb
from google.appengine.api.files import records
from google.appengine.ext import db
from mapreduce import base_handler
from mapreduce import control
from mapreduce import mapreduce_pipeline
from mapreduce import model
from mapreduce import output_writers
from mapreduce import shuffler
from mapreduce import test_support
from testlib import testutil
import unittest


class SortFileEndToEndTest(testutil.HandlerTestBase):
  """End-to-end test for _SortFilePipeline."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testSortFile(self):
    """Test sorting a file."""
    input_file = files.blobstore.create()

    input_data = [
        (str(i), "_" + str(i)) for i in range(100)]

    with files.open(input_file, "a") as f:
      with records.RecordsWriter(f) as w:
        for (k, v) in input_data:
          proto = file_service_pb.KeyValue()
          proto.set_key(k)
          proto.set_value(v)
          w.write(proto.Encode())
    files.finalize(input_file)
    input_file = files.blobstore.get_file_name(
        files.blobstore.get_blob_key(input_file))

    p = shuffler._SortChunksPipeline("testjob", [input_file])
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    p = shuffler._SortChunksPipeline.from_id(p.pipeline_id)

    input_data.sort()
    output_files = p.outputs.default.value[0]
    output_data = []
    for output_file in output_files:
      with files.open(output_file, "r") as f:
        for binary_record in records.RecordsReader(f):
          proto = file_service_pb.KeyValue()
          proto.ParseFromString(binary_record)
          output_data.append((proto.key(), proto.value()))

    self.assertEquals(input_data, output_data)


def test_handler_yield_str(key, value, partial):
  """Test handler that yields parameters converted to string."""
  yield str((key, value, partial))


class TestMergePipeline(base_handler.PipelineBase):
  """A pipeline to merge-sort multiple sorted files.

  Args:
    filenames: list of input file names as string. Each file is of records
    format with file_service_pb.KeyValue protocol messages. All files should
    be sorted by key value.

  Returns:
    The list of filenames as string. Resulting files contain records with
    str((key, values)) obtained from MergingReader.
  """

  def run(self, filenames):
    yield mapreduce_pipeline.MapperPipeline(
        "sort",
        __name__ + ".test_handler_yield_str",
        shuffler.__name__ + "._MergingReader",
        output_writers.__name__ + ".BlobstoreRecordsOutputWriter",
        params={
          shuffler._MergingReader.FILES_PARAM:[filenames],
          shuffler._MergingReader.MAX_VALUES_COUNT_PARAM:
              shuffler._MergePipeline._MAX_VALUES_COUNT,
          shuffler._MergingReader.MAX_VALUES_SIZE_PARAM:
              shuffler._MergePipeline._MAX_VALUES_SIZE,
          },
        )


class MergingReaderEndToEndTest(testutil.HandlerTestBase):
  """End-to-end test for MergingReader."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testMergeFiles(self):
    """Test merging multiple files."""
    input_data = [(str(i), "_" + str(i)) for i in range(100)]
    input_data.sort()

    input_file = files.blobstore.create()

    with files.open(input_file, "a") as f:
      with records.RecordsWriter(f) as w:
        for (k, v) in input_data:
          proto = file_service_pb.KeyValue()
          proto.set_key(k)
          proto.set_value(v)
          w.write(proto.Encode())
    files.finalize(input_file)
    input_file = files.blobstore.get_file_name(
        files.blobstore.get_blob_key(input_file))

    p = TestMergePipeline([input_file, input_file, input_file])
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    p = TestMergePipeline.from_id(p.pipeline_id)

    output_file = p.outputs.default.value[0]
    output_data = []
    with files.open(output_file, "r") as f:
      for record in records.RecordsReader(f):
        output_data.append(record)

    expected_data = [
        str((k, [v, v, v], False)) for (k, v) in input_data]
    self.assertEquals(expected_data, output_data)

  def testPartialRecords(self):
    """Test merging into partial key values."""
    try:
      self._prev_max_values_count = shuffler._MergePipeline._MAX_VALUES_COUNT
      # force max values count to extremely low value.
      shuffler._MergePipeline._MAX_VALUES_COUNT = 1

      input_data = [('1', 'a'), ('2', 'b'), ('3', 'c')]
      input_data.sort()

      input_file = files.blobstore.create()

      with files.open(input_file, "a") as f:
        with records.RecordsWriter(f) as w:
          for (k, v) in input_data:
            proto = file_service_pb.KeyValue()
            proto.set_key(k)
            proto.set_value(v)
            w.write(proto.Encode())
      files.finalize(input_file)
      input_file = files.blobstore.get_file_name(
          files.blobstore.get_blob_key(input_file))

      p = TestMergePipeline([input_file, input_file, input_file])
      p.start()
      test_support.execute_until_empty(self.taskqueue)
      p = TestMergePipeline.from_id(p.pipeline_id)

      output_file = p.outputs.default.value[0]
      output_data = []
      with files.open(output_file, "r") as f:
        for record in records.RecordsReader(f):
          output_data.append(record)

      expected_data = [
          ('1', ['a'], True),
          ('1', ['a'], True),
          ('1', ['a'], False),
          ('2', ['b'], True),
          ('2', ['b'], True),
          ('2', ['b'], False),
          ('3', ['c'], True),
          ('3', ['c'], True),
          ('3', ['c'], False),
          ]
      self.assertEquals([str(e) for e in expected_data], output_data)
    finally:
      shuffler._MergePipeline._MAX_VALUES_COUNT = self._prev_max_values_count


class ShuffleEndToEndTest(testutil.HandlerTestBase):
  """End-to-end test for ShufflePipeline."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testShuffleNoData(self):
    input_file = files.blobstore.create()
    files.finalize(input_file)
    input_file = files.blobstore.get_file_name(
        files.blobstore.get_blob_key(input_file))

    p = shuffler.ShufflePipeline(
        "testjob", [input_file, input_file, input_file])
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    p = shuffler.ShufflePipeline.from_id(p.pipeline_id)
    for filename in p.outputs.default.value:
      self.assertEqual(0, files.stat(filename).st_size)

  def testShuffleNoFile(self):
    p = shuffler.ShufflePipeline(
        "testjob", [])
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    p = shuffler.ShufflePipeline.from_id(p.pipeline_id)
    for filename in p.outputs.default.value:
      self.assertEqual(0, files.stat(filename).st_size)

  def testShuffleFiles(self):
    """Test shuffling multiple files."""
    input_data = [(str(i), str(i)) for i in range(100)]
    input_data.sort()

    input_file = files.blobstore.create()

    with files.open(input_file, "a") as f:
      with records.RecordsWriter(f) as w:
        for (k, v) in input_data:
          proto = file_service_pb.KeyValue()
          proto.set_key(k)
          proto.set_value(v)
          w.write(proto.Encode())
    files.finalize(input_file)
    input_file = files.blobstore.get_file_name(
        files.blobstore.get_blob_key(input_file))

    p = shuffler.ShufflePipeline(
        "testjob", [input_file, input_file, input_file])
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    p = shuffler.ShufflePipeline.from_id(p.pipeline_id)

    output_files = p.outputs.default.value
    output_data = []
    for output_file in output_files:
      with files.open(output_file, "r") as f:
        for record in records.RecordsReader(f):
          proto = file_service_pb.KeyValues()
          proto.ParseFromString(record)
          output_data.append((proto.key(), proto.value_list()))
    output_data.sort()

    expected_data = sorted([
        (str(k), [str(v), str(v), str(v)]) for (k, v) in input_data])
    self.assertEquals(expected_data, output_data)


if __name__ == "__main__":
  unittest.main()
