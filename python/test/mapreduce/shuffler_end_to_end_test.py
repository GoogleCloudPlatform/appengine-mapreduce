#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.





import unittest

from mapreduce.lib import pipeline
from mapreduce.lib import files
from mapreduce.lib.files import file_service_pb
from mapreduce.lib.files import records
from google.appengine.ext import db
from mapreduce import base_handler
from mapreduce import control
from mapreduce import mapreduce_pipeline
from mapreduce import model
from mapreduce import output_writers
from mapreduce import shuffler
from testlib import testutil
import unittest


KEY_VALUE_WRITER_NAME = (shuffler.__name__ + "." +
                         shuffler._KeyValueBlobstoreOutputWriter.__name__)


class TestEntity(db.Model):
  """Test entity class."""


def test_handler_yield_key_value(entity):
  """Test handler which yields key value pair."""
  yield (entity.key(), entity)


class KeyValueOutputWriterEndToEndTest(testutil.HandlerTestBase):
  """End-to-end test for KeyValueBlobstoreOutputWriter."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testKeyValueOutputWriter(self):
    entity_count = 1000

    for _ in range(entity_count):
      TestEntity().put()

    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".test_handler_yield_key_value",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
        },
        shard_count=4,
        base_path="/mapreduce_base_path",
        output_writer_spec=KEY_VALUE_WRITER_NAME)

    testutil.execute_until_empty(self.taskqueue)

    mapreduce_state = model.MapreduceState.get_by_job_id(mapreduce_id)
    filenames = shuffler._KeyValueBlobstoreOutputWriter.get_filenames(
        mapreduce_state)
    self.assertEqual(4, len(filenames))

    file_lengths = []
    for shard_files in filenames:
      for filename in shard_files:
        self.assertTrue(filename.startswith("/blobstore/"))
        self.assertFalse(filename.startswith("/blobstore/writable:"))

        with files.open(filename, "r") as f:
          reader = records.RecordsReader(f)
          count = 0
          try:
            while True:
              reader.read()
              count += 1
          except EOFError:
            pass
          file_lengths.append(count)

    # these numbers are totally random and depend on our sharding,
    # which is quite deterministic.
    expected_lengths = [
        48, 54, 48, 49,
        58, 47, 51, 54,
        60, 74, 71, 70,
        84, 77, 76, 79,
        ]
    self.assertEqual(1000, sum(expected_lengths))
    self.assertEquals(expected_lengths, file_lengths)


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

    p = shuffler.SortPipeline([input_file])
    p.start()
    testutil.execute_until_empty(self.taskqueue)
    p = shuffler.SortPipeline.from_id(p.pipeline_id)

    input_data.sort()
    output_file = p.outputs.default.value[0]
    output_data = []
    with files.open(output_file, "r") as f:
      for binary_record in records.RecordsReader(f):
        proto = file_service_pb.KeyValue()
        proto.ParseFromString(binary_record)
        output_data.append((proto.key(), proto.value()))

    self.assertEquals(input_data, output_data)


def test_handler_yield_str(key, value):
  """Test handler that yields parameters converted to string."""
  yield str((key, value))


class MergePipeline(base_handler.PipelineBase):
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
        {"files": [filenames]},
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
    input_data = [
        (str(i), "_" + str(i)) for i in range(100)]
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

    p = MergePipeline([input_file, input_file, input_file])
    p.start()
    testutil.execute_until_empty(self.taskqueue)
    p = MergePipeline.from_id(p.pipeline_id)

    output_file = p.outputs.default.value[0]
    output_data = []
    with files.open(output_file, "r") as f:
      for record in records.RecordsReader(f):
        output_data.append(record)

    expected_data = [
        str((k, [v, v, v])) for (k, v) in input_data]
    self.assertEquals(expected_data, output_data)


if __name__ == "__main__":
  unittest.main()
