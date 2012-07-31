#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.




import re
import unittest


from mapreduce.lib import pipeline
from mapreduce.lib import files
from mapreduce.lib.files import file_service_pb
from mapreduce.lib.files import records
from google.appengine.ext import blobstore
from google.appengine.ext import db
from mapreduce import input_readers
from mapreduce import mapreduce_pipeline
from mapreduce import output_writers
from mapreduce import test_support
from testlib import testutil


class TestEntity(db.Model):
  """Test entity class."""
  data = db.TextProperty()


def test_handler_yield_key(entity):
  """Test handler that yields entity key."""
  yield entity.key()


def test_handler_yield_str(o):
  """Test handler that yields parameter converted to string."""
  yield str(o)


def test_map(entity):
  """Test map handler."""
  yield (entity.data, "")


def test_reduce(key, values):
  """Test reduce handler."""
  yield str((key, values))


class MapreducePipelineTest(testutil.HandlerTestBase):
  """Tests for MapreducePipeline."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testMapReduce(self):
    # Prepare test data
    entity_count = 200

    for i in range(entity_count):
      TestEntity(data=str(i)).put()
      TestEntity(data=str(i)).put()

    # Run Mapreduce
    p = mapreduce_pipeline.MapreducePipeline(
        "test",
        __name__ + ".test_map",
        __name__ + ".test_reduce",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=
            output_writers.__name__ + ".BlobstoreRecordsOutputWriter",
        mapper_params={
            "entity_kind": __name__ + "." + TestEntity.__name__,
            },
        shards=16)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    self.assertEquals(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith(
        "Pipeline successful:"))

    # Verify reduce output.
    p = mapreduce_pipeline.MapreducePipeline.from_id(p.pipeline_id)
    output_data = []
    for output_file in p.outputs.default.value:
      with files.open(output_file, "r") as f:
        for record in records.RecordsReader(f):
          output_data.append(record)

    expected_data = [
        str((str(d), ["", ""])) for d in range(entity_count)]
    expected_data.sort()
    output_data.sort()
    self.assertEquals(expected_data, output_data)

    # Verify that mapreduce doesn't leave intermediate files behind.
    blobInfos = blobstore.BlobInfo.all().fetch(limit=1000)
    for blobinfo in blobInfos:
      self.assertTrue(
          "Bad filename: %s" % blobinfo.filename,
          re.match("test-reduce-.*-output-\d+", blobinfo.filename))


class ReducerReaderTest(testutil.HandlerTestBase):
  """Tests for _ReducerReader."""

  def testMultipleRequests(self):
    """Tests restoring the reader state across multiple requests."""
    input_file = files.blobstore.create()

    # Create a file with two records.
    with files.open(input_file, "a") as f:
      with records.RecordsWriter(f) as w:
        proto = file_service_pb.KeyValues()
        proto.set_key("key2")
        proto.value_list().extend(["a", "b"])
        proto.set_partial(True)
        w.write(proto.Encode())

        proto = file_service_pb.KeyValues()
        proto.set_key("key2")
        proto.value_list().extend(["c", "d"])
        w.write(proto.Encode())

    files.finalize(input_file)
    input_file = files.blobstore.get_file_name(
        files.blobstore.get_blob_key(input_file))

    # Now read the records in two attempts, serializing and recreating the
    # input reader as if it's a separate request.
    reader = mapreduce_pipeline._ReducerReader([input_file], 0)
    it = iter(reader)
    self.assertEquals(input_readers.ALLOW_CHECKPOINT, it.next())

    reader_state = reader.to_json()
    other_reader = mapreduce_pipeline._ReducerReader.from_json(reader_state)
    it = iter(reader)
    self.assertEquals(("key2", ["a", "b", "c", "d"]), it.next())

  def testSingleRequest(self):
    """Tests when a key can be handled during a single request."""
    input_file = files.blobstore.create()

    with files.open(input_file, "a") as f:
      with records.RecordsWriter(f) as w:
        # First record is full
        proto = file_service_pb.KeyValues()
        proto.set_key("key1")
        proto.value_list().extend(["a", "b"])
        w.write(proto.Encode())
        # Second record is partial
        proto = file_service_pb.KeyValues()
        proto.set_key("key2")
        proto.value_list().extend(["a", "b"])
        proto.set_partial(True)
        w.write(proto.Encode())
        proto = file_service_pb.KeyValues()
        proto.set_key("key2")
        proto.value_list().extend(["c", "d"])
        w.write(proto.Encode())

    files.finalize(input_file)
    input_file = files.blobstore.get_file_name(
        files.blobstore.get_blob_key(input_file))

    reader = mapreduce_pipeline._ReducerReader([input_file], 0)
    self.assertEquals(
        [("key1", ["a", "b"]),
         input_readers.ALLOW_CHECKPOINT,
         ("key2", ["a", "b", "c", "d"])],
        list(reader))

    # now test state serialization
    reader = mapreduce_pipeline._ReducerReader([input_file], 0)
    i = reader.__iter__()
    self.assertEquals(
        {"position": 0,
         "current_values": "Ti4=",
         "current_key": "Ti4=",
         "filenames": [input_file]},
        reader.to_json())

    self.assertEquals(("key1", ["a", "b"]), i.next())
    self.assertEquals(
        {"position": 19,
         "current_values": "Ti4=",
         "current_key": "Ti4=",
         "filenames": [input_file]},
        reader.to_json())

    self.assertEquals(input_readers.ALLOW_CHECKPOINT, i.next())
    self.assertEquals(
        {"position": 40,
         "current_values": "KGxwMApTJ2EnCnAxCmFTJ2InCnAyCmEu",
         "current_key": "UydrZXkyJwpwMAou",
         "filenames": [input_file]},
        reader.to_json())

    self.assertEquals(("key2", ["a", "b", "c", "d"]), i.next())
    self.assertEquals(
        {"position": 59,
         "current_values": "Ti4=",
         "current_key": "Ti4=",
         "filenames": [input_file]},
        reader.to_json())

    try:
      i.next()
      self.fail("Exception expected")
    except StopIteration:
      # expected
      pass

    # now do test deserialization at every moment.
    reader = mapreduce_pipeline._ReducerReader([input_file], 0)
    i = reader.__iter__()
    reader = mapreduce_pipeline._ReducerReader.from_json(reader.to_json())

    self.assertEquals(("key1", ["a", "b"]), i.next())
    reader = mapreduce_pipeline._ReducerReader.from_json(reader.to_json())

    self.assertEquals(input_readers.ALLOW_CHECKPOINT, i.next())
    reader = mapreduce_pipeline._ReducerReader.from_json(reader.to_json())

    self.assertEquals(("key2", ["a", "b", "c", "d"]), i.next())
    reader = mapreduce_pipeline._ReducerReader.from_json(reader.to_json())

    try:
      i.next()
      self.fail("Exception expected")
    except StopIteration:
      # expected
      pass


if __name__ == "__main__":
  unittest.main()
