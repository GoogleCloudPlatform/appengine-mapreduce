#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
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
#

"""Tests for mapreduce.input_readers."""



from google.appengine.tools import os_compat

import cStringIO
from testlib import mox
import os
import zipfile

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_file_stub
from mapreduce.lib import blobstore
from google.appengine.ext import db
from mapreduce.lib import key_range
from mapreduce import input_readers
from mapreduce import model
import unittest


class TestJsonType(object):
  """Test class with to_json/from_json methods."""

  def __init__(self, size=0):
    self.size = size

  def to_json(self):
    return {"size": self.size}

  @classmethod
  def from_json(cls, json):
    return cls(json["size"])


class TestEntity(db.Model):
  """Test entity class."""

  json_property = model.JsonProperty(TestJsonType)
  json_property_default_value = model.JsonProperty(
      TestJsonType, default=TestJsonType())


ENTITY_KIND = "__main__.TestEntity"


class DatastoreInputReaderTest(unittest.TestCase):
  """Test model.DatastoreInputReader functionality."""

  def setUp(self):
    unittest.TestCase.setUp(self)
    self.appid = "testapp"
    os.environ["APPLICATION_ID"] = self.appid
    self.resetDatastore()

  def resetDatastore(self, require_indexes=False):
    self.datastore = datastore_file_stub.DatastoreFileStub(
        self.appid, "/dev/null", "/dev/null", require_indexes=require_indexes)

    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub("datastore_v3", self.datastore)

  def key(self, entity_id):
    """Create a key for TestEntity with specified id.

    Used to shorted expected data.

    Args:
      entity_id: entity id
    Returns:
      db.Key instance with specified id for TestEntity.
    """
    return db.Key.from_path("TestEntity", entity_id)

  def split(self, shard_count):
    """Generate TestEntity split.

    Args:
      shard_count: number of shards to split into as int.

    Returns:
      list of key_range.KeyRange (not DatastoreInputReader for easier testing).
    """
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        {"entity_kind": ENTITY_KIND},
        shard_count)
    ds_input_readers = input_readers.DatastoreInputReader.split_input(
        mapper_spec)
    return [input_reader._key_range for input_reader in ds_input_readers]

  def testSplitNoData(self):
    """Empty split should be produced if there's no data in database."""
    self.assertEquals([], self.split(10))

  def testSplitNotEnoughData(self):
    """Splits should not intersect, if there's not enough data for each."""
    TestEntity().put()
    TestEntity().put()
    self.assertEqual([

        key_range.KeyRange(key_start=self.key(1),
                           key_end=self.key(1),
                           direction="DESC",
                           include_start=True,
                           include_end=True),

        key_range.KeyRange(key_start=self.key(1),
                           key_end=self.key(1),
                           direction="ASC",
                           include_start=False,
                           include_end=False),

        key_range.KeyRange(key_start=self.key(1),
                           key_end=self.key(1),
                           direction="DESC",
                           include_start=False,
                           include_end=False),

        key_range.KeyRange(key_start=self.key(1),
                           key_end=self.key(2),
                           direction="ASC",
                           include_start=False,
                           include_end=True),
        ],
        self.split(4))

  def testSplitLotsOfData(self):
    """Test lots of data case."""

    for _ in range(0, 100):
      TestEntity().put()

    self.assertEqual([

        key_range.KeyRange(key_start=self.key(1),
                           key_end=self.key(25),
                           direction="DESC",
                           include_start=True,
                           include_end=True),

        key_range.KeyRange(key_start=self.key(25),
                           key_end=self.key(50),
                           direction="ASC",
                           include_start=False,
                           include_end=True),

        key_range.KeyRange(key_start=self.key(50),
                           key_end=self.key(75),
                           direction="DESC",
                           include_start=False,
                           include_end=True),

        key_range.KeyRange(key_start=self.key(75),
                           key_end=self.key(100),
                           direction="ASC",
                           include_start=False,
                           include_end=True),
        ],
        self.split(4))

  def testSplitGuessEnd(self):
    """Tests doing query splits when there is no descending index."""
    self.resetDatastore(require_indexes=True)
    TestEntity().put()
    TestEntity().put()
    self.assertEqual(
        [key_range.KeyRange(
             key_start=self.key(1),
             key_end=self.key(2),
             direction="ASC",
             include_start=True,
             include_end=True)
        ], self.split(1))

  def testGenerator(self):
    """Test DatastoreInputReader as generator."""
    for _ in range(0, 100):
      TestEntity().put()

    krange = key_range.KeyRange(key_start=self.key(25), key_end=self.key(50),
                                direction="ASC",
                                include_start=False, include_end=True)
    query_range = input_readers.DatastoreInputReader(ENTITY_KIND, krange, 50)

    entities = []

    for entity in query_range:
      entities.append(entity)
      self.assertEquals(
          key_range.KeyRange(key_start=entity.key(), key_end=self.key(50),
                             direction="ASC",
                             include_start=False, include_end=True),
          query_range._key_range)

    self.assertEquals(25, len(entities))

  def testShardDescription(self):
    """Tests the human-visible description of Datastore readers."""
    TestEntity().put()
    TestEntity().put()
    splits = self.split(2)
    stringified = [str(s) for s in splits]
    self.assertEquals(
        ["DESC[datastore_types.Key.from_path(u'TestEntity', 1, _app=u'testapp') to "
         "datastore_types.Key.from_path(u'TestEntity', 1, _app=u'testapp')]",
         "ASC(datastore_types.Key.from_path(u'TestEntity', 1, _app=u'testapp') to "
         "datastore_types.Key.from_path(u'TestEntity', 2, _app=u'testapp')]"],
        stringified)


class MockBlobInfo(object):
  def __init__(self, size):
    self.size = size


class InputReaderTest(unittest.TestCase):
  def setUp(self):
    unittest.TestCase.setUp(self)

    self.appid = "testapp"
    os.environ["APPLICATION_ID"] = self.appid
    self.mox = mox.Mox()

  def tearDown(self):
    self.mox.UnsetStubs()
    self.mox.ResetAll()

  def initMockedBlobstoreLineReader(self,
                                    initial_position,
                                    num_blocks_read,
                                    eof_read,
                                    end_offset,
                                    buffer_size,
                                    data):
    input_readers.BlobstoreLineInputReader._BLOB_BUFFER_SIZE = buffer_size
    blob_key_str = "foo"
    self.mox.StubOutWithMock(blobstore, "fetch_data")
    for block_index in range(num_blocks_read):
      block_start = initial_position + buffer_size * block_index
      block_end = initial_position + buffer_size * (block_index + 1)
      data_slice = data[block_start:block_end]
      blobstore.fetch_data(blob_key_str,
                           block_start,
                           block_end
                          ).AndReturn(data_slice)
    if eof_read:
      blobstore.fetch_data(blob_key_str, len(data),
                           len(data) + buffer_size).AndReturn('')
    self.mox.ReplayAll()
    r = input_readers.BlobstoreLineInputReader(blob_key_str,
                                               initial_position,
                                               initial_position + end_offset)
    return r

  def assertNextEquals(self, reader, expected_k, expected_v):
    k, v = reader.next()
    self.assertEquals(expected_k, k)
    self.assertEquals(expected_v, v)

  def assertDone(self, reader):
    self.assertRaises(StopIteration, reader.next)

  def testAtStart(self):
    """If we start at position 0, read the first record."""
    blob_reader = self.initMockedBlobstoreLineReader(
        0, 1, True, 100, 100, "foo\nbar")
    self.assertNextEquals(blob_reader, 0, "foo")
    self.assertNextEquals(blob_reader, len("foo\n"), "bar")
    self.mox.VerifyAll()

  def testOmitFirst(self):
    """If we start in the middle of a record, start with the next record."""
    blob_reader = self.initMockedBlobstoreLineReader(
        1, 1, True, 100, 100, "foo\nbar")
    self.assertNextEquals(blob_reader, len("foo\n"), "bar")
    self.mox.VerifyAll()

  def testOmitNewline(self):
    """If we start on a newline, start with the record on the next byte."""
    blob_reader = self.initMockedBlobstoreLineReader(
        3, 1, True, 100, 100, "foo\nbar")
    self.assertNextEquals(blob_reader, len("foo\n"), "bar")
    self.mox.VerifyAll()

  def testSpanBlocks(self):
    """Test the multi block case."""
    blob_reader = self.initMockedBlobstoreLineReader(
        0, 4, True, 100, 2, "foo\nbar")
    self.assertNextEquals(blob_reader, 0, "foo")
    self.assertNextEquals(blob_reader, len("foo\n"), "bar")
    self.mox.VerifyAll()

  def testStopAtEnd(self):
    """If we pass end position, then we don't get a record past the end."""
    blob_reader = self.initMockedBlobstoreLineReader(
        0, 1, False, 1, 100, "foo\nbar")
    self.assertNextEquals(blob_reader, 0, "foo")
    self.assertDone(blob_reader)
    self.mox.VerifyAll()

  def testDontReturnAnythingIfPassEndBeforeFirst(self):
    """Test end behavior.

    If we pass the end position when reading to the first record,
    then we don't get a record past the end.
    """
    blob_reader = self.initMockedBlobstoreLineReader(
        3, 1, False, 0, 100, "foo\nbar")
    self.assertDone(blob_reader)
    self.mox.VerifyAll()

  def mockOutBlobInfoSize(self, size):
    blob_key_str = "foo"
    self.mox.StubOutWithMock(blobstore, "BlobKey", use_mock_anything=True)
    blob_key = "bar"
    blobstore.BlobKey(blob_key_str).AndReturn(blob_key)
    self.mox.StubOutWithMock(blobstore.BlobInfo, "get", use_mock_anything=True)
    blobstore.BlobInfo.get("bar").AndReturn(MockBlobInfo(size))

  BLOBSTORE_READER_NAME = (
      'mapreduce.input_readers.BlobstoreLineInputReader')

  def testSplitInput(self):
    """Test Google3CSVFileInputReader.split_input."""
    self.mockOutBlobInfoSize(200)
    self.mox.ReplayAll()
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": self.BLOBSTORE_READER_NAME,
        "mapper_params": {"blob_keys": ["foo"]},
        "mapper_shard_count": 1})
    blob_readers = input_readers.BlobstoreLineInputReader.split_input(
        mapper_spec)
    self.assertEquals([{"blob_key": "foo",
                        "initial_position": 0,
                        "end_position": 200}],
                      [r.to_json() for r in blob_readers])
    self.mox.VerifyAll()

  def testSplitInputMultiSplit(self):
    """Test Google3CSVFileInputReader.split_input."""
    self.mockOutBlobInfoSize(199)
    self.mox.ReplayAll()
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": self.BLOBSTORE_READER_NAME,
        "mapper_params": {"blob_keys": ["foo"]},
        "mapper_shard_count": 2})
    blob_readers = input_readers.BlobstoreLineInputReader.split_input(
        mapper_spec)
    self.assertEquals(
        [{"blob_key": "foo",
          "initial_position": 0,
          "end_position": 99},
         {"blob_key": "foo",
          "initial_position": 99,
          "end_position": 199}],
        [r.to_json() for r in blob_readers])
    self.mox.VerifyAll()

  def testShardDescription(self):
    """Tests the human-readable shard description."""
    self.mockOutBlobInfoSize(199)
    self.mox.ReplayAll()
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": self.BLOBSTORE_READER_NAME,
        "mapper_params": {"blob_keys": ["foo"]},
        "mapper_shard_count": 2})
    blob_readers = input_readers.BlobstoreLineInputReader.split_input(
        mapper_spec)
    stringified = [str(s) for s in blob_readers]
    self.assertEquals(
        ["blobstore.BlobKey('foo'):[0, 99]",
         "blobstore.BlobKey('foo'):[99, 199]"],
         stringified)
    self.mox.VerifyAll()

  def testTooManyKeys(self):
    """Tests when there are too many blobkeys present as input."""
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": self.BLOBSTORE_READER_NAME,
        "mapper_params": {"blob_keys": ["foo"] * 1000},
        "mapper_shard_count": 2})
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.BlobstoreLineInputReader.split_input,
                      mapper_spec)

  def testNoKeys(self):
    """Tests when there are no blobkeys present as input."""
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": self.BLOBSTORE_READER_NAME,
        "mapper_params": {"blob_keys": []},
        "mapper_shard_count": 2})
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.BlobstoreLineInputReader.split_input,
                      mapper_spec)


class BlobstoreZipInputReaderTest(unittest.TestCase):
  READER_NAME = (
      'mapreduce.input_readers.BlobstoreZipInputReader')

  def setUp(self):
    unittest.TestCase.setUp(self)

    self.appid = "testapp"
    os.environ["APPLICATION_ID"] = self.appid

    self.zipdata = cStringIO.StringIO()
    zip = zipfile.ZipFile(self.zipdata, "w")
    for i in range(10):
      zip.writestr("%d.txt" % i, "%d: %s" % (i, "*"*i))
    zip.close()

  def mockZipReader(self, blob_key):
    """Mocked out reader function that returns our in-memory zipfile."""
    return self.zipdata

  def testReadFirst(self):
    """Test that the first file in the zip is returned correctly."""
    reader = input_readers.BlobstoreZipInputReader("", 0, 1, self.mockZipReader)
    file_info, data_func = reader.next()
    self.assertEqual(file_info.filename, "0.txt")
    self.assertEqual(data_func(), "0: ")

  def testReadLast(self):
    """Test we can read right up to the last file in the zip."""
    reader = input_readers.BlobstoreZipInputReader("", 9, 10,
                                                   self.mockZipReader)
    file_info, data_func = reader.next()
    self.assertEqual(file_info.filename, "9.txt")
    self.assertEqual(data_func(), "9: *********")

  def testStopIteration(self):
    """Test that StopIteration is raised when we fetch past the end."""
    reader = input_readers.BlobstoreZipInputReader("", 0, 1, self.mockZipReader)
    reader.next()
    self.assertRaises(StopIteration, reader.next)

  def testSplitInput(self):
    """Test that split_input functions as expected."""
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": self.READER_NAME,
        "mapper_params": {"blob_key": ["foo"]},
        "mapper_shard_count": 2})
    readers = input_readers.BlobstoreZipInputReader.split_input(
        mapper_spec, self.mockZipReader)
    self.assertEqual(len(readers), 2)
    self.assertEqual(str(readers[0]), "blobstore.BlobKey(['foo']):[0, 7]")
    self.assertEqual(str(readers[1]), "blobstore.BlobKey(['foo']):[7, 10]")


if __name__ == "__main__":
  unittest.main()
