#!/usr/bin/env python
#
# Copyright 2010 Google Inc.
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





# Disable "Invalid method name"
# pylint: disable-msg=C6409

# os_compat must be first to ensure timezones are UTC.

from google.appengine.tools import os_compat  # pylint: disable-msg=W0611

import cStringIO
import os
import unittest
import zipfile
from testlib import mox

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore
from google.appengine.api import datastore_file_stub
from mapreduce.lib import blobstore
from google.appengine.ext import db
from mapreduce.lib import key_range
from mapreduce.lib.blobstore import blobstore as blobstore_internal
from mapreduce import input_readers
from mapreduce import model


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


def key(entity_id):
  """Create a key for TestEntity with specified id.

  Used to shorten expected data.

  Args:
    entity_id: entity id
  Returns:
    db.Key instance with specified id for TestEntity.
  """
  return db.Key.from_path("TestEntity", entity_id)


class DatastoreInputReaderTest(unittest.TestCase):
  """Test Datastore{,Key,Entity}InputReader classes."""

  def setUp(self):
    unittest.TestCase.setUp(self)
    self.appid = "testapp"
    os.environ["APPLICATION_ID"] = self.appid
    os.environ["AUTH_DOMAIN"]= "gmail.com"
    self.resetDatastore()

  def resetDatastore(self, require_indexes=False):
    self.datastore = datastore_file_stub.DatastoreFileStub(
        self.appid, "/dev/null", "/dev/null", require_indexes=require_indexes)

    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub("datastore_v3", self.datastore)


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

  def testValidate_Passes(self):
    """Test validate function accepts valid parameters."""
    params = {
        "entity_kind": ENTITY_KIND,
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    input_readers.DatastoreInputReader.validate(mapper_spec)

  def testValidate_NoEntityFails(self):
    """Test validate function raises exception with no entity parameter."""
    params = {}
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.DatastoreInputReader.validate,
                      mapper_spec)

  def testValidate_BadClassFails(self):
    """Test validate function rejects non-matching class parameter."""
    params = {}
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.DatastoreKeyInputReader.validate,
                      mapper_spec)

  def testValidate_KeysOnly(self):
    """Test validate function rejects keys_only parameter."""
    # Setting keys_only to true is an error.
    params = {
        "entity_kind": ENTITY_KIND,
        "keys_only": "True"
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.DatastoreInputReader.validate,
                      mapper_spec)

  def testValidate_BadEntityKind(self):
    """Test validate function fails without entity kind."""
    # Setting keys_only to true is an error.
    params = {}
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.DatastoreInputReader.validate,
                      mapper_spec)


  def testValidate_BadEntityKind(self):
    """Test validate function rejects bad entity kind."""
    # Setting keys_only to true is an error.
    params = {
        "entity_kind": "foo",
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.DatastoreInputReader.validate,
                      mapper_spec)

  def testValidate_BadBatchSize(self):
    """Test validate function rejects bad entity kind."""
    # Setting keys_only to true is an error.
    params = {
        "entity_kind": ENTITY_KIND,
        "batch_size": "xxx"
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.DatastoreInputReader.validate,
                      mapper_spec)
    params = {
        "entity_kind": ENTITY_KIND,
        "batch_size": "0"
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.DatastoreInputReader.validate,
                      mapper_spec)
    params = {
        "entity_kind": ENTITY_KIND,
        "batch_size": "-1"
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.DatastoreInputReader.validate,
                      mapper_spec)

  def testParameters(self):
    """Test that setting string parameters as they would be passed from a
    web interface works.
    """
    for _ in range(0, 100):
      TestEntity().put()

    krange = key_range.KeyRange(key_start=key(25), key_end=key(50),
                                direction="ASC",
                                include_start=False, include_end=True)

    params = {}
    params["app"] = "blah"
    params["batch_size"] = "42"
    params["entity_kind"] = ENTITY_KIND
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    reader = input_readers.DatastoreInputReader.split_input(
        mapper_spec)
    self.assertEquals(input_readers.DatastoreInputReader, reader[0].__class__)
    self.assertEquals(42, reader[0]._batch_size)

    params["batch_size"] = "24"
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    reader = input_readers.DatastoreInputReader.split_input(
        mapper_spec)
    self.assertEquals(24, reader[0]._batch_size)

    # Setting keys_only to false is OK (it's ignored.)
    params["keys_only"] = "False"
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    reader = input_readers.DatastoreInputReader.split_input(
        mapper_spec)

    # But it's totally ignored on the DatastoreKeyInputReader.
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers."
        "DatastoreKeyInputReader",
        params, 1)
    reader = input_readers.DatastoreKeyInputReader.split_input(mapper_spec)

    del params["keys_only"]
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers."
        "DatastoreKeyInputReader",
        params, 1)
    reader = input_readers.DatastoreKeyInputReader.split_input(
        mapper_spec)
    self.assertEquals(input_readers.DatastoreKeyInputReader,
                      reader[0].__class__)

    # Coverage test: DatastoreEntityInputReader
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers."
        "DatastoreEntityInputReader",
        params, 1)
    reader = input_readers.DatastoreEntityInputReader.split_input(mapper_spec)
    self.assertEquals(input_readers.DatastoreEntityInputReader,
                      reader[0].__class__)

  def testSplitNoData(self):
    """Empty split should be produced if there's no data in database."""
    self.assertEquals([], self.split(10))

  def testSplitNotEnoughData(self):
    """Splits should not intersect, if there's not enough data for each."""
    TestEntity().put()
    TestEntity().put()
    self.assertEqual([
        # [1, 1]
        key_range.KeyRange(key_start=key(1),
                           key_end=key(1),
                           direction="DESC",
                           include_start=True,
                           include_end=True),
        # (1, 1)
        key_range.KeyRange(key_start=key(1),
                           key_end=key(1),
                           direction="ASC",
                           include_start=False,
                           include_end=False),
        # (1, 1)
        key_range.KeyRange(key_start=key(1),
                           key_end=key(1),
                           direction="DESC",
                           include_start=False,
                           include_end=False),
        # (1, 2]
        key_range.KeyRange(key_start=key(1),
                           key_end=key(2),
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
        # [1, 25]
        key_range.KeyRange(key_start=key(1),
                           key_end=key(25),
                           direction="DESC",
                           include_start=True,
                           include_end=True),
        # (25, 50]
        key_range.KeyRange(key_start=key(25),
                           key_end=key(50),
                           direction="ASC",
                           include_start=False,
                           include_end=True),
        # (50, 75]
        key_range.KeyRange(key_start=key(50),
                           key_end=key(75),
                           direction="DESC",
                           include_start=False,
                           include_end=True),
        # (75, 100]
        key_range.KeyRange(key_start=key(75),
                           key_end=key(100),
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
             key_start=key(1),
             key_end=key(2),
             direction="ASC",
             include_start=True,
             include_end=True)
        ], self.split(1))

  def testGenerator(self):
    """Test DatastoreInputReader as generator."""
    expected_entities = []
    for _ in range(0, 100):
      entity = TestEntity()
      entity.put()
      expected_entities.append(entity)

    krange = key_range.KeyRange(key_start=key(25), key_end=key(50),
                                direction="ASC",
                                include_start=False, include_end=True)
    query_range = input_readers.DatastoreInputReader(
        ENTITY_KIND, krange, 50)

    entities = []

    for entity in query_range:
      entities.append(entity)
      self.assertEquals(
          key_range.KeyRange(key_start=entity.key(), key_end=key(50),
                             direction="ASC",
                             include_start=False, include_end=True),
          query_range._key_range)

    self.assertEquals(25, len(entities))
    # Model instances are not comparable, so we'll compare a serialization.
    expected_values = [entity.to_xml() for entity in expected_entities[25:50]]
    actual_values = [entity.to_xml() for entity in entities]
    self.assertEquals(expected_values, actual_values)


  def testEntityGenerator(self):
    """Test DatastoreEntityInputReader."""
    expected_entities = []
    for _ in range(0, 100):
      model_instance = TestEntity()
      model_instance.put()
      expected_entities.append(model_instance._populate_internal_entity())

    krange = key_range.KeyRange(key_start=key(25), key_end=key(50),
                                direction="ASC",
                                include_start=False, include_end=True)
    query_range = input_readers.DatastoreEntityInputReader(
        ENTITY_KIND, krange, 50)

    entities = []
    for entity in query_range:
      entities.append(entity)
      self.assertEquals(
          key_range.KeyRange(key_start=entity.key(), key_end=key(50),
                             direction="ASC",
                             include_start=False, include_end=True),
          query_range._key_range)

    self.assertEquals(25, len(entities))
    self.assertEquals(expected_entities[25:50], entities)

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


class DatastoreKeyInputReaderTest(unittest.TestCase):
  """Tests for DatastoreKeyInputReader."""

  def testValidate_Passes(self):
    """Tests validation function even with invalid kind."""
    params = {
        "entity_kind": "InvalidKind",
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreKeyInputReader",
        params, 1)
    input_readers.DatastoreKeyInputReader.validate(mapper_spec)

  def testGenerator(self):
    """Test generator functionality."""
    expected_keys = []
    for _ in range(0, 100):
      expected_keys.append(TestEntity().put())

    krange = key_range.KeyRange(key_start=key(25), key_end=key(50),
                                direction="ASC",
                                include_start=False, include_end=True)
    query_range = input_readers.DatastoreKeyInputReader(
        ENTITY_KIND, krange, 50)

    keys = []

    for k in query_range:
      keys.append(k)
      self.assertEquals(
          key_range.KeyRange(key_start=k, key_end=key(50),
                             direction="ASC",
                             include_start=False, include_end=True),
          query_range._key_range)

    self.assertEquals(25, len(keys))
    self.assertEquals(expected_keys[25:50], keys)

  def tesGeneratorNoModelOtherApp(self):
    """Test DatastoreKeyInputReader when raw kind is given, not a Model path."""
    OTHER_KIND = "blahblah"
    OTHER_APP = "blah"
    apiproxy_stub_map.apiproxy.GetStub("datastore_v3").SetTrusted(True)

    expected_keys = []
    for _ in range(0, 100):
      expected_keys.append(datastore.Put(datastore.Entity(OTHER_KIND,
                                                          _app=OTHER_APP)))

    key_start = db.Key.from_path(OTHER_KIND, 25, _app=OTHER_APP)
    key_end = db.Key.from_path(OTHER_KIND, 50, _app=OTHER_APP)
    krange = key_range.KeyRange(key_start=key_start, key_end=key_end,
                                direction="ASC",
                                include_start=False, include_end=True,
                                _app=OTHER_APP)

    query_range = input_readers.DatastoreKeyInputReader(
        OTHER_KIND, krange,
        {"app": OTHER_APP, "batch_size": 50})

    keys = []

    for key in query_range:
      keys.append(key)
      self.assertEquals(
          key_range.KeyRange(key_start=key, key_end=key_end,
                             direction="ASC",
                             include_start=False, include_end=True),
          query_range._key_range)

    self.assertEquals(25, len(keys))
    self.assertEquals(expected_keys[25:50], keys)



class MockBlobInfo(object):
  def __init__(self, size):
    self.size = size


class BlobstoreLineInputReaderTest(unittest.TestCase):
  def setUp(self):
    unittest.TestCase.setUp(self)

    self.appid = "testapp"
    os.environ["APPLICATION_ID"] = self.appid
    self.mox = mox.Mox()
    self.original_fetch_data = blobstore.fetch_data

    self.mox.StubOutWithMock(blobstore, "BlobKey", use_mock_anything=True)
    self.mox.StubOutWithMock(blobstore.BlobInfo, "get", use_mock_anything=True)

  def tearDown(self):
    self.mox.UnsetStubs()
    self.mox.ResetAll()
    blobstore.fetch_data = self.original_fetch_data

  def initMockedBlobstoreLineReader(self,
                                    initial_position,
                                    num_blocks_read,
                                    eof_read,
                                    end_offset,
                                    buffer_size,
                                    data):
    input_readers.BlobstoreLineInputReader._BLOB_BUFFER_SIZE = buffer_size
    # Mock out blob key so as to avoid validation.
    blob_key_str = "foo"

    def fetch_data(blob_key, start, end):
      return data[start:end + 1]
    blobstore_internal.fetch_data = fetch_data

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
        0, 1, True, 100, 100, "foo\nbar\nfoobar")
    self.assertNextEquals(blob_reader, 0, "foo")
    self.assertNextEquals(blob_reader, len("foo\n"), "bar")
    self.assertNextEquals(blob_reader, len("foo\nbar\n"), "foobar")

  def testOmitFirst(self):
    """If we start in the middle of a record, start with the next record."""
    blob_reader = self.initMockedBlobstoreLineReader(
        1, 1, True, 100, 100, "foo\nbar\nfoobar")
    self.assertNextEquals(blob_reader, len("foo\n"), "bar")
    self.assertNextEquals(blob_reader, len("foo\nbar\n"), "foobar")

  def testOmitNewline(self):
    """If we start on a newline, start with the record on the next byte."""
    blob_reader = self.initMockedBlobstoreLineReader(
        3, 1, True, 100, 100, "foo\nbar")
    self.assertNextEquals(blob_reader, len("foo\n"), "bar")

  def testSpanBlocks(self):
    """Test the multi block case."""
    blob_reader = self.initMockedBlobstoreLineReader(
        0, 4, True, 100, 2, "foo\nbar")
    self.assertNextEquals(blob_reader, 0, "foo")
    self.assertNextEquals(blob_reader, len("foo\n"), "bar")

  def testStopAtEnd(self):
    """If we pass end position, then we don't get a record past the end."""
    blob_reader = self.initMockedBlobstoreLineReader(
        0, 1, False, 1, 100, "foo\nbar")
    self.assertNextEquals(blob_reader, 0, "foo")
    self.assertDone(blob_reader)

  def testDontReturnAnythingIfPassEndBeforeFirst(self):
    """Test end behavior.

    If we pass the end position when reading to the first record,
    then we don't get a record past the end.
    """
    blob_reader = self.initMockedBlobstoreLineReader(
        3, 1, False, 0, 100, "foo\nbar")
    self.assertDone(blob_reader)

  def mockOutBlobInfoSize(self, size, blob_key_str="foo"):
    blob_key = "bar" + blob_key_str
    blobstore.BlobKey(blob_key_str).AndReturn(blob_key)
    blobstore.BlobInfo.get(blob_key).AndReturn(MockBlobInfo(size))

  BLOBSTORE_READER_NAME = (
      "mapreduce.input_readers.BlobstoreLineInputReader")

  def testSplitInput(self):
    
    # TODO(user): Mock out equiv
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

  def testSplitInputMultiKey(self):
    
    # TODO(user): Mock out equiv
    for i in range(5):
      self.mockOutBlobInfoSize(200, "foo%d" % i)
    self.mox.ReplayAll()
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": self.BLOBSTORE_READER_NAME,
        "mapper_params": {"blob_keys": ["foo%d" % i for i in range(5)]},
        "mapper_shard_count": 2})
    blob_readers = input_readers.BlobstoreLineInputReader.split_input(
        mapper_spec)
    # Blob readers are built out of a dictionary of blob_keys and thus unsorted.
    blob_readers_json = [r.to_json() for r in blob_readers]
    blob_readers_json.sort(key=lambda r: r["blob_key"])
    self.assertEquals([{"blob_key": "foo%d" % i,
                        "initial_position": 0,
                        "end_position": 200} for i in range(5)],
                      blob_readers_json)
    self.mox.VerifyAll()

  def testSplitInputMultiSplit(self):
    
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
                      input_readers.BlobstoreLineInputReader.validate,
                      mapper_spec)

  def testNoKeys(self):
    """Tests when there are no blobkeys present as input."""
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": self.BLOBSTORE_READER_NAME,
        "mapper_params": {"blob_keys": []},
        "mapper_shard_count": 2})
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.BlobstoreLineInputReader.validate,
                      mapper_spec)


class BlobstoreZipInputReaderTest(unittest.TestCase):
  READER_NAME = (
      "mapreduce.input_readers.BlobstoreZipInputReader")

  def setUp(self):
    unittest.TestCase.setUp(self)

    self.appid = "testapp"
    os.environ["APPLICATION_ID"] = self.appid

    self.zipdata = cStringIO.StringIO()
    archive = zipfile.ZipFile(self.zipdata, "w")
    for i in range(10):
      archive.writestr("%d.txt" % i, "%d: %s" % (i, "*"*i))
    archive.close()

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

  def testJson(self):
    """Test that we can persist/restore using the json mechanism."""
    reader = input_readers.BlobstoreZipInputReader("someblob", 0, 1,
                                                   self.mockZipReader)
    json = reader.to_json()
    self.assertEquals({"blob_key": "someblob",
                       "start_index": 0,
                       "end_index": 1},
                      json)
    reader2 = input_readers.BlobstoreZipInputReader.from_json(json)
    self.assertEqual(str(reader), str(reader2))


class BlobstoreZipLineInputReaderTest(unittest.TestCase):
  READER_NAME = ("mapreduce.input_readers."
                 "BlobstoreZipLineInputReader")

  def setUp(self):
    unittest.TestCase.setUp(self)

    self.appid = "testapp"
    os.environ["APPLICATION_ID"] = self.appid

  def create_zip_data(self, blob_count):
    """Create blob_count blobs with uneven zip data."""
    self.zipdata = {}
    blob_keys = []
    for blob_number in range(blob_count):
      stream = cStringIO.StringIO()
      archive = zipfile.ZipFile(stream, "w")
      for file_number in range(3):
        lines = []
        for i in range(file_number + 1):
          lines.append("archive %s file %s line %s" %
                       (blob_number, file_number, i))
        archive.writestr("%d.txt" % file_number, "\n".join(lines))
      archive.close()
      blob_key = "blob%d" % blob_number
      self.zipdata[blob_key] = stream
      blob_keys.append(blob_key)

    return blob_keys

  def mockZipReader(self, blob_key):
    """Mocked out reader function that returns our in-memory zipfile."""
    return self.zipdata.get(blob_key)

  def split_input(self, blob_count, shard_count):
    """Generate some blobs and return the reader's split of them."""
    blob_keys = self.create_zip_data(blob_count)
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": self.READER_NAME,
        "mapper_params": {"blob_keys": blob_keys},
        "mapper_shard_count": shard_count})
    readers = input_readers.BlobstoreZipLineInputReader.split_input(
        mapper_spec, self.mockZipReader)
    return readers

  def testSplitInputOneBlob(self):
    """Simple case: split one blob into two groups."""
    readers = self.split_input(1, 2)
    self.assertEqual(2, len(readers))
    self.assertEqual("blobstore.BlobKey('blob0'):[0, 2]:0", str(readers[0]))
    self.assertEqual("blobstore.BlobKey('blob0'):[2, 3]:0", str(readers[1]))

  def testSplitInputOneBlobFourShards(self):
    """Corner case: Ask for more shards than we can deliver."""
    readers = self.split_input(1, 4)
    self.assertEqual(2, len(readers))
    self.assertEqual("blobstore.BlobKey('blob0'):[0, 2]:0", str(readers[0]))
    self.assertEqual("blobstore.BlobKey('blob0'):[2, 3]:0", str(readers[1]))

  def testSplitInputTwoBlobsTwoShards(self):
    """Simple case: Ask for num shards == num blobs."""
    readers = self.split_input(2, 2)
    self.assertEqual(2, len(readers))
    self.assertEqual("blobstore.BlobKey('blob0'):[0, 3]:0", str(readers[0]))
    self.assertEqual("blobstore.BlobKey('blob1'):[0, 3]:0", str(readers[1]))

  def testSplitInputTwoBlobsFourShards(self):
    """Easy case: Files split nicely into blobs."""
    readers = self.split_input(2, 4)
    self.assertEqual(4, len(readers))
    self.assertEqual("blobstore.BlobKey('blob0'):[0, 2]:0", str(readers[0]))
    self.assertEqual("blobstore.BlobKey('blob0'):[2, 3]:0", str(readers[1]))
    self.assertEqual("blobstore.BlobKey('blob1'):[0, 2]:0", str(readers[2]))
    self.assertEqual("blobstore.BlobKey('blob1'):[2, 3]:0", str(readers[3]))

  def testSplitInputTwoBlobsSixShards(self):
    """Corner case: Shards don't split nicely so we get too few."""
    readers = self.split_input(2, 6)
    # Note we might be able to make this return 6 with a more clever algorithm.
    self.assertEqual(4, len(readers))
    self.assertEqual("blobstore.BlobKey('blob0'):[0, 2]:0", str(readers[0]))
    self.assertEqual("blobstore.BlobKey('blob0'):[2, 3]:0", str(readers[1]))
    self.assertEqual("blobstore.BlobKey('blob1'):[0, 2]:0", str(readers[2]))
    self.assertEqual("blobstore.BlobKey('blob1'):[2, 3]:0", str(readers[3]))

  def testSplitInputTwoBlobsThreeShards(self):
    """Corner case: Shards don't split nicely so we get too few."""
    readers = self.split_input(2, 3)
    # Note we might be able to make this return 3 with a more clever algorithm.
    self.assertEqual(2, len(readers))
    self.assertEqual("blobstore.BlobKey('blob0'):[0, 3]:0", str(readers[0]))
    self.assertEqual("blobstore.BlobKey('blob1'):[0, 3]:0", str(readers[1]))

  def testSplitInputThreeBlobsTwoShards(self):
    """Corner case: More blobs than requested shards."""
    readers = self.split_input(3, 2)
    self.assertEqual(3, len(readers))
    self.assertEqual("blobstore.BlobKey('blob0'):[0, 3]:0", str(readers[0]))
    self.assertEqual("blobstore.BlobKey('blob1'):[0, 3]:0", str(readers[1]))
    self.assertEqual("blobstore.BlobKey('blob2'):[0, 3]:0", str(readers[2]))

  def testReadOneLineFile(self):
    """Test that the first file in the zip is returned correctly."""
    self.create_zip_data(1)
    reader = input_readers.BlobstoreZipLineInputReader("blob0", 0, 1, 0,
                                                       self.mockZipReader)
    offset_info, line = reader.next()
    self.assertEqual(("blob0", 0, 0), offset_info)
    self.assertEqual("archive 0 file 0 line 0", line)

    # This file only has one line.
    self.assertRaises(StopIteration, reader.next)

  def testReadTwoLineFile(self):
    """Test that the second file in the zip is returned correctly."""
    self.create_zip_data(1)
    reader = input_readers.BlobstoreZipLineInputReader("blob0", 1, 2, 0,
                                                       self.mockZipReader)
    offset_info, line = reader.next()
    self.assertEqual(("blob0", 1, 0), offset_info)
    self.assertEqual("archive 0 file 1 line 0", line)

    offset_info, line = reader.next()
    self.assertEqual(("blob0", 1, 24), offset_info)
    self.assertEqual("archive 0 file 1 line 1", line)

    # This file only has two lines.
    self.assertRaises(StopIteration, reader.next)

  def testReadSecondLineFile(self):
    """Test that the second line is returned correctly."""
    self.create_zip_data(1)
    reader = input_readers.BlobstoreZipLineInputReader("blob0", 2, 3, 5,
                                                       self.mockZipReader)
    offset_info, line = reader.next()
    self.assertEqual(("blob0", 2, 24), offset_info)
    self.assertEqual("archive 0 file 2 line 1", line)

    # If we persist/restore the reader, the new one should pick up where
    # we left off.
    reader2 = input_readers.BlobstoreZipLineInputReader.from_json(
        reader.to_json(), self.mockZipReader)

    offset_info, line = reader2.next()
    self.assertEqual(("blob0", 2, 48), offset_info)
    self.assertEqual("archive 0 file 2 line 2", line)

  def testReadAcrossFiles(self):
    """Test that we can read across all the files in the single blob."""
    self.create_zip_data(1)
    reader = input_readers.BlobstoreZipLineInputReader("blob0", 0, 3, 0,
                                                       self.mockZipReader)

    for file_number in range(3):
      for i in range(file_number + 1):
        offset_info, line = reader.next()
        self.assertEqual("blob0", offset_info[0])
        self.assertEqual(file_number, offset_info[1])
        self.assertEqual("archive %s file %s line %s" % (0, file_number, i),
                         line)

    self.assertRaises(StopIteration, reader.next)

  def testJson(self):
    """Test that we can persist/restore using the json mechanism."""
    reader = input_readers.BlobstoreZipLineInputReader("blob0", 0, 3, 20,
                                                       self.mockZipReader)
    json = reader.to_json()
    self.assertEquals({"blob_key": "blob0",
                       "start_file_index": 0,
                       "end_file_index": 3,
                       "offset": 20},
                      json)
    reader2 = input_readers.BlobstoreZipLineInputReader.from_json(json)
    self.assertEqual(str(reader), str(reader2))

if __name__ == "__main__":
  unittest.main()
