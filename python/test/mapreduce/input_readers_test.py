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
import logging
from testlib import mox
import os
import random
import string
import tempfile
import time
import unittest
import zipfile

from google.appengine.api import apiproxy_stub_map
from google.appengine.api.blobstore import blobstore_stub
from google.appengine.api.blobstore import dict_blob_storage
from google.appengine.api import datastore
from google.appengine.api import datastore_file_stub
from mapreduce.lib import files
from mapreduce.lib.files import records
from google.appengine.api import logservice
from google.appengine.api.logservice import log_service_pb
from google.appengine.api.logservice import logservice_stub
from google.appengine.api import namespace_manager
from google.appengine.ext import blobstore
from google.appengine.ext import db
from mapreduce.lib import key_range
from google.appengine.ext.blobstore import blobstore as blobstore_internal
from mapreduce import errors
from mapreduce import file_format_root
from mapreduce import input_readers
from mapreduce import model
from mapreduce import namespace_range
from testlib import testutil


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
  int_property = db.IntegerProperty()

class TestEntityWithDot(db.Model):
  """Test entity class with dot in its kind."""

  @classmethod
  def kind(cls):
    return "Test.Entity.With.Dot"


ENTITY_KIND = "__main__.TestEntity"


def key(entity_id, namespace=None, kind="TestEntity"):
  """Create a key for TestEntity with specified id.

  Used to shorten expected data.

  Args:
    entity_id: entity id
  Returns:
    db.Key instance with specified id for TestEntity.
  """
  return db.Key.from_path(kind, entity_id, namespace=namespace)


class DatastoreInputReaderTest(unittest.TestCase):
  """Test Datastore{,Key,Entity}InputReader classes."""

  def setUp(self):
    unittest.TestCase.setUp(self)
    self.appid = "testapp"
    os.environ["APPLICATION_ID"] = self.appid
    os.environ["AUTH_DOMAIN"] = "gmail.com"
    self.resetDatastore()
    namespace_manager.set_namespace(None)

  def resetDatastore(self, require_indexes=False):
    self.datastore = datastore_file_stub.DatastoreFileStub(
        self.appid, "/dev/null", "/dev/null", require_indexes=require_indexes)

    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub("datastore_v3", self.datastore)

  def split_into_key_ranges(self, shard_count, namespace=None):
    """Generate TestEntity split.

    This method assumes that the entity data will be split by KeyRange.

    Args:
      shard_count: number of shards to split into as int.
      namespace: the namespace to consider in the mapreduce. If not set then all
          namespaces are considered.

    Returns:
      list of key_range.KeyRange.
    """
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        {"entity_kind": ENTITY_KIND,
         "namespace": namespace},
        shard_count)
    ds_input_readers = input_readers.DatastoreInputReader.split_input(
        mapper_spec)
    return [
        input_reader._key_ranges or input_reader._ns_range
        for input_reader in ds_input_readers]

  def split_into_namespace_ranges(self, shard_count):
    """Generate TestEntity split.

    This method assumes that the entity data will be split by NamespaceRange.

    Args:
      shard_count: number of shards to split into as int.

    Returns:
      list of namespace_range.NamespaceRange.
    """
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        {"entity_kind": ENTITY_KIND},
        shard_count)
    ds_input_readers = input_readers.DatastoreInputReader.split_input(
        mapper_spec)
    ns_ranges = [input_reader._ns_range for input_reader in ds_input_readers]
    assert all(ns_ranges)
    return ns_ranges

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

  def testValidate_MissingEntityKind(self):
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
    """Test validate function with bad entity kind."""
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
    # Entity & Key reader should be ok.
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers."
            "DatastoreEntityInputReader",
        params, 1)
    input_readers.DatastoreEntityInputReader.validate(mapper_spec)

    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers."
            "DatastoreKeyInputReader",
        params, 1)
    input_readers.DatastoreKeyInputReader.validate(mapper_spec)

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

  def testValidate_Namespaces(self):
    """Tests validate function rejects namespaces param."""
    params = {
        "entity_kind": ENTITY_KIND,
        "namespaces": "hello"
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.DatastoreInputReader.validate,
                      mapper_spec)

  def testValidate_WrongTypeNamespace(self):
    """Tests validate function rejects namespace of incorrect type."""
    params = {
        "entity_kind": ENTITY_KIND,
        "namespace": 5
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.DatastoreInputReader.validate,
                      mapper_spec)

  def testValidate_Filters(self):
    """Tests validating filters parameter."""
    params = {
        "entity_kind": ENTITY_KIND,
        "filters": [("a", "=", 1), ("b", "=", 2)],
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    input_readers.DatastoreInputReader.validate(mapper_spec)

    params["filters"] = [["a", "=", 1]]
    input_readers.DatastoreInputReader.validate(mapper_spec)

    params["filters"] = {"a": "1"}
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.DatastoreInputReader.validate,
                      mapper_spec)

    params["filters"] = [("a", "<=", 1)]
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.DatastoreInputReader.validate,
                      mapper_spec)

    params["filters"] = [(1, "=", 1)]
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.DatastoreInputReader.validate,
                      mapper_spec)

  def testParameters(self):
    """Test that setting string parameters as they would be passed from a
    web interface works.
    """
    for _ in range(0, 100):
      TestEntity().put()

    namespace_manager.set_namespace("google")
    for _ in range(0, 100):
      TestEntity().put()

    namespace_manager.set_namespace("ibm")
    for _ in range(0, 100):
      TestEntity().put()

    namespace_manager.set_namespace(None)

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
    self.assertEquals(set(["", "ibm", "google"]),
                      set(k.namespace for k in reader[0]._key_ranges))

    params["batch_size"] = "24"
    params["namespace"] = ""
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    reader = input_readers.DatastoreInputReader.split_input(
        mapper_spec)
    self.assertEquals(24, reader[0]._batch_size)
    self.assertEquals([""], [k.namespace for k in reader[0]._key_ranges])

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
    """Full namespace range should be produced if there's no data."""
    self.assertEquals(
        [namespace_range.NamespaceRange()],
        self.split_into_key_ranges(10))

  def testChooseSplitPoints(self):
    """Tests AbstractDatastoreInputReader._choose_split_points."""
    self.assertEquals(
        [5],
        input_readers.AbstractDatastoreInputReader._choose_split_points(
            sorted([0, 9, 8, 7, 1, 2, 3, 4, 5, 6]), 2))

    self.assertEquals(
        [3, 7],
        input_readers.AbstractDatastoreInputReader._choose_split_points(
            sorted([0, 1, 7, 8, 9, 3, 2, 4, 6, 5]), 3))

    self.assertEquals(
        range(1, 10),
        input_readers.AbstractDatastoreInputReader._choose_split_points(
            sorted([0, 1, 7, 8, 9, 3, 2, 4, 6, 5]), 10))

    # Too few random keys
    self.assertRaises(
        AssertionError,
        input_readers.AbstractDatastoreInputReader._choose_split_points,
        sorted([0, 1, 7, 8, 9, 3, 2, 4, 6, 5]), 11)

  def testSplitNotEnoughData(self):
    """Splits should not intersect, if there's not enough data for each."""
    TestEntity().put()
    TestEntity().put()
    self.assertEquals([
        [key_range.KeyRange(key_start=None,
                            key_end=key(2),
                            direction='ASC',
                            include_start=False,
                            include_end=False,
                            namespace='')],
        [key_range.KeyRange(key_start=key(2),
                            key_end=None,
                            direction='ASC',
                            include_start=True,
                            include_end=False,
                            namespace='')],
        [None], [None]
        ],
        self.split_into_key_ranges(4))

  def testSplitNotEnoughDataSorts(self):
    """Splits should not intersect, if there's not enough data for each."""
    TestEntity().put()
    TestEntity().put()
    TestEntity().put()
    TestEntity().put()
    self.assertEquals([
        [key_range.KeyRange(key_start=None,
                            key_end=key(2),
                            direction='ASC',
                            include_start=False,
                            include_end=False,
                            namespace='')],
        [key_range.KeyRange(key_start=key(2),
                            key_end=key(4),
                            direction='ASC',
                            include_start=True,
                            include_end=False,
                            namespace='')],
        [key_range.KeyRange(key_start=key(4),
                            key_end=None,
                            direction='ASC',
                            include_start=True,
                            include_end=False,
                            namespace='')],
        [None]
        ],
        self.split_into_key_ranges(4))

  def testSplitLotsOfData(self):
    """Test lots of data case."""

    for _ in range(0, 100):
      TestEntity().put()

    namespace_manager.set_namespace("google")
    for _ in range(0, 40):
      TestEntity().put()
    namespace_manager.set_namespace(None)

    (reader1_key_ranges,
     reader2_key_ranges,
     reader3_key_ranges,
     reader4_key_ranges) = self.split_into_key_ranges(4)

    self.assertEquals(
        [
            key_range.KeyRange(key_start=None,
                               key_end=key(113, namespace="google"),
                               direction="ASC",
                               include_start=False,
                               include_end=False,
                               namespace="google"),
            key_range.KeyRange(key_start=None,
                               key_end=key(25),
                               direction="ASC",
                               include_start=False,
                               include_end=False),
        ],
        reader1_key_ranges)

    self.assertEquals(
        [
            key_range.KeyRange(key_start=key(113, namespace="google"),
                               key_end=key(120, namespace="google"),
                               direction="ASC",
                               include_start=True,
                               include_end=False,
                               namespace="google"),
            key_range.KeyRange(key_start=key(25),
                               key_end=key(48),
                               direction="ASC",
                               include_start=True,
                               include_end=False),
        ],
        reader2_key_ranges)

    self.assertEquals(
        [
            key_range.KeyRange(key_start=key(120, namespace="google"),
                               key_end=key(134, namespace="google"),
                               direction="ASC",
                               include_start=True,
                               include_end=False,
                               namespace="google"),
            key_range.KeyRange(key_start=key(48),
                               key_end=key(67),
                               direction="ASC",
                               include_start=True,
                               include_end=False),
        ],
        reader3_key_ranges)

    self.assertEquals(
        [
            key_range.KeyRange(key_start=key(134, namespace="google"),
                               key_end=None,
                               direction="ASC",
                               include_start=True,
                               include_end=False,
                               namespace="google"),
            key_range.KeyRange(key_start=key(67),
                               key_end=None,
                               direction="ASC",
                               include_start=True,
                               include_end=False),
        ],
        reader4_key_ranges)

  def testSplitLotsOfDataWithSingleNamespaceSpecified(self):
    """Split lots of data when a namespace param is given."""

    for _ in range(0, 100):
      TestEntity().put()

    namespace_manager.set_namespace("google")
    for _ in range(0, 40):
      TestEntity().put()
    namespace_manager.set_namespace(None)

    (reader1_key_ranges,
     reader2_key_ranges) = self.split_into_key_ranges(2, namespace="google")

    self.assertEquals(
        [key_range.KeyRange(key_start=None,
                            key_end=key(120, namespace="google"),
                            direction="ASC",
                            include_start=False,
                            include_end=False,
                            namespace="google")],
        reader1_key_ranges)

    self.assertEquals(
        [key_range.KeyRange(key_start=key(120, namespace="google"),
                            key_end=None,
                            direction="ASC",
                            include_start=True,
                            include_end=False,
                            namespace="google")],
        reader2_key_ranges)

  def testSplitLotsOfNamespaces(self):
    """Test splitting data with many namespaces."""
    for namespace in string.lowercase:
      namespace_manager.set_namespace(namespace)
      TestEntity().put()
    namespace_manager.set_namespace(None)

    self.assertEquals(
        set([namespace_range.NamespaceRange("", "m" + "z" * 99),
             namespace_range.NamespaceRange("n", "z" * 100)]),
        set(self.split_into_namespace_ranges(2)))

  def testGeneratorWithKeyRanges(self):
    """Test DatastoreInputReader as generator using KeyRanges."""
    expected_entities = []
    for _ in range(0, 100):
      entity = TestEntity()
      entity.put()
      expected_entities.append(entity)

    namespace_manager.set_namespace("google")
    for _ in range(0, 100):
      entity = TestEntity()
      entity.put()
      expected_entities.append(entity)
    namespace_manager.set_namespace(None)

    kranges = [key_range.KeyRange(key_start=key(25), key_end=key(50),
                                  direction="ASC",
                                  include_start=False, include_end=True),
               key_range.KeyRange(key_start=key(110, namespace="google"),
                                  key_end=key(150, namespace="google"),
                                  direction="ASC",
                                  include_start=False,
                                  include_end=True,
                                  namespace="google")]

    query_range = input_readers.DatastoreInputReader(
        ENTITY_KIND, key_ranges=kranges, ns_range=None, batch_size=50)

    entities = []

    for entity in query_range:
      entities.append(entity)

    self.assertEquals(65, len(entities))
    # Model instances are not comparable, so we'll compare a serialization.
    expected_values = [entity.to_xml() for entity
                       in expected_entities[25:50] + expected_entities[110:150]]
    actual_values = [entity.to_xml() for entity in entities]
    self.assertEquals(expected_values, actual_values)

  def testGeneratorWithNamespaceRange(self):
    """Test DatastoreInputReader as generator using a NamespaceRange."""
    expected_objects = []
    expected_entities = []
    for namespace in ["A", "B", "C", "D", "E"]:
      namespace_manager.set_namespace(namespace)
      for i in range(10):
        entity = TestEntity()
        entity.put()
        if namespace in ["B", "C", "D"]:
          if i == 0:
            expected_objects.append(input_readers.ALLOW_CHECKPOINT)
          expected_objects.append(entity)
          expected_entities.append(entity)
    namespace_manager.set_namespace(None)

    ns_range = namespace_range.NamespaceRange("B", "D")

    query_range = input_readers.DatastoreInputReader(
        ENTITY_KIND, key_ranges=None, ns_range=ns_range, batch_size=50)

    objects = []
    entities = []

    for o in query_range:
      objects.append(o)
      if o is not input_readers.ALLOW_CHECKPOINT:
        entities.append(o)

    self.assertEquals(len(expected_objects), len(objects))
    self.assertEquals(input_readers.ALLOW_CHECKPOINT, objects[0])
    self.assertEquals(input_readers.ALLOW_CHECKPOINT, objects[11])
    self.assertEquals(input_readers.ALLOW_CHECKPOINT, objects[22])

    # Model instances are not comparable, so we'll compare a serialization.
    expected_values = [entity.to_xml() for entity in expected_entities]
    actual_values = [entity.to_xml() for entity in entities]
    self.assertEqual(expected_values, actual_values)

  def testEntityGenerator(self):
    """Test DatastoreEntityInputReader."""
    expected_entities = []
    for _ in range(0, 100):
      model_instance = TestEntity()
      model_instance.put()
      expected_entities.append(model_instance._populate_internal_entity())

    namespace_manager.set_namespace("google")
    for _ in range(0, 100):
      model_instance = TestEntity()
      model_instance.put()
      expected_entities.append(model_instance._populate_internal_entity())
    namespace_manager.set_namespace(None)

    kranges = [key_range.KeyRange(key_start=key(25), key_end=key(50),
                                  direction="ASC",
                                  include_start=False, include_end=True),
               key_range.KeyRange(key_start=key(110, namespace="google"),
                                  key_end=key(150, namespace="google"),
                                  direction="ASC",
                                  include_start=False,
                                  include_end=True,
                                  namespace="google")]

    query_range = input_readers.DatastoreEntityInputReader(
        TestEntity.kind(), key_ranges=kranges, ns_range=None, batch_size=50)

    entities = []
    for entity in query_range:
      entities.append(entity)

    self.assertEquals(65, len(entities))
    self.assertEquals(expected_entities[25:50] +
                      expected_entities[110:150],
                      entities)

  def testShardDescription(self):
    """Tests the human-visible description of Datastore readers."""
    for i in xrange(10):
      TestEntity().put()
    splits = self.split_into_key_ranges(2)
    stringified = [str(s[0]) for s in splits]
    self.assertEquals(
        ["ASC(None to "
         "datastore_types.Key.from_path(u'TestEntity', 6L, _app=u'testapp'))",
         "ASC["
         "datastore_types.Key.from_path(u'TestEntity', 6L, _app=u'testapp')"
         " to None)"],
        stringified)

  def testFilterIter(self):
    """Tests iterating over input with filter set."""
    for i in range(0, 100):
      TestEntity(int_property=(i % 5)).put()

    reader = input_readers.DatastoreInputReader(
        ENTITY_KIND,
        key_ranges=[
          key_range.KeyRange(
            key_start=None,
            key_end=None,
            direction="ASC",
            include_start=False,
            include_end=False,
            namespace="")],
          ns_range=None,
          batch_size=50,
        filters=[("int_property", "=", 0)])

    entities = []
    for entity in reader:
      self.assertEquals(0, entity.int_property)
      entities.append(entity)
    self.assertEquals(20, len(entities))


class DatastoreKeyInputReaderTest(unittest.TestCase):
  """Tests for DatastoreKeyInputReader."""

  def setUp(self):
    unittest.TestCase.setUp(self)
    self.appid = "testapp"
    self.datastore = datastore_file_stub.DatastoreFileStub(
        self.appid, "/dev/null", "/dev/null", require_indexes=False)

    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub("datastore_v3", self.datastore)
    namespace_manager.set_namespace(None)

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

    namespace_manager.set_namespace("google")
    for _ in range(0, 100):
      expected_keys.append(TestEntity().put())
    namespace_manager.set_namespace(None)

    kranges = [key_range.KeyRange(key_start=key(25), key_end=key(50),
                                  direction="ASC",
                                  include_start=False, include_end=True),
               key_range.KeyRange(key_start=key(110, namespace="google"),
                                  key_end=key(150, namespace="google"),
                                  direction="ASC",
                                  include_start=False,
                                  include_end=True,
                                  namespace="google")]
    query_range = input_readers.DatastoreKeyInputReader(
        TestEntity.kind(), key_ranges=kranges, ns_range=None, batch_size=50)

    keys = []
    for k in query_range:
      keys.append(k)

    self.assertEquals(65, len(keys))
    self.assertEquals(expected_keys[25:50] + expected_keys[110:150], keys)

  def testEntityKindWithDot(self):
    """Test generator functionality."""
    expected_keys = []
    for _ in range(0, 5):
      expected_keys.append(TestEntityWithDot().put())

    params = {}
    params["entity_kind"] = TestEntityWithDot.kind()
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    readers = input_readers.DatastoreKeyInputReader.split_input(
        mapper_spec)
    self.assertEquals(1, len(readers))

    keys = []
    for k in readers[0]:
      keys.append(k)
    self.assertEquals(expected_keys, keys)

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
        OTHER_KIND, [krange],
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


BLOBSTORE_READER_NAME = (
    "mapreduce.input_readers.BlobstoreLineInputReader")


class BlobstoreLineInputReaderBlobstoreStubTest(unittest.TestCase):
  """Test the BlobstoreLineInputReader using the blobstore_stub.

  This test uses the blobstore stub to store the test data, vs the other
  test which uses mox and the like to simulate the blobstore having data.
  """

  def setUp(self):
    unittest.TestCase.setUp(self)

    self.appid = "testapp"
    os.environ["APPLICATION_ID"] = self.appid

    self.blob_storage = dict_blob_storage.DictBlobStorage()
    self.blobstore = blobstore_stub.BlobstoreServiceStub(self.blob_storage)
    # Blobstore uses datastore for BlobInfo.
    self.datastore = datastore_file_stub.DatastoreFileStub(
        self.appid, "/dev/null", "/dev/null", require_indexes=False)

    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub("blobstore", self.blobstore)
    apiproxy_stub_map.apiproxy.RegisterStub("datastore_v3", self.datastore)

  def BlobInfoGet(self, blob_key):
    """Mock out BlobInfo.get instead of the datastore."""
    data = self.blob_storage.OpenBlob(blob_key)
    return MockBlobInfo(data.len)

  def CheckAllDataRead(self, data, blob_readers):
    """Check that we can read all the data with several blob readers."""
    expected_results = data.split("\n")
    if not expected_results[-1]:
      expected_results.pop(-1)
    actual_results = []
    for reader in blob_readers:
      while True:
        try:
          unused_offset, line = reader.next()
          actual_results.append(line)
        except StopIteration:
          break
    self.assertEquals(expected_results, actual_results)

  def EndToEndTest(self, data, shard_count):
    """Create a blobstorelineinputreader and run it through its paces."""

    blob_key = "myblob_%d" % shard_count
    self.blobstore.CreateBlob(blob_key, data)
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": BLOBSTORE_READER_NAME,
        "mapper_params": {"blob_keys": [blob_key]},
        "mapper_shard_count": shard_count})
    blob_readers = input_readers.BlobstoreLineInputReader.split_input(
        mapper_spec)

    # Check that the shards cover the entire data range.
    # Another useful test would be to verify the exact splits generated or
    # the evenness of them.
    self.assertEqual(shard_count, len(blob_readers))
    previous_position = 0
    for reader in blob_readers:
      reader_info = reader.to_json()
      self.assertEqual(blob_key, reader_info["blob_key"])
      self.assertEqual(previous_position, reader_info["initial_position"])
      previous_position = reader_info["end_position"]
    self.assertEqual(len(data), previous_position)

    # See if we can read all the data with this split configuration.
    self.CheckAllDataRead(data, blob_readers)

  def TestAllSplits(self, data):
    """Test every split point by creating 2 splits, 0-m and m-n."""
    blob_key = "blob_key"
    self.blobstore.CreateBlob(blob_key, data)
    data_len = len(data)
    cls = input_readers.BlobstoreLineInputReader
    for i in range(data_len):
      chunks = []
      chunks.append(cls.from_json({
          cls.BLOB_KEY_PARAM: blob_key,
          cls.INITIAL_POSITION_PARAM: 0,
          cls.END_POSITION_PARAM: i+1}))
      chunks.append(cls.from_json({
          cls.BLOB_KEY_PARAM: blob_key,
          cls.INITIAL_POSITION_PARAM: i+1,
          cls.END_POSITION_PARAM: data_len}))
      self.CheckAllDataRead(data, chunks)

  def testEndToEnd(self):
    """End to end test of some data--split and read.."""
    # This particular pattern once caused bugs with 8 shards.
    data = "20-questions\r\n20q\r\na\r\n"
    self.EndToEndTest(data, 8)
    self.EndToEndTest(data, 1)
    self.EndToEndTest(data, 16)
    self.EndToEndTest(data, 7)

  def testEndToEndNoData(self):
    """End to end test of some data--split and read.."""
    data = ""
    self.EndToEndTest(data, 8)

  def testEverySplit(self):
    """Test some data with every possible split point."""
    self.TestAllSplits("20-questions\r\n20q\r\na\r\n")
    self.TestAllSplits("a\nbb\nccc\ndddd\n")
    self.TestAllSplits("aaaa\nbbb\ncc\nd\n")

  def testEverySplitNoTrailingNewLine(self):
    """Test some data with every possible split point."""
    self.TestAllSplits("20-questions\r\n20q\r\na")
    self.TestAllSplits("a\nbb\nccc\ndddd")
    self.TestAllSplits("aaaa\nbbb\ncc\nd")


class MockBlobInfo(object):
  def __init__(self, size):
    self.size = size


class BlobstoreLineInputReaderTest(unittest.TestCase):
  def setUp(self):
    unittest.TestCase.setUp(self)

    self.appid = "testapp"
    os.environ["APPLICATION_ID"] = self.appid
    self.mox = mox.Mox()
    self.mock_out_blob_info_size_called = False

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
    # Mock out blob key so as to avoid validation.
    blob_key_str = "foo"

    def fetch_data(blob_key, start, end):
      return data[start:end + 1]
    self.mox.stubs.Set(blobstore_internal, "fetch_data", fetch_data)

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
    if not self.mock_out_blob_info_size_called:
      self.mock_out_blob_info_size_called = True
      self.mox.StubOutWithMock(blobstore, "BlobKey", use_mock_anything=True)
      self.mox.StubOutWithMock(blobstore.BlobInfo, "get",
                               use_mock_anything=True)

    blob_key = "bar" + blob_key_str
    blobstore.BlobKey(blob_key_str).AndReturn(blob_key)
    blobstore.BlobInfo.get(blob_key).AndReturn(MockBlobInfo(size))

  def testSplitInput(self):
    # TODO(user): Mock out equiv
    self.mockOutBlobInfoSize(200)
    self.mox.ReplayAll()
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": BLOBSTORE_READER_NAME,
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
        "mapper_input_reader": BLOBSTORE_READER_NAME,
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
        "mapper_input_reader": BLOBSTORE_READER_NAME,
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
        "mapper_input_reader": BLOBSTORE_READER_NAME,
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
        "mapper_input_reader": BLOBSTORE_READER_NAME,
        "mapper_params": {"blob_keys": ["foo"] * 1000},
        "mapper_shard_count": 2})
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.BlobstoreLineInputReader.validate,
                      mapper_spec)

  def testNoKeys(self):
    """Tests when there are no blobkeys present as input."""
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": BLOBSTORE_READER_NAME,
        "mapper_params": {"blob_keys": []},
        "mapper_shard_count": 2})
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.BlobstoreLineInputReader.validate,
                      mapper_spec)

  def testInvalidKey(self):
    """Tests when there a blobkeys in the input is invalid."""
    mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": BLOBSTORE_READER_NAME,
        "mapper_params": {"blob_keys": ["foo", "nosuchblob"]},
        "mapper_shard_count": 2})
    self.mockOutBlobInfoSize(100, blob_key_str="foo")
    blobstore.BlobKey("nosuchblob").AndReturn("nosuchblob")
    blobstore.BlobInfo.get("nosuchblob").AndReturn(None)
    self.mox.ReplayAll()
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.BlobstoreLineInputReader.validate,
                      mapper_spec)
    self.mox.VerifyAll()


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


# Dummy start up time of a mapreduce.
STARTUP_TIME_US = 1000


class MockUnappliedQuery(datastore.Query):
  """Mocks unapplied query in order to mimic existence of unapplied jobs."""

  def __init__(self, results):
    """Constructs unapplied query with given results."""
    self.results = results

  def Get(self, limit, config):
    """Fetches query results."""
    if limit != input_readers.ConsistentKeyReader._BATCH_SIZE:
      raise Exception("Unexpected limit %s" % limit)
    if config.deadline != 270:
      raise Exception("Unexpected deadline %s" % config.deadline)

    return self.results


class RandomStringInputReaderTest(unittest.TestCase):
  """Tests for RandomStringInputReader."""

  def testIter(self):
    input_reader = input_readers.RandomStringInputReader(10, 9)
    i = 0
    for content in input_reader:
      i += 1
      self.assertEquals(9, len(content))
    self.assertEquals(10, i)

  def testEndToEnd(self):
    mapper_spec = model.MapperSpec(
        "test_handler",
        input_readers.__name__ + ".RandomStringInputReader",
        {
            "input_reader": {
                "count": 1000
            },
        },
        99)
    readers = input_readers.RandomStringInputReader.split_input(mapper_spec)
    i = 0
    for reader in readers:
      for _ in reader:
        i += 1
    self.assertEquals(1000, i)

  def testValidate(self):
    mapper_spec = model.MapperSpec(
        "test_handler",
        input_readers.__name__ + ".RandomStringInputReader",
        {
            "input_reader": {
                "count": "1000"
            },
        },
        99)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.RandomStringInputReader.validate,
                      mapper_spec)

    mapper_spec = model.MapperSpec(
        "test_handler",
        input_readers.__name__ + ".RandomStringInputReader",
        {
            "input_reader": {
                "count": -1
            },
        },
        99)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.RandomStringInputReader.validate,
                      mapper_spec)

    mapper_spec = model.MapperSpec(
        "test_handler",
        input_readers.__name__ + ".RandomStringInputReader",
        {
            "input_reader": {
                "count": 100
            },
        },
        -1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.RandomStringInputReader.validate,
                      mapper_spec)

    mapper_spec = model.MapperSpec(
        "test_handler",
        input_readers.__name__ + ".RandomStringInputReader",
        {
            "input_reader": {
                "count": 100,
                "string_length": 1.5
            },
        },
        99)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.RandomStringInputReader.validate,
                      mapper_spec)

  def testToFromJson(self):
    input_reader = input_readers.RandomStringInputReader(10, 9)
    reader_in_json = input_reader.to_json()
    self.assertEquals({"count": 10, "string_length": 9}, reader_in_json)
    input_readers.RandomStringInputReader.from_json(reader_in_json)
    self.assertEquals(10, input_reader._count)


class ConsistentKeyReaderTest(unittest.TestCase):
  """Tests for the ConsistentKeyReader."""

  MAPREDUCE_READER_SPEC = ("%s.%s" %
                           (input_readers.ConsistentKeyReader.__module__,
                            input_readers.ConsistentKeyReader.__name__))

  def setUp(self):
    """Sets up the test harness."""
    unittest.TestCase.setUp(self)
    self.app_id = "myapp"
    self.kind_id = "somekind"

    self.mapper_params = {
        "entity_kind": self.kind_id,
        "start_time_us": STARTUP_TIME_US,
        "enable_quota": False}
    self.mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": ConsistentKeyReaderTest.MAPREDUCE_READER_SPEC,
        "mapper_params": self.mapper_params,
        "mapper_shard_count": 10})

    self.reader = input_readers.ConsistentKeyReader(
        self.kind_id,
        key_ranges=[key_range.KeyRange()])
    self.reader.start_time_us = STARTUP_TIME_US
    os.environ["APPLICATION_ID"] = self.app_id

    self.datastore = datastore_file_stub.DatastoreFileStub(
        self.app_id, "/dev/null", "/dev/null")

    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub("datastore_v3", self.datastore)

    self.mox = mox.Mox()

  def tearDown(self):
    """Verifies mox expectations."""
    try:
      self.mox.VerifyAll()
    finally:
      self.mox.UnsetStubs()

  def testSplitInputNoData(self):
    """Splits empty input among several shards."""
    readers = input_readers.ConsistentKeyReader.split_input(self.mapper_spec)
    self.assertEquals(1, len(readers))

    r = readers[0]
    self.assertEquals(self.kind_id, r._entity_kind)
    self.assertEquals(STARTUP_TIME_US, r.start_time_us)
    self.assertEquals(namespace_range.NamespaceRange(), r._ns_range)

  def testSplitInputNotEnoughData(self):
    """Tests mapper_shard_count > number of entities."""
    datastore.Put(datastore.Entity(self.kind_id))
    datastore.Put(datastore.Entity(self.kind_id))
    namespace_manager.set_namespace('a')
    datastore.Put(datastore.Entity(self.kind_id))
    namespace_manager.set_namespace(None)

    readers = input_readers.ConsistentKeyReader.split_input(self.mapper_spec)
    self.assertEquals(10, len(readers))

    for r in readers:
      self.assertEquals(self.kind_id, r._entity_kind)
      self.assertEquals(STARTUP_TIME_US, r.start_time_us)

    self.assertEqual([key_range.KeyRange(key_start=None,
                                         key_end=None,
                                         direction='ASC',
                                         include_start=False,
                                         include_end=False,
                                         namespace='a'),
                      key_range.KeyRange(key_start=None,
                                         key_end=key(2, kind=self.kind_id),
                                         direction='ASC',
                                         include_start=False,
                                         include_end=False,
                                         namespace='')],
                     readers[0]._key_ranges)

    self.assertEqual([None,
                      key_range.KeyRange(key_start=key(2, kind=self.kind_id),
                                         key_end=None,
                                         direction='ASC',
                                         include_start=True,
                                         include_end=False,
                                         namespace='')],
                     readers[1]._key_ranges)

    for i in range(2, len(readers)):
      self.assertEquals([None, None],
                        readers[i]._key_ranges,
                        '[None] != readers[%d]._key_ranges (%s)' %
                        (i, readers[i]._key_ranges))

  def testSplitInput(self):
    """Splits input among several shards."""
    for _ in range(100):
      datastore.Put(datastore.Entity(self.kind_id))
    readers = input_readers.ConsistentKeyReader.split_input(self.mapper_spec)
    self.assertEquals(10, len(readers))

    for r in readers:
      self.assertEquals(self.kind_id, r._entity_kind)
      self.assertEquals(STARTUP_TIME_US, r.start_time_us)

    # The end ranges should be half openned.
    self.assertEquals(None, readers[0]._key_ranges[0].key_start)
    self.assertEquals(None, readers[-1]._key_ranges[0].key_end)

  def testReaderGeneratorSimple(self):
    """Tests reader generator when there are no unapplied jobs."""
    k1 = datastore.Put(datastore.Entity(self.kind_id))
    k2 = datastore.Put(datastore.Entity(self.kind_id))

    keys = list(self.reader)
    self.assertEquals([k1, k2], keys)

  def testReaderGeneratorSimpleWithEmptyDatastore(self):
    """Tests reader generator when there are no unapplied jobs or entities."""
    keys = list(self.reader)
    self.assertEquals([], keys)

  def testReaderGeneratorUnappliedJobsWithKeyRanges(self):
    """Tests reader generator when there are some unapplied jobs."""
    k1 = datastore.Put(datastore.Entity(self.kind_id))
    k2 = datastore.Put(datastore.Entity(self.kind_id))
    k3 = datastore.Put(datastore.Entity(self.kind_id))

    dummy_k1 = db.Key.from_path(
        *(k1.to_path() + [input_readers.ConsistentKeyReader.DUMMY_KIND,
                          input_readers.ConsistentKeyReader.DUMMY_ID]))
    dummy_k2 = db.Key.from_path(
        *(k2.to_path() + [input_readers.ConsistentKeyReader.DUMMY_KIND,
                          input_readers.ConsistentKeyReader.DUMMY_ID]))
    dummy_k3 = db.Key.from_path(
        *(k3.to_path() + [input_readers.ConsistentKeyReader.DUMMY_KIND,
                          input_readers.ConsistentKeyReader.DUMMY_ID]))

    self.mox.StubOutWithMock(self.reader, "_make_unapplied_query")
    self.mox.StubOutWithMock(db, "get")

    # Pretend that batch_size = 2.
    self.reader._make_unapplied_query(key_range.KeyRange()).AndReturn(
        MockUnappliedQuery([k1, k2]))
    db.get([dummy_k1, dummy_k2], config=mox.IgnoreArg())
    self.reader._make_unapplied_query(
        key_range.KeyRange(key_start=k2, include_start=False)).AndReturn(
            MockUnappliedQuery([k3]))
    db.get([dummy_k3], config=mox.IgnoreArg())  # Entity was deleted.
    self.reader._make_unapplied_query(
        key_range.KeyRange(key_start=k3, include_start=False)).AndReturn(
            MockUnappliedQuery([]))

    self.mox.ReplayAll()

    keys = list(self.reader)
    self.assertEquals([k1, k2, k3], keys)

  def testReaderGeneratorWithNamespaceRange(self):
    """Tests reader generator with namespaces but no unapplied jobs."""
    self.reader = input_readers.ConsistentKeyReader(
        self.kind_id,
        ns_range=namespace_range.NamespaceRange("a", "z"))
    self.reader.start_time_us = STARTUP_TIME_US

    expected_objects = set()
    for ns in ["b", "g", "happy", "helloworld", "r", "s", "t"]:
      namespace_manager.set_namespace(ns)
      expected_objects.add(input_readers.ALLOW_CHECKPOINT)
      expected_objects.add(datastore.Put(datastore.Entity(self.kind_id)))
    namespace_manager.set_namespace(None)

    self.mox.ReplayAll()
    keys = set(self.reader)
    self.assertEquals(expected_objects, keys)

  def testReaderGeneratorUnappliedJobsWithNamespaceRange(self):
    """Tests reader generator when there are some unapplied jobs."""
    self.reader = input_readers.ConsistentKeyReader(
        self.kind_id,
        ns_range=namespace_range.NamespaceRange("a", "z"))
    self.reader.start_time_us = STARTUP_TIME_US

    expected_objects = set()
    def AddUnappliedEntities(*args):
      namespace_manager.set_namespace("a")
      expected_objects.add(input_readers.ALLOW_CHECKPOINT)
      expected_objects.add(datastore.Put(
        datastore.Entity(self.kind_id, name="a")))

      namespace_manager.set_namespace("d")
      expected_objects.add(input_readers.ALLOW_CHECKPOINT)
      expected_objects.add(datastore.Put(
        datastore.Entity(self.kind_id, name="d")))

      namespace_manager.set_namespace("z")
      expected_objects.add(input_readers.ALLOW_CHECKPOINT)
      expected_objects.add(datastore.Put
          (datastore.Entity(self.kind_id, name="z")))
      namespace_manager.set_namespace(None)

    for c in ["b", "g", "t"]:
      namespace_manager.set_namespace(c)
      expected_objects.add(input_readers.ALLOW_CHECKPOINT)
      expected_objects.add(datastore.Put(datastore.Entity(self.kind_id)))
    namespace_manager.set_namespace(None)

    self.mox.StubOutWithMock(
        input_readers.ConsistentKeyReader,
        "_get_unapplied_jobs_accross_namespaces")

    # Applying jobs first.
    input_readers.ConsistentKeyReader._get_unapplied_jobs_accross_namespaces(
        "a", "z", None).WithSideEffects(
            AddUnappliedEntities).AndReturn([
                db.Key.from_path(self.kind_id, "a", namespace="a"),
                db.Key.from_path(self.kind_id, "d", namespace="d"),
                db.Key.from_path(self.kind_id, "z", namespace="z")])
    input_readers.ConsistentKeyReader._get_unapplied_jobs_accross_namespaces(
        "a", "z", None).AndReturn([])

    self.mox.ReplayAll()
    keys = set(self.reader)
    self.assertEquals(expected_objects, keys)


class NamespaceInputReaderTest(unittest.TestCase):
  """Tests for NamespaceInputReader."""

  MAPREDUCE_READER_SPEC = ("%s.%s" %
                           (input_readers.NamespaceInputReader.__module__,
                            input_readers.NamespaceInputReader.__name__))

  def setUp(self):
    unittest.TestCase.setUp(self)
    self.app_id = "myapp"

    self.mapper_spec = model.MapperSpec.from_json({
        "mapper_handler_spec": "FooHandler",
        "mapper_input_reader": NamespaceInputReaderTest.MAPREDUCE_READER_SPEC,
        "mapper_params": {"batch_size": 2},
        "mapper_shard_count": 10})

    os.environ["APPLICATION_ID"] = self.app_id

    self.datastore = datastore_file_stub.DatastoreFileStub(
        self.app_id, "/dev/null", "/dev/null")

    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub("datastore_v3", self.datastore)

  def testSplitInputNoData(self):
    """Test reader with no data in datastore."""
    readers = input_readers.NamespaceInputReader.split_input(self.mapper_spec)
    self.assertEquals(10, len(readers))

    namespaces = set()
    for r in readers:
      namespaces.update(list(r))
    self.assertEquals(set(), namespaces)

  def testSplitDefaultNamespaceOnly(self):
    """Test reader with only default namespace populated."""
    TestEntity().put()
    readers = input_readers.NamespaceInputReader.split_input(self.mapper_spec)
    self.assertEquals(10, len(readers))

    namespaces = set()
    for r in readers:
      namespaces.update(list(r))
    self.assertEquals(set([""]), namespaces)

  def testSplitNamespacesPresent(self):
    """Test reader with multiple namespaces present."""
    TestEntity().put()
    for i in string.letters + string.digits:
      namespace_manager.set_namespace(i)
      TestEntity().put()
    namespace_manager.set_namespace(None)

    readers = input_readers.NamespaceInputReader.split_input(self.mapper_spec)
    self.assertEquals(10, len(readers))

    namespaces = set()
    for r in readers:
      namespaces.update(list(r))

    # test read
    self.assertEquals(set(list(string.letters + string.digits) + [""]),
                      namespaces)

  def testValidate_Passes(self):
    """Test validate function accepts valid parameters."""
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.NamespaceInputReader",
        {"batch_size": 10}, 1)
    input_readers.NamespaceInputReader.validate(mapper_spec)

  def testValidate_BadClassFails(self):
    """Test validate function rejects non-matching class parameter."""
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        {}, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.NamespaceInputReader.validate,
                      mapper_spec)

  def testValidate_BadBatchSize(self):
    """Test validate function rejects bad batch size."""
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.NamespaceInputReader",
        {"batch_size": "xxx"}, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.NamespaceInputReader.validate,
                      mapper_spec)

    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.NamespaceInputReader",
        {"batch_size": "0"}, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.NamespaceInputReader.validate,
                      mapper_spec)

  def testJson(self):
    """Test that we can persist/restore using the json mechanism."""
    reader = input_readers.NamespaceInputReader(
        namespace_range.NamespaceRange("", "A"))

    self.assertEquals(
        {"namespace_range": {"namespace_end": "A", "namespace_start": ""},
         "batch_size": 10},
        reader.to_json())

    TestEntity().put()
    iter(reader).next()
    json = reader.to_json()
    self.assertEquals(
        {"namespace_range": {"namespace_end": "A", "namespace_start": "-"},
         "batch_size": 10},
        json)

    self.assertEquals(
        reader.ns_range,
        input_readers.NamespaceInputReader.from_json(json).ns_range)

    self.assertEquals(
        reader._batch_size,
        input_readers.NamespaceInputReader.from_json(json)._batch_size)


class RecordsReaderTest(testutil.HandlerTestBase):
  """Tests for RecordsReader."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    self.mapper_spec = model.MapperSpec(
        "test_handler",
        input_readers.__name__ + ".RecordsReader",
        {"file": "testfile"},
        1)

  def testValidatePass(self):
    """Test validation passes."""
    input_readers.RecordsReader.validate(self.mapper_spec)

  def testValidateInvalidReaderClass(self):
    """Test invalid reader class name."""
    self.mapper_spec.input_reader_spec = __name__ + ".RecordsReaderTest"
    self.assertRaises(errors.BadReaderParamsError,
                      input_readers.RecordsReader.validate,
                      self.mapper_spec)

  def testValidateNoFileParam(self):
    """Test validate without file param."""
    self.mapper_spec.params = {}
    self.assertRaises(errors.BadReaderParamsError,
                      input_readers.RecordsReader.validate,
                      self.mapper_spec)

  def testSplitInput(self):
    """Test split input implementation."""
    readers = input_readers.RecordsReader.split_input(self.mapper_spec)
    self.assertEquals(1, len(readers))
    self.assertEquals({"filenames": ["testfile"], "position": 0},
                      readers[0].to_json())

  def testSplitInput_MultipleShards(self):
    """Test split input implementation for multiple shards."""
    test_files = ["testfile%d" % i for i in xrange(25)]
    self.mapper_spec.shard_count = 4

    # Only one file but multiple shards requested
    self.mapper_spec.params["files"] = test_files[:1]
    readers = input_readers.RecordsReader.split_input(self.mapper_spec)
    self.assertEquals(
        [{'position': 0, 'filenames': ['testfile0']},
         {'position': 0, 'filenames': []},
         {'position': 0, 'filenames': []},
         {'position': 0, 'filenames': []}],
        [r.to_json() for r in readers])

    # Number of files equal to number of shards
    self.mapper_spec.params["files"] = test_files[:4]
    readers = input_readers.RecordsReader.split_input(self.mapper_spec)
    self.assertEquals(
        [{'position': 0, 'filenames': ['testfile0']},
         {'position': 0, 'filenames': ['testfile1']},
         {'position': 0, 'filenames': ['testfile2']},
         {'position': 0, 'filenames': ['testfile3']}],
        [r.to_json() for r in readers])

    # Number of files less than 2x the number of shards
    self.mapper_spec.params["files"] = test_files[:7]
    readers = input_readers.RecordsReader.split_input(self.mapper_spec)
    self.assertEquals(
        [{'position': 0, 'filenames': ['testfile0', 'testfile4']},
         {'position': 0, 'filenames': ['testfile1', 'testfile5']},
         {'position': 0, 'filenames': ['testfile2', 'testfile6']},
         {'position': 0, 'filenames': ['testfile3']}],
        [r.to_json() for r in readers])

    # Number of files more than 2x the number of shards
    self.mapper_spec.params["files"] = test_files[:10]
    readers = input_readers.RecordsReader.split_input(self.mapper_spec)
    self.assertEquals(
        [{'position': 0, 'filenames': ['testfile0', 'testfile4', 'testfile8']},
         {'position': 0, 'filenames': ['testfile1', 'testfile5', 'testfile9']},
         {'position': 0, 'filenames': ['testfile2', 'testfile6']},
         {'position': 0, 'filenames': ['testfile3', 'testfile7']}],
        [r.to_json() for r in readers])

    # Multiple files but only one shard requested
    self.mapper_spec.shard_count = 1
    self.mapper_spec.params["files"] = test_files[:3]
    readers = input_readers.RecordsReader.split_input(self.mapper_spec)
    self.assertEquals(
        [{'position': 0, 'filenames': ['testfile0', 'testfile1', 'testfile2']}],
        [r.to_json() for r in readers])

  def testToJsonFromJson(self):
    """Test to/from json implementations."""
    reader = input_readers.RecordsReader.from_json(
        {"filenames": ["test"], "position": 200})
    self.assertEquals({"filenames": ["test"], "position": 200},
                      reader.to_json())

  def testIter(self):
    """Test __iter__ implementation."""
    # Prepare the file.
    input_file = files.blobstore.create()
    input_data = [str(i) for i in range(100)]

    with files.open(input_file, "a") as f:
      with records.RecordsWriter(f) as w:
        for record in input_data:
          w.write(record)
    files.finalize(input_file)
    input_file = files.blobstore.get_file_name(
        files.blobstore.get_blob_key(input_file))

    # Test reader.
    reader = input_readers.RecordsReader([input_file], 0)
    self.assertEquals(input_data, list(reader))

    # Move offset past EOF.
    reader = input_readers.RecordsReader([input_file], 10000000)
    self.assertEquals([], list(reader))

  def testIterMultipleFiles(self):
    """Test __iter__ implementation over multiple files."""
    # Prepare the file.
    input_file1 = files.blobstore.create()
    input_file2 = files.blobstore.create()
    input_data1 = [str(i) for i in range(100)]
    input_data2 = [str(i * i) for i in range(100)]

    with files.open(input_file1, "a") as f:
      with records.RecordsWriter(f) as w:
        for record in input_data1:
          w.write(record)

    with files.open(input_file2, "a") as f:
      with records.RecordsWriter(f) as w:
        for record in input_data2:
          w.write(record)

    files.finalize(input_file1)
    files.finalize(input_file2)
    input_file1 = files.blobstore.get_file_name(
        files.blobstore.get_blob_key(input_file1))
    input_file2 = files.blobstore.get_file_name(
        files.blobstore.get_blob_key(input_file2))

    # Test reader.
    reader = input_readers.RecordsReader([input_file1, input_file2], 0)
    self.assertEquals(input_data1 + input_data2, list(reader))

  def testStr(self):
    """Tests the __str__ conversion method."""
    reader = input_readers.RecordsReader.from_json(
        {"filenames": ["test"], "position": 200})
    self.assertEquals("['test']:200", str(reader))
    reader = input_readers.RecordsReader.from_json(
        {"filenames": [], "position": 0})
    self.assertEquals("[]:0", str(reader))


class LogInputReaderTest(unittest.TestCase):
  """Tests for LogInputReaderTest."""

  def setUp(self):
    unittest.TestCase.setUp(self)

    self.num_shards = 4
    self.app_id = "app1"
    self.major_version_id = "1"
    self.version_id = "1.2"
    self.offset = "\n\x0eO)\x1c\xf7\x00\tn\xde\x08\xd5C{\x00\x00"
    os.environ["APPLICATION_ID"] = self.app_id
    prototype_request = log_service_pb.LogReadRequest()
    prototype_request.set_app_id(self.app_id)
    self.mapper_spec = model.MapperSpec(
        "test_handler",
        input_readers.__name__ + ".LogInputReader",
        {
          "input_reader": {
            "start_time": 0,
            "end_time": 128,
            "offset": self.offset,
            "version_ids": ["1"],
            "minimum_log_level": logservice.LOG_LEVEL_INFO,
            "include_incomplete": True,
            "include_app_logs": True,
            "prototype_request": prototype_request.Encode()
          },
        },
        self.num_shards)

  def testValidatePasses(self):
    """Test validate function accepts valid parameters."""
    input_readers.LogInputReader.validate(self.mapper_spec)

  def testValidateInvalidReaderClass(self):
    """Test invalid reader class name."""
    self.mapper_spec.input_reader_spec = __name__ + ".LogInputReaderTest"
    self.assertRaises(errors.BadReaderParamsError,
                      input_readers.LogInputReader.validate,
                      self.mapper_spec)

  def testValidateUnrecognizedParam(self):
    """Test validate with an unrecognized parameter."""
    self.mapper_spec.params["input_reader"]["unrecognized"] = True
    self.assertRaises(errors.BadReaderParamsError,
                      input_readers.LogInputReader.validate,
                      self.mapper_spec)

  def testValidateNoVersionIdsParam(self):
    """Test validate without version_ids param."""
    del self.mapper_spec.params["input_reader"]["version_ids"]
    self.assertRaises(errors.BadReaderParamsError,
                      input_readers.LogInputReader.validate,
                      self.mapper_spec)

  def testValidateTooManyVersionIdsParam(self):
    """Test validate with a malformed version_ids param."""
    # This is really testing the validation that logservice.fetch() itself does.
    self.mapper_spec.params["input_reader"]["version_ids"] = "1"
    self.assertRaises(errors.BadReaderParamsError,
                      input_readers.LogInputReader.validate,
                      self.mapper_spec)

  def testValidateTimeRangeParams(self):
    """Test validate with bad sets of start/end time params."""
    # start_time must be specified and may not be None.
    del self.mapper_spec.params["input_reader"]["start_time"]
    self.assertRaises(errors.BadReaderParamsError,
                      input_readers.LogInputReader.validate,
                      self.mapper_spec)

    self.mapper_spec.params["input_reader"]["start_time"] = None
    self.assertRaises(errors.BadReaderParamsError,
                      input_readers.LogInputReader.validate,
                      self.mapper_spec)

    # It's okay to not specify an end_time; the current time will be assumed.
    self.mapper_spec.params["input_reader"]["start_time"] = time.time() - 1
    del self.mapper_spec.params["input_reader"]["end_time"]
    input_readers.LogInputReader.validate(self.mapper_spec)

    # Start time must be less than end_time, whether implicit or explicit.
    self.mapper_spec.params["input_reader"]["start_time"] = time.time() + 100
    self.assertRaises(errors.BadReaderParamsError,
                      input_readers.LogInputReader.validate,
                      self.mapper_spec)

    self.mapper_spec.params["input_reader"]["end_time"] = time.time() + 50
    self.assertRaises(errors.BadReaderParamsError,
                      input_readers.LogInputReader.validate,
                      self.mapper_spec)

    # Success again
    self.mapper_spec.params["input_reader"]["end_time"] = time.time() + 150
    input_readers.LogInputReader.validate(self.mapper_spec)

    # start_time must be less than end time.
    self.mapper_spec.params["input_reader"]["start_time"] = \
        self.mapper_spec.params["input_reader"]["end_time"]
    self.assertRaises(errors.BadReaderParamsError,
                      input_readers.LogInputReader.validate,
                      self.mapper_spec)

  def testValidateInvalidPrototypeRequest(self):
    """Test validate without specifying a prototype request."""
    self.mapper_spec.params["input_reader"]["prototype_request"] = "__bad__"
    self.assertRaises(errors.BadReaderParamsError,
                      input_readers.LogInputReader.validate,
                      self.mapper_spec)

  def testValidateNoPrototypeRequest(self):
    """Test validate without specifying a prototype request."""
    del self.mapper_spec.params["input_reader"]["prototype_request"]
    input_readers.LogInputReader.validate(self.mapper_spec)

  def testStr(self):
    """Simplistic test for stringification of LogInputReader."""
    readers = input_readers.LogInputReader.split_input(self.mapper_spec)
    self.assertEqual(
        "LogInputReader"
        "(end_time=128,"
        " include_app_logs=True,"
        " include_incomplete=True,"
        " minimum_log_level=1,"
        " offset=\'%s',"
        " prototype_request=\'app_id: \"app1\"\n\',"
        " start_time=96,"
        " version_ids=[\'1\']"
        ")" % self.offset, str(readers[-1]))

  def testEvenLogSplit(self):
    readers = input_readers.LogInputReader.split_input(self.mapper_spec)
    self.assertEquals(self.num_shards, len(readers))

    for i, reader in enumerate(readers):
      start = i * 32
      end = (i + 1) * 32
      self.assertEquals(start, reader._LogInputReader__params["start_time"])
      self.assertEquals(end, reader._LogInputReader__params["end_time"])

  def testUnevenLogSplit(self):
    start = 0
    end = 100
    num_shards = 3

    mapper_spec = model.MapperSpec(
        "test_handler",
        input_readers.__name__ + ".LogInputReader",
        {"start_time": start, "end_time": end},
        num_shards)

    readers = input_readers.LogInputReader.split_input(mapper_spec)
    self.assertEquals(num_shards, len(readers))

    self.assertEquals(0, readers[0]._LogInputReader__params["start_time"])
    self.assertEquals(33, readers[0]._LogInputReader__params["end_time"])
    self.assertEquals(33, readers[1]._LogInputReader__params["start_time"])
    self.assertEquals(66, readers[1]._LogInputReader__params["end_time"])
    self.assertEquals(66, readers[2]._LogInputReader__params["start_time"])
    self.assertEquals(100, readers[2]._LogInputReader__params["end_time"])

  def testToJsonFromJson(self):
    """Test to/from json implementations."""
    readers = input_readers.LogInputReader.split_input(self.mapper_spec)
    start_time = self.mapper_spec.params["input_reader"]["start_time"]
    end_time = self.mapper_spec.params["input_reader"]["end_time"]
    seconds_per_shard = (end_time - start_time) / self.mapper_spec.shard_count
    for i, reader in enumerate(readers):
      # Full roundtrip test; this cannot verify that all fields are encoded.
      as_json = reader.to_json_str()
      from_json = input_readers.LogInputReader.from_json_str(as_json)
      self.assertEquals(from_json.to_json_str(), as_json)

      # Test correctness of individual fields.
      params_from_json = from_json._LogInputReader__params
      self.assertEquals(params_from_json["start_time"],
                        start_time + i * seconds_per_shard)
      if i != len(readers) - 1:
        self.assertEquals(params_from_json["end_time"],
                          start_time + (i + 1) * seconds_per_shard)
      else:
        self.assertEquals(params_from_json["end_time"], end_time)
      self.assertEquals(params_from_json["offset"], self.offset)
      self.assertEquals(params_from_json["version_ids"], ["1"])
      self.assertEquals(params_from_json["minimum_log_level"],
                        logservice.LOG_LEVEL_INFO)
      self.assertEquals(params_from_json["include_incomplete"], True)
      self.assertEquals(params_from_json["include_app_logs"], True)
      self.assertEquals(params_from_json["prototype_request"].app_id(),
                        self.app_id)

  def createLogs(self, count=10):
    """Create a set of test log records."""
    # Prepare the data.
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub(
        "datastore_v3",
        datastore_file_stub.DatastoreFileStub(self.app_id, None))
    apiproxy_stub_map.apiproxy.RegisterStub(
        "logservice", logservice_stub.LogServiceStub(persist=False))

    # Write test data.
    writer = logservice_stub.RequestLogWriter(persist=True)
    expected = []
    for i in xrange(count):
      os.environ["REQUEST_ID_HASH"] = str(i)  # Required by logservice_stub.
      writer.write_request_info(ip="127.0.0.1",
                                app_id=self.app_id,
                                version_id=self.version_id,
                                nickname="test@example.com",
                                user_agent="Chrome/15.0.874.106",
                                host="127.0.0.1:8080",
                                start_time=i * 1000000,
                                end_time=i * 1000000 + 500)
      writer.write("GET", "/", 200, 0, "HTTP/1.1")
      expected.append({"start_time": i, "end_time": i + .0005})
    # NOTE(user): write_request_info() calls set_namespace(), and has a long
    # comment on why it can't restore the previous namespace, but I don't
    # understand it.  Manually resetting here to avoid test state leakage.
    namespace_manager.set_namespace(None)
    expected.reverse()  # Results come back in most-recent-first order.

    return expected

  def verifyLogs(self, expected, retrieved):
    """Verifies a list of retrieved logs against a list of expectations.

    Args:
      expected: A list of dictionaries, each containing property names and
        values to be compared against the corresponding entries in 'retrieved'.
      retrieved: A list of RequestLog objects, hopefully matching 'expected'.
    """
    self.assertEquals(len(expected), len(retrieved))
    for expected, retrieved in zip(expected, retrieved):
      for property_name, value in expected.iteritems():
        self.assertEquals(value, getattr(retrieved, property_name))

  def testIter(self):
    """Test __iter__ implementation."""
    request_count = 100  # Number of log records over which we'll test.
    restart_interval = 7  # Frequency of de/serialization test below.

    expected = self.createLogs(request_count)

    # Test simple read.
    reader = input_readers.LogInputReader(version_ids=[self.major_version_id],
                                          start_time=0, end_time=101 * 1e6)
    self.verifyLogs(expected, list(reader))

    # Test interrupted read, exercising de/serialization process occassionally.
    reader = input_readers.LogInputReader(version_ids=[self.major_version_id],
                                          start_time=0, end_time=101 * 1e6)
    logs = []
    iterator = reader.__iter__()
    restart_counter = 0
    while True:
      restart_counter += 1
      if restart_counter >= restart_interval:
        as_json = reader.to_json_str()
        reader = input_readers.LogInputReader.from_json_str(as_json)
        iterator = reader.__iter__()
        restart_counter = 0
      try:
        logs.append(iterator.next())
      except StopIteration:
        break
    self.verifyLogs(expected, logs)

  def testOffset(self):
    """Test that the user can provide an offset parameter."""
    request_count = 10  # NOTE(user): This test is O(N*N), so keep this low.

    # Iterate through the logs using logservice.fetch(); for each log found,
    # retrieve all records after it using the LogInputReader, and verify that
    # the sum of records fetched plus those from the InputReader adds up.
    expected = self.createLogs(request_count)
    fetched_logs = []
    for log in logservice.fetch(version_ids=[self.major_version_id]):
      fetched_logs.append(log)
      reader = input_readers.LogInputReader(version_ids=[self.major_version_id],
                                            start_time=0, end_time=101 * 1e6,
                                            offset=log.offset)
      self.verifyLogs(expected, fetched_logs + list(reader))


class FileInputReaderTest(unittest.TestCase):
  """Tests for FileInputReader."""

  def setUp(self):
    unittest.TestCase.setUp(self)

    self._files_open = files.file.open
    self._files_stat = files.file.stat
    files.file.open = open
    files.file.stat = os.stat
    self.__created_files = []

    self.mox = mox.Mox()

  def tearDown(self):
    self.mox.UnsetStubs()
    self.mox.VerifyAll()
    files.file.open = self._files_open
    files.file.stat = self._files_stat
    for filename in self.__created_files:
      os.remove(filename)

  def createTmp(self):
    _, path = tempfile.mkstemp()
    self.__created_files.append(path)
    return path

  def createReader(self, filenames, format_string):
    root = file_format_root.split(filenames, format_string, 1)[0]
    return input_readers.FileInputReader(root)

  def assertEqualsAfterJson(self, expected, input_reader):
    contents = []

    while True:
      try:
        contents.append(input_reader.next())
        input_reader = input_readers.FileInputReader.from_json(
            input_reader.to_json())
      except StopIteration:
        break

    self.assertEquals(expected, contents)

  def testIter(self):
    filenames = []
    for _ in range(3):
      path = self.createTmp()
      filenames.append(path)
      f = open(path, "w")
      f.write("l1\nl2\nl3\n")
      f.close()

    input_reader = self.createReader(filenames, "lines")
    self.assertEqualsAfterJson(["l1\n", "l2\n", "l3\n"]*3, input_reader)

  def setUpForEndToEndTest(self, num_shards):
    """Setup mox for end to end test.

    Create 100 zip files, each of which has 10 member files.

    Args:
      num_shards: number of shards from mapper_spec.

    Returns:
      mapper_spec: a mapper spec model with attributes set accordingly.
    """

    def _random_filename():
      return "".join(random.choice(string.ascii_letters) for _ in range(10))

    tmp_filenames = []
    for i in range(100):
      path = self.createTmp()
      tmp_filenames.append(path)
      archive = zipfile.ZipFile(path, "w")
      for i in range(10):
        archive.writestr(_random_filename(), (string.ascii_letters + "\n")*100)
      archive.close()
    input_filenames = ["/gs/bucket/" + name for name in tmp_filenames]

    self.mox.StubOutWithMock(files.gs, "parseGlob")
    # Once for validate.
    for i, input_filename in enumerate(input_filenames):
      files.gs.parseGlob(input_filename).AndReturn(tmp_filenames[i])

    # Once for split input.
    for i, input_filename in enumerate(input_filenames):
      files.gs.parseGlob(input_filename).AndReturn(tmp_filenames[i])

    self.mox.ReplayAll()

    return model.MapperSpec(
        "test_handler",
        input_readers.__name__ + ".FileInputReader",
        {
            "input_reader": {
                "format": "zip[lines]",
                "files": input_filenames
            }
        },
        num_shards)

  def runEndToEndTest(self, num_shards):
    """Create FileInputReaders and run them through their phases.

    FileInputReader has these phases: validate, split input, __init__, __iter__,
    to_json, from_json, __iter__.

    Args:
      num_shards: number of shards to run mappers.
    """
    mapper_spec = self.setUpForEndToEndTest(num_shards)
    input_readers.FileInputReader.validate(mapper_spec)
    mr_input_readers = input_readers.FileInputReader.split_input(mapper_spec)

    # Check we can read all inputs.
    counter = 0
    logging.warning("FileInputReader shards %d readers %d",
                    num_shards, len(mr_input_readers))
    for reader in mr_input_readers:
      for line in reader:
        self.assertEquals(string.ascii_letters + "\n", line)
        counter += 1
        # This single line will increase test time by 100 magnitude.
        # The reason is that all the cached zipfiles have to be reread.
        # reader = input_readers.FileInputReader.from_json(reader.to_json())
    self.assertEquals(100*10*100, counter)

  def testEndToEnd1(self):
    self.runEndToEndTest(1)

  def testEndToEnd3(self):
    self.runEndToEndTest(3)

  def testEndToEnd33(self):
    self.runEndToEndTest(33)

  def testEndToEnd66(self):
    self.runEndToEndTest(66)

  def testEndToEnd100(self):
    self.runEndToEndTest(100)

  def testEndToEnd1000(self):
    self.runEndToEndTest(1000)

  def testValidateMissingFiles(self):
    params = {"format": "zip"}
    mapper_spec = model.MapperSpec(
        "FooHandler",
        input_readers.__name__ + ".FileInputReader",
        params,
        1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.FileInputReader.validate,
                      mapper_spec)

  def testValidateInvalidFiles(self):
    params = {"files": None}
    mapper_spec = model.MapperSpec(
        "FooHandler",
        input_readers.__name__ + ".FileInputReader",
        params,
        1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.FileInputReader.validate,
                      mapper_spec)

    mapper_spec.params = {"files": "/gs/bucket/file", "format": "line"}
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.FileInputReader.validate,
                      mapper_spec)

    mapper_spec.params = {"files": ["/gs/bucket/file",
                                    "/typoe/bucket/file"],
                          "format": "line"}
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.FileInputReader.validate,
                      mapper_spec)

  def testValidateMissingFormats(self):
    params = {"files": ["/gs/bucket1/file1"]}
    mapper_spec = model.MapperSpec(
        "FooHandler",
        input_readers.__name__ + ".FileInputReader",
        params,
        1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.FileInputReader.validate,
                      mapper_spec)

  def testValidateInvalidFormats(self):
    params = {"format": 1,
              "files": ["/gs/bucket/file"]}
    mapper_spec = model.MapperSpec(
        "FooHandler",
        input_readers.__name__ + ".FileInputReader",
        params,
        1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.FileInputReader.validate,
                      mapper_spec)

    mapper_spec.params["format"] = "foo"
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.FileInputReader.validate,
                      mapper_spec)


if __name__ == "__main__":
  unittest.main()
