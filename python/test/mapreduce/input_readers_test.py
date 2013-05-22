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
import datetime
import logging
import math
from testlib import mox
import os
import random
import string
import tempfile
import time
import unittest
import zipfile

from google.appengine.ext import ndb

from google.appengine.api import apiproxy_stub_map
from google.appengine.api.blobstore import blobstore_stub
from google.appengine.api.blobstore import dict_blob_storage
from google.appengine.api import datastore
from google.appengine.api import datastore_file_stub
from google.appengine.api import datastore_types
from google.appengine.api import files
from google.appengine.api.files import records
from google.appengine.api import logservice
from google.appengine.api.logservice import log_service_pb
from google.appengine.api.logservice import logservice_stub
from google.appengine.datastore import datastore_stub_util
from google.appengine.api import namespace_manager
from google.appengine.ext import blobstore
from google.appengine.ext import db
from mapreduce.lib import key_range
from google.appengine.ext import testbed
from google.appengine.ext.blobstore import blobstore as blobstore_internal
from mapreduce import errors
from mapreduce import file_format_root
from mapreduce import input_readers
from mapreduce import model
from mapreduce import namespace_range
from testlib import testutil
from google.appengine.datastore import entity_pb

# pylint: disable=g-import-not-at-top
# TODO(user): Cleanup imports if/when cloudstorage becomes part of runtime.
try:
  import cloudstorage
  enable_cloudstorage_tests = True
except ImportError:
  enable_cloudstorage_tests = False


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
  datetime_property = db.DateTimeProperty(auto_now=True)

  a = db.IntegerProperty()
  b = db.IntegerProperty()


class NdbTestEntity(ndb.Model):
  datetime_property = ndb.DateTimeProperty(auto_now=True)

  a = ndb.IntegerProperty()
  b = ndb.IntegerProperty()


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


def set_scatter_setter(key_names_to_vals):
  """Monkey patch the scatter property setter.

  Args:
    key_names_to_vals: a dict from key_name to scatter property value.
      The value will be casted to str as the scatter property is. Entities
      who key is in the map will have the corresponding scatter property.
  """

  def _get_scatter_property(entity_proto):
    key_name = entity_proto.key().path().element_list()[-1].name()
    if key_name in key_names_to_vals:
      scatter_property = entity_pb.Property()
      scatter_property.set_name(datastore_types.SCATTER_SPECIAL_PROPERTY)
      scatter_property.set_meaning(entity_pb.Property.BYTESTRING)
      scatter_property.set_multiple(False)
      property_value = scatter_property.mutable_value()
      property_value.set_stringvalue(str(key_names_to_vals[key_name]))
      return scatter_property
  datastore_stub_util._SPECIAL_PROPERTY_MAP[
      datastore_types.SCATTER_SPECIAL_PROPERTY] = (
          False, True, _get_scatter_property)


def _create_entities(keys_itr,
                     key_to_scatter_val,
                     ns=None,
                     entity_model_cls=TestEntity):
  """Create entities for tests.

  Args:
    keys_itr: an iterator that contains all the key names.
    key_to_scatter_val: a dict that maps key names to its scatter values.
    ns: the namespace to create the entity at.
    entity_model_cls: entity model class.

  Returns:
    A list of entities created.
  """
  namespace_manager.set_namespace(ns)
  set_scatter_setter(key_to_scatter_val)
  entities = []
  for k in keys_itr:
    entity = entity_model_cls(key_name=str(k))
    entities.append(entity)
    entity.put()
  namespace_manager.set_namespace(None)
  return entities


class AbstractDatastoreInputReaderTest(unittest.TestCase):
  """Tests for AbstractDatastoreInputReader."""

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.appid = "testapp"
    os.environ["APPLICATION_ID"] = self.appid
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    namespace_manager.set_namespace(None)

  def tearDown(self):
    # Restore the scatter property setter to the original one.
    datastore_stub_util._SPECIAL_PROPERTY_MAP[
        datastore_types.SCATTER_SPECIAL_PROPERTY] = (
            False, True, datastore_stub_util._GetScatterProperty)
    self.testbed.deactivate()

  def testValidate_Passes(self):
    """Test validate function accepts valid parameters."""
    params = {
        "entity_kind": ENTITY_KIND,
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    input_readers.AbstractDatastoreInputReader.validate(mapper_spec)

  def testValidate_NoEntityFails(self):
    """Test validate function raises exception with no entity parameter."""
    params = {}
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      input_readers.AbstractDatastoreInputReader.validate,
                      mapper_spec)

  def testValidate_EntityKindWithNoModel(self):
    """Test validate function with bad entity kind."""
    params = {
        "entity_kind": "foo",
        }

    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params, 1)
    input_readers.AbstractDatastoreInputReader.validate(mapper_spec)

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
                      input_readers.AbstractDatastoreInputReader.validate,
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
                      input_readers.AbstractDatastoreInputReader.validate,
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
                      input_readers.AbstractDatastoreInputReader.validate,
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
                      input_readers.AbstractDatastoreInputReader.validate,
                      mapper_spec)

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

  def _assertEquals_splitNSByScatter(self, shards, expected, ns=""):
    results = input_readers.RawDatastoreInputReader._split_ns_by_scatter(
        shards, ns, "TestEntity", self.appid)
    self.assertEquals(expected, results)

  def testSplitNSByScatter_NotEnoughData(self):
    """Splits should not intersect, if there's not enough data for each."""
    _create_entities(range(2), {"1": 1})

    expected = [key_range.KeyRange(key_start=None,
                                   key_end=key("1"),
                                   direction="ASC",
                                   include_start=False,
                                   include_end=False,
                                   namespace="",
                                   _app=self.appid),
                key_range.KeyRange(key_start=key("1"),
                                   key_end=None,
                                   direction="ASC",
                                   include_start=True,
                                   include_end=False,
                                   namespace="",
                                   _app=self.appid),
                None, None]
    self._assertEquals_splitNSByScatter(4, expected)

  def testSplitNSByScatter_NotEnoughData2(self):
    """Splits should not intersect, if there's not enough data for each."""
    _create_entities(range(10), {"2": 2, "4": 4})
    expected = [key_range.KeyRange(key_start=None,
                                   key_end=key("2"),
                                   direction="ASC",
                                   include_start=False,
                                   include_end=False,
                                   namespace="",
                                   _app=self.appid),
                key_range.KeyRange(key_start=key("2"),
                                   key_end=key("4"),
                                   direction="ASC",
                                   include_start=True,
                                   include_end=False,
                                   namespace="",
                                   _app=self.appid),
                key_range.KeyRange(key_start=key("4"),
                                   key_end=None,
                                   direction="ASC",
                                   include_start=True,
                                   include_end=False,
                                   namespace="",
                                   _app=self.appid),
                None]
    self._assertEquals_splitNSByScatter(4, expected)

  def testSplitNSByScatter_LotsOfData(self):
    """Split lots of data for each shard."""
    _create_entities(range(100),
                     {"80": 80, "50": 50, "30": 30, "10": 10},
                     ns="google")
    expected = [key_range.KeyRange(key_start=None,
                                   key_end=key("30", namespace="google"),
                                   direction="ASC",
                                   include_start=False,
                                   include_end=False,
                                   namespace="google",
                                   _app=self.appid),
                key_range.KeyRange(key_start=key("30", namespace="google"),
                                   key_end=key("80", namespace="google"),
                                   direction="ASC",
                                   include_start=True,
                                   include_end=False,
                                   namespace="google",
                                   _app=self.appid),
                key_range.KeyRange(key_start=key("80", namespace="google"),
                                   key_end=None,
                                   direction="ASC",
                                   include_start=True,
                                   include_end=False,
                                   namespace="google",
                                   _app=self.appid),
               ]
    self._assertEquals_splitNSByScatter(3, expected, ns="google")

  def testToKeyRangesByShard(self):
    namespaces = [str(i) for i in range(3)]
    for ns in namespaces:
      _create_entities(range(10), {"5": 5}, ns)
    shards = 2

    expected = [
        key_range.KeyRange(key_start=None,
                           key_end=key("5", namespace="0"),
                           direction="ASC",
                           include_start=False,
                           include_end=False,
                           namespace="0",
                           _app=self.appid),
        key_range.KeyRange(key_start=None,
                           key_end=key("5", namespace="1"),
                           direction="ASC",
                           include_start=False,
                           include_end=False,
                           namespace="1",
                           _app=self.appid),
        key_range.KeyRange(key_start=None,
                           key_end=key("5", namespace="2"),
                           direction="ASC",
                           include_start=False,
                           include_end=False,
                           namespace="2",
                           _app=self.appid),

        key_range.KeyRange(key_start=key("5", namespace="0"),
                           key_end=None,
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="0",
                           _app=self.appid),
        key_range.KeyRange(key_start=key("5", namespace="1"),
                           key_end=None,
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="1",
                           _app=self.appid),
        key_range.KeyRange(key_start=key("5", namespace="2"),
                           key_end=None,
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="2",
                           _app=self.appid),
    ]

    kranges_by_shard = (
        input_readers.AbstractDatastoreInputReader._to_key_ranges_by_shard(
            self.appid, namespaces, shards,
            model.QuerySpec(entity_kind="TestEntity")))
    self.assertEquals(shards, len(kranges_by_shard))

    expected.sort()
    results = []
    for kranges in kranges_by_shard:
      results.extend(list(kranges))
    results.sort()
    self.assertEquals(expected, results)

  def testToKeyRangesByShard_UnevenNamespaces(self):
    namespaces = [str(i) for i in range(3)]
    _create_entities(range(10), {"5": 5}, namespaces[0])
    _create_entities(range(10), {"5": 5, "6": 6}, namespaces[1])
    _create_entities(range(10), {"5": 5, "6": 6, "7": 7}, namespaces[2])
    shards = 3

    expected = [
        # shard 1
        key_range.KeyRange(key_start=None,
                           key_end=key("5", namespace="0"),
                           direction="ASC",
                           include_start=False,
                           include_end=False,
                           namespace="0",
                           _app=self.appid),
        key_range.KeyRange(key_start=None,
                           key_end=key("5", namespace="1"),
                           direction="ASC",
                           include_start=False,
                           include_end=False,
                           namespace="1",
                           _app=self.appid),
        key_range.KeyRange(key_start=None,
                           key_end=key("6", namespace="2"),
                           direction="ASC",
                           include_start=False,
                           include_end=False,
                           namespace="2",
                           _app=self.appid),
        # shard 2
        key_range.KeyRange(key_start=key("5", namespace="0"),
                           key_end=None,
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="0",
                           _app=self.appid),
        key_range.KeyRange(key_start=key("5", namespace="1"),
                           key_end=key("6", namespace="1"),
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="1",
                           _app=self.appid),
        key_range.KeyRange(key_start=key("6", namespace="2"),
                           key_end=key("7", namespace="2"),
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="2",
                           _app=self.appid),
        # shard 3
        key_range.KeyRange(key_start=key("6", namespace="1"),
                           key_end=None,
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="1",
                           _app=self.appid),
        key_range.KeyRange(key_start=key("7", namespace="2"),
                           key_end=None,
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="2",
                           _app=self.appid),
    ]
    kranges_by_shard = (
        input_readers.AbstractDatastoreInputReader._to_key_ranges_by_shard(
            self.appid, namespaces, shards,
            model.QuerySpec(entity_kind="TestEntity")))
    self.assertEquals(shards, len(kranges_by_shard))

    expected.sort()
    results = []
    for kranges in kranges_by_shard:
      results.extend(list(kranges))
    results.sort()
    self.assertEquals(expected, results)


class DatastoreInputReaderTestCommon(unittest.TestCase):
  """Common tests for concrete DatastoreInputReaders."""

  # Subclass should override with its own create entities function.
  @property
  def _create_entities(self):
    return _create_entities

  # Subclass should override with its own entity kind or model class path
  @property
  def entity_kind(self):
    return "TestEntity"

  # Subclass should override with its own reader class.
  @property
  def reader_cls(self):
    return input_readers.RawDatastoreInputReader

  def _get_keyname(self, entity):
    """Get keyname from an entity of certain type."""
    return entity.key().name()

  # Subclass should override with its own assert equals.
  def _assertEquals_splitInput(self, itr, keys):
    """AssertEquals helper for splitInput tests.

    Check the outputs from a single shard.

    Args:
      itr: input reader returned from splitInput.
      keys: a set of expected key names from this iterator.
    """
    results = []
    while True:
      try:
        results.append(self._get_keyname(iter(itr).next()))
        itr = itr.__class__.from_json(itr.to_json())
      except StopIteration:
        break
    results.sort()
    keys.sort()
    self.assertEquals(keys, results)

  # Subclass should override with its own assert equals.
  def _assertEqualsForAllShards_splitInput(self, keys, max_read, *itrs):
    """AssertEquals helper for splitInput tests.

    Check the outputs from all shards. This is used when sharding
    has random factor.

    Args:
      keys: a set of expected key names from this iterator.
      max_read: limit number of results read from the iterators before failing
        or None for no limit. Useful for preventing infinite loops or bounding
        the execution of the test.
      *itrs: input readers returned from splitInput.
    """
    results = []
    for itr in itrs:
      while True:
        try:
          results.append(self._get_keyname(iter(itr).next()))
          itr = itr.__class__.from_json(itr.to_json())
          if max_read is not None and len(results) > max_read:
            self.fail("Too many results found")
        except StopIteration:
          break
    results.sort()
    keys.sort()
    self.assertEquals(keys, results)

  def setUp(self):
    self.testbed = testbed.Testbed()
    unittest.TestCase.setUp(self)
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    namespace_manager.set_namespace(None)
    self._original_max = (
        self.reader_cls.MAX_NAMESPACES_FOR_KEY_SHARD)
    self.reader_cls.MAX_NAMESPACES_FOR_KEY_SHARD = 2

  def tearDown(self):
    # Restore the scatter property set to the original one.
    datastore_stub_util._SPECIAL_PROPERTY_MAP[
        datastore_types.SCATTER_SPECIAL_PROPERTY] = (
            False, True, datastore_stub_util._GetScatterProperty)
    # Restore max limit on ns sharding.
    self.reader_cls.MAX_NAMESPACES_FOR_KEY_SHARD = (
        self._original_max)
    self.testbed.deactivate()

  def testSplitInput_withNs(self):
    self._create_entities(range(3), {"1": 1}, "f")
    params = {
        "entity_kind": self.entity_kind,
        "namespace": "f",
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 2)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertEquals(2, len(results))
    self._assertEqualsForAllShards_splitInput(["0", "1", "2"], None, *results)

  def testSplitInput_withNs_moreShardThanScatter(self):
    self._create_entities(range(3), {"1": 1}, "f")
    params = {
        "entity_kind": self.entity_kind,
        "namespace": "f",
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 4)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertTrue(len(results) >= 2)
    self._assertEqualsForAllShards_splitInput(["0", "1", "2"], None, *results)

  def testSplitInput_noEntity(self):
    params = {
        "entity_kind": self.entity_kind,
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 1)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertEquals(None, results)

  def testSplitInput_moreThanOneNS(self):
    self._create_entities(range(3), {"1": 1}, "1")
    self._create_entities(range(10, 13), {"11": 11}, "2")
    params = {
        "entity_kind": self.entity_kind,
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 4)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertTrue(len(results) >= 2)
    self._assertEqualsForAllShards_splitInput(
        ["0", "1", "2", "10", "11", "12"], None, *results)

  def testSplitInput_moreThanOneUnevenNS(self):
    self._create_entities(range(5), {"1": 1, "3": 3}, "1")
    self._create_entities(range(10, 13), {"11": 11}, "2")
    params = {
        "entity_kind": self.entity_kind,
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 4)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertTrue(len(results) >= 3)
    self._assertEqualsForAllShards_splitInput(
        ["0", "1", "2", "3", "4", "10", "11", "12"], None, *results)

  def testSplitInput_lotsOfNS(self):
    self._create_entities(range(3), {"1": 1}, "9")
    self._create_entities(range(3, 6), {"4": 4}, "_")
    self._create_entities(range(6, 9), {"7": 7}, "a")
    params = {
        "entity_kind": self.entity_kind,
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 3)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertEquals(3, len(results))
    self._assertEquals_splitInput(results[0], ["0", "1", "2"])
    self._assertEquals_splitInput(results[1], ["3", "4", "5"])
    self._assertEquals_splitInput(results[2], ["6", "7", "8"])

  def testSplitInput_withNsAndDefaultNs(self):
    shards = 2
    # 10 entities in the default namespace
    empty_ns_keys = [str(k) for k in range(10)]
    self._create_entities(empty_ns_keys,
                          dict([(k, 1) for k in empty_ns_keys]),
                          None)
    # 10 entities for each of N different non-default namespaces. The number
    # of namespaces, N, is set to be twice the cutoff for switching to sharding
    # by namespace instead of keys.
    non_empty_ns_keys = []
    for ns_num in range(self.reader_cls.MAX_NAMESPACES_FOR_KEY_SHARD * 2):
      ns_keys = ["n-%02d-k-%02d" % (ns_num, k) for k in range(10)]
      non_empty_ns_keys.extend(ns_keys)
      self._create_entities(ns_keys,
                            dict([(k, 1) for k in ns_keys]),
                            "%02d" % ns_num)

    # Test a query over all namespaces
    params = {
        "entity_kind": self.entity_kind,
        "namespace": None}
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params,
        shards)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertEqual(shards, len(results))
    all_keys = empty_ns_keys + non_empty_ns_keys
    self._assertEqualsForAllShards_splitInput(all_keys,
                                              len(all_keys),
                                              *results)


class RawDatastoreInputReaderTest(DatastoreInputReaderTestCommon):
  """RawDatastoreInputReader specific tests."""

  @property
  def reader_cls(self):
    return input_readers.RawDatastoreInputReader

  def testValidate_Filters(self):
    """Tests validating filters parameter."""
    params = {
        "entity_kind": self.entity_kind,
        "filters": [("a", "=", 1), ("b", "=", 2)],
        }
    new = datetime.datetime.now()
    old = new.replace(year=new.year-1)
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "RawDatastoreInputReader",
        params, 1)
    self.reader_cls.validate(mapper_spec)

    # Only equality filters supported.
    params["filters"] = [["datetime_property", ">", old],
                         ["datetime_property", "<=", new],
                         ["a", "=", 1]]
    self.assertRaises(input_readers.BadReaderParamsError,
                      self.reader_cls.validate,
                      mapper_spec)

  def testEntityKindWithDot(self):
    self._create_entities(range(3), {"1": 1}, "", TestEntityWithDot)

    params = {
        "entity_kind": TestEntityWithDot.kind(),
        "namespace": "",
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "RawDatastoreInputReader",
        params, 2)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertEquals(2, len(results))
    self._assertEqualsForAllShards_splitInput(["0", "1", "2"], None, *results)

  def testRawEntityTypeFromOtherApp(self):
    """Test reading from other app."""
    OTHER_KIND = "bar"
    OTHER_APP = "foo"
    apiproxy_stub_map.apiproxy.GetStub("datastore_v3").SetTrusted(True)
    expected_keys = [str(i) for i in range(10)]
    for k in expected_keys:
      datastore.Put(datastore.Entity(OTHER_KIND, name=k, _app=OTHER_APP))

    params = {
        "entity_kind": OTHER_KIND,
        "_app": OTHER_APP,
    }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "RawDatastoreInputReader",
        params, 1)
    itr = self.reader_cls.split_input(mapper_spec)[0]
    self._assertEquals_splitInput(itr, expected_keys)
    apiproxy_stub_map.apiproxy.GetStub("datastore_v3").SetTrusted(False)


class DatastoreEntityInputReaderTest(RawDatastoreInputReaderTest):
  """DatastoreEntityInputReader tests."""

  @property
  def reader_cls(self):
    return input_readers.DatastoreEntityInputReader

  def _get_keyname(self, entity):
    # assert item is of low level datastore Entity type.
    self.assertTrue(isinstance(entity, datastore.Entity))
    return entity.key().name()


class DatastoreKeyInputReaderTest(RawDatastoreInputReaderTest):
  """DatastoreKeyInputReader tests."""

  @property
  def reader_cls(self):
    return input_readers.DatastoreKeyInputReader

  def _get_keyname(self, entity):
    return entity.name()


class DatastoreInputReaderTest(DatastoreInputReaderTestCommon):
  """Test DatastoreInputReader."""

  @property
  def reader_cls(self):
    return input_readers.DatastoreInputReader

  @property
  def entity_kind(self):
    return "__main__.TestEntity"

  def testValidate_EntityKindWithNoModel(self):
    """Test validate function with no model."""
    params = {
        "entity_kind": "foo",
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "DatastoreInputReader",
        params, 1)
    self.assertRaises(input_readers.BadReaderParamsError,
                      self.reader_cls.validate,
                      mapper_spec)

  def testValidate_Filters(self):
    """Tests validating filters parameter."""
    params = {
        "entity_kind": self.entity_kind,
        "filters": [("a", "=", 1), ("b", "=", 2)],
        }
    new = datetime.datetime.now()
    old = new.replace(year=new.year-1)
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "DatastoreInputReader",
        params, 1)
    self.reader_cls.validate(mapper_spec)

    params["filters"] = [["a", ">", 1], ["a", "<", 2]]
    self.reader_cls.validate(mapper_spec)

    params["filters"] = [["datetime_property", ">", old],
                         ["datetime_property", "<=", new],
                         ["a", "=", 1]]
    self.reader_cls.validate(mapper_spec)

    params["filters"] = [["a", "=", 1]]
    self.reader_cls.validate(mapper_spec)

    # Invalid field c
    params["filters"] = [("c", "=", 1)]
    self.assertRaises(input_readers.BadReaderParamsError,
                      self.reader_cls.validate,
                      mapper_spec)

    # Expect a range.
    params["filters"] = [("a", "<=", 1)]
    self.assertRaises(input_readers.BadReaderParamsError,
                      self.reader_cls.validate,
                      mapper_spec)

    # Value should be a datetime.
    params["filters"] = [["datetime_property", ">", 1],
                         ["datetime_property", "<=", datetime.datetime.now()]]
    self.assertRaises(input_readers.BadReaderParamsError,
                      self.reader_cls.validate,
                      mapper_spec)

    # Expect a closed range.
    params["filters"] = [["datetime_property", ">", new],
                         ["datetime_property", "<=", old]]
    self.assertRaises(input_readers.BadReaderParamsError,
                      self.reader_cls.validate,
                      mapper_spec)

  def _set_vals(self, entities, a_vals, b_vals):
    """Set a, b values for entities."""
    vals = []
    for a in a_vals:
      for b in b_vals:
        vals.append((a, b))
    for e, val in zip(entities, vals):
      e.a = val[0]
      e.b = val[1]
      e.put()

  def testSplitInput_shardByFilters_withNs(self):
    entities = self._create_entities(range(12), {}, "f")
    self._set_vals(entities, list(range(6)), list(range(2)))
    params = {
        "entity_kind": self.entity_kind,
        "namespace": "f",
        "filters": [("a", ">", 0),
                    ("a", "<=", 3),
                    ("b", "=", 1)],
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 2)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertEquals(2, len(results))
    self._assertEquals_splitInput(results[0], ["3", "5"])
    self._assertEquals_splitInput(results[1], ["7"])

  def testSplitInput_shardByFilters_noEntity(self):
    params = {
        "entity_kind": self.entity_kind,
        "namespace": "f",
        "filters": [("a", ">", 0), ("a", "<=", 3), ("b", "=", 1)]
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 100)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertEquals(3, len(results))
    self._assertEquals_splitInput(results[0], [])
    self._assertEquals_splitInput(results[1], [])
    self._assertEquals_splitInput(results[2], [])

  def testSplitInput_shardByFilters_bigShardNumber(self):
    entities = self._create_entities(range(12), {}, "f")
    self._set_vals(entities, list(range(6)), list(range(2)))
    params = {
        "entity_kind": self.entity_kind,
        "namespace": "f",
        "filters": [("a", ">", 0), ("a", "<=", 3), ("b", "=", 1)]
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 100)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertEquals(3, len(results))
    self._assertEquals_splitInput(results[0], ["3"])
    self._assertEquals_splitInput(results[1], ["5"])
    self._assertEquals_splitInput(results[2], ["7"])

  def testSplitInput_shardByFilters_lotsOfNS(self):
    """Lots means more than 2 in test cases."""
    entities = self._create_entities(range(12), {}, "f")
    self._set_vals(entities, list(range(6)), list(range(2)))
    entities = self._create_entities(range(12, 24), {}, "g")
    self._set_vals(entities, list(range(6)), list(range(2)))
    entities = self._create_entities(range(24, 36), {}, "h")
    self._set_vals(entities, list(range(6)), list(range(2)))
    entities = self._create_entities(range(36, 48), {}, "h")
    self._set_vals(entities, [0]*6, list(range(2)))

    params = {
        "entity_kind": self.entity_kind,
        "filters": [("a", ">", 0), ("a", "<=", 3), ("b", "=", 1)]
        }
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "InputReader",
        params, 100)
    results = self.reader_cls.split_input(mapper_spec)
    self.assertEquals(3, len(results))
    self._assertEquals_splitInput(results[0], ["3", "5", "7"])
    self._assertEquals_splitInput(results[1], ["15", "17", "19"])
    self._assertEquals_splitInput(results[2], ["27", "29", "31"])


class DatastoreInputReaderNdbTest(DatastoreInputReaderTest):

  @property
  def entity_kind(self):
    return "__main__.NdbTestEntity"

  def _create_entities(self,
                       keys_itr,
                       key_to_scatter_val,
                       ns=None,
                       entity_model_cls=NdbTestEntity):
    """Create ndb entities for tests.

    Args:
      keys_itr: an iterator that contains all the key names.
        Will be casted to str.
      key_to_scatter_val: a dict that maps key names to its scatter values.
      ns: the namespace to create the entity at.
      entity_model_cls: entity model class.

    Returns:
      A list of entities created.
    """
    set_scatter_setter(key_to_scatter_val)
    entities = []
    for i in keys_itr:
      k = ndb.Key(entity_model_cls._get_kind(), str(i), namespace=ns)
      entity = entity_model_cls(key=k)
      entities.append(entity)
      entity.put()
    return entities

  def _get_keyname(self, entity):
    return entity.key.id()


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

  def testSerialization(self):
    """Test json serialization."""
    # Create a single empty reader.
    readers = input_readers.ConsistentKeyReader.split_input(self.mapper_spec)
    self.assertEqual(1, len(readers))
    r = readers[0]
    # Serialize it.
    new_r = input_readers.ConsistentKeyReader.from_json(r.to_json())
    self.assertEqual(r.start_time_us, new_r.start_time_us)

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
    stub = logservice_stub.LogServiceStub()
    apiproxy_stub_map.apiproxy.RegisterStub("logservice", stub)

    # Write test data.
    expected = []
    for i in xrange(count):
      stub.start_request(request_id=i,
                         user_request_id="",
                         ip="127.0.0.1",
                         app_id=self.app_id,
                         version_id=self.version_id,
                         nickname="test@example.com",
                         user_agent="Chrome/15.0.874.106",
                         host="127.0.0.1:8080",
                         method="GET",
                         resource="/",
                         http_version="HTTP/1.1",
                         start_time=i * 1000000)
      stub.end_request(i, 200, 0, end_time=i * 1000000 + 500)
      expected.append({"start_time": i, "end_time": i + .0005})
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


class GoogleCloudStorageInputTestBase(testutil.CloudStorageTestBase):
  """Base class for running input tests with Google Cloud Storage.

  Subclasses must define READER_NAME and may redefine NUM_SHARDS.
  """

  # Defaults
  NUM_SHARDS = 10

  def create_mapper_spec(self, num_shards=None, input_params=None):
    """Create a Mapper specification using the GoogleCloudStorageInputReader.

    The specification generated uses a dummy handler and by default the
    number of shards is 10.

    Args:
      num_shards: optionally specify the number of shards.
      input_params: parameters for the input reader.

    Returns:
      a model.MapperSpec with default settings and specified input_params.
    """
    mapper_spec = model.MapperSpec(
        "DummyHandler",
        self.READER_NAME,
        {"input_reader": input_params or {}},
        num_shards or self.NUM_SHARDS)
    return mapper_spec

  def create_test_file(self, filename, content):
    """Create a test file with minimal content.

    Args:
      filename: the name of the file in the form "/bucket/object".
      content: the content to put in the file or if None a dummy string
        containing the filename will be used.
    """
    test_file = cloudstorage.open(filename, mode="w")
    test_file.write(content)
    test_file.close()


class GoogleCloudStorageInputReaderTest(GoogleCloudStorageInputTestBase):
  """Tests for GoogleCloudStorageInputReader."""

  READER_CLS = input_readers._GoogleCloudStorageInputReader
  READER_NAME = input_readers.__name__ + "." + READER_CLS.__name__

  def setUp(self):
    super(GoogleCloudStorageInputReaderTest, self).setUp()

    # create test content
    self.test_bucket = "testing"
    self.test_content = []
    self.test_num_files = 20
    for file_num in range(self.test_num_files):
      content = "Dummy Content %03d" % file_num
      self.test_content.append(content)
      self.create_test_file("/%s/file-%03d" % (self.test_bucket, file_num),
                            content)

  def testValidate_NoParams(self):
    self.assertRaises(
        errors.BadReaderParamsError,
        self.READER_CLS.validate,
        self.create_mapper_spec())

  def testValidate_NoBucket(self):
    self.assertRaises(
        errors.BadReaderParamsError,
        self.READER_CLS.validate,
        self.create_mapper_spec(input_params={"objects": ["1", "2", "3"]}))

  def testValidate_NoObjects(self):
    self.assertRaises(
        errors.BadReaderParamsError,
        self.READER_CLS.validate,
        self.create_mapper_spec(input_params={"bucket_name": self.test_bucket}))

  def testValidate_NonList(self):
    self.assertRaises(
        errors.BadReaderParamsError,
        self.READER_CLS.validate,
        self.create_mapper_spec(input_params={"bucket_name": self.test_bucket,
                                              "objects": "1"}))

  def testValidate_SingleObject(self):
    # expect no errors are raised
    self.READER_CLS.validate(
        self.create_mapper_spec(input_params={"bucket_name": self.test_bucket,
                                              "objects": ["1"]}))

  def testValidate_ObjectList(self):
    # expect no errors are raised
    self.READER_CLS.validate(
        self.create_mapper_spec(input_params={"bucket_name": self.test_bucket,
                                              "objects": ["1", "2", "3"]}))

  def testValidate_ObjectListNonString(self):
    self.assertRaises(
        errors.BadReaderParamsError,
        self.READER_CLS.validate,
        self.create_mapper_spec(input_params={"bucket_name": self.test_bucket,
                                              "objects": ["1", ["2", "3"]]}))

  def testSplit_SingleObjectSingleShard(self):
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.test_bucket,
                                              "objects": ["1"]}))
    self.assertEqual(1, len(readers))
    self.assertEqual(1, len(readers[0]._filenames))

  def testSplit_SingleObjectManyShards(self):
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=10,
                                input_params={"bucket_name": self.test_bucket,
                                              "objects": ["1"]}))
    self.assertEqual(1, len(readers))
    self.assertEqual(1, len(readers[0]._filenames))

  def testSplit_ManyObjectEvenlySplitManyShards(self):
    num_shards = 10
    files_per_shard = 3
    filenames = ["f-%d" % f for f in range(num_shards * files_per_shard)]
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=num_shards,
                                input_params={"bucket_name": self.test_bucket,
                                              "objects": filenames}))
    self.assertEqual(num_shards, len(readers))
    for reader in readers:
      self.assertEqual(files_per_shard, len(reader._filenames))

  def testSplit_ManyObjectUnevenlySplitManyShards(self):
    num_shards = 10
    total_files = int(10 * 2.33)
    filenames = ["f-%d" % f for f in range(total_files)]
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=num_shards,
                                input_params={"bucket_name": self.test_bucket,
                                              "objects": filenames}))
    self.assertEqual(num_shards, len(readers))
    found_files = 0
    for reader in readers:
      shard_num_files = len(reader._filenames)
      found_files += shard_num_files
      # ensure per-shard distribution is even
      min_files = math.floor(total_files / num_shards)
      max_files = min_files + 1
      self.assertTrue(min_files <= shard_num_files,
                      msg="Too few files (%d > %d) in reader: %s" %
                      (min_files, shard_num_files, reader))
      self.assertTrue(max_files >= shard_num_files,
                      msg="Too many files (%d < %d) in reader: %s" %
                      (max_files, shard_num_files, reader))
    self.assertEqual(total_files, found_files)

  def testSplit_Wildcard(self):
    # test prefix matching all files
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.test_bucket,
                                              "objects": ["file-*"]}))
    self.assertEqual(1, len(readers))
    self.assertEqual(self.test_num_files, len(readers[0]._filenames))

    # test prefix the first 10 (those with 00 prefix)
    self.assertTrue(self.test_num_files > 10,
                    msg="More than 10 files required for testing")
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.test_bucket,
                                              "objects": ["file-00*"]}))
    self.assertEqual(1, len(readers))
    self.assertEqual(10, len(readers[0]._filenames))

    # test prefix matching no files
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.test_bucket,
                                              "objects": ["badprefix*"]}))
    self.assertEqual(0, len(readers))

  def testNext(self):
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.test_bucket,
                                              "objects": ["file-*"]}))
    self.assertEqual(1, len(readers))
    reader_files = list(readers[0])
    self.assertEqual(self.test_num_files, len(reader_files))
    found_content = []
    for reader_file in reader_files:
      found_content.append(reader_file.read())
    self.assertEqual(len(self.test_content), len(found_content))
    for content in self.test_content:
      self.assertTrue(content in found_content)

  def testSerialization(self):
    readers = self.READER_CLS.split_input(
        self.create_mapper_spec(num_shards=1,
                                input_params={"bucket_name": self.test_bucket,
                                              "objects": ["file-*"]}))
    self.assertEqual(1, len(readers))
    # serialize/deserialize unused reader
    reader = self.READER_CLS.from_json(readers[0].to_json())

    found_content = []
    for _ in range(self.test_num_files):
      reader_file = reader.next()
      found_content.append(reader_file.read())
      # serialize/deserialize after each file is read
      reader = self.READER_CLS.from_json(reader.to_json())
    self.assertEqual(len(self.test_content), len(found_content))
    for content in self.test_content:
      self.assertTrue(content in found_content)

    # verify a reader at EOF still raises EOF after serialization
    self.assertRaises(StopIteration, reader.next)


class GoogleCloudStorageRecordInputReaderTest(GoogleCloudStorageInputTestBase):
  """Tests for GoogleCloudStorageInputReader."""

  READER_CLS = input_readers._GoogleCloudStorageRecordInputReader
  READER_NAME = input_readers.__name__ + "." + READER_CLS.__name__
  TEST_BUCKET = "testing"

  def create_test_file(self, filename, content):
    """Create a test LevelDB file with a RecordWriter.

    Args:
      filename: the name of the file in the form "/bucket/object".
      content: list of content to put in file in LevelDB format.
    """
    test_file = cloudstorage.open(filename, mode="w")
    with records.RecordsWriter(test_file) as w:
      for c in content:
        w.write(c)
    test_file.close()

  def testSingleFileNoRecord(self):
    filename = "/%s/empty-file" % self.TEST_BUCKET
    self.create_test_file(filename, [])
    reader = self.READER_CLS([filename])

    self.assertRaises(StopIteration, reader.next)

  def testSingleFileOneRecord(self):
    filename = "/%s/single-record-file" % self.TEST_BUCKET
    data = "foobardata"
    self.create_test_file(filename, [data])
    reader = self.READER_CLS([filename])

    self.assertEqual(data, reader.next())
    self.assertRaises(StopIteration, reader.next)

  def testSingleFileManyRecords(self):
    filename = "/%s/many-records-file" % self.TEST_BUCKET
    data = []
    for record_num in range(100):  # Make 100 records
      data.append(("%03d" % record_num) * 10)  # Make each record 30 chars long
    self.create_test_file(filename, data)
    reader = self.READER_CLS([filename])

    for record in data:
      self.assertEqual(record, reader.next())
    self.assertRaises(StopIteration, reader.next)
    # ensure StopIteration is still raised after its first encountered
    self.assertRaises(StopIteration, reader.next)

  def testManyFilesManyRecords(self):
    filenames = []
    all_data = []
    for file_num in range(10):  # Make 10 files
      filename = "/%s/file-%03d" % (self.TEST_BUCKET, file_num)
      data_set = []
      for record_num in range(10):  # Make 10 records, each 30 chars long
        data_set.append(("%03d" % record_num) * 10)
      self.create_test_file(filename, data_set)
      filenames.append(filename)
      all_data.append(data_set)
    reader = self.READER_CLS(filenames)

    for data_set in all_data:
      for record in data_set:
        self.assertEqual(record, reader.next())
    self.assertRaises(StopIteration, reader.next)

  def testManyFilesSomeEmpty(self):
    filenames = []
    all_data = []
    for file_num in range(50):  # Make 10 files
      filename = "/%s/file-%03d" % (self.TEST_BUCKET, file_num)
      data_set = []
      for record_num in range(file_num % 5):  # Make up to 4 records
        data_set.append(("%03d" % record_num) * 10)
      self.create_test_file(filename, data_set)
      filenames.append(filename)
      all_data.append(data_set)
    reader = self.READER_CLS(filenames)

    for data_set in all_data:
      for record in data_set:
        self.assertEqual(record, reader.next())
    self.assertRaises(StopIteration, reader.next)

  def testSerialization(self):
    filenames = []
    all_data = []
    for file_num in range(50):  # Make 10 files
      filename = "/%s/file-%03d" % (self.TEST_BUCKET, file_num)
      data_set = []
      for record_num in range(file_num % 5):  # Make up to 4 records
        data_set.append(("%03d" % record_num) * 10)
      self.create_test_file(filename, data_set)
      filenames.append(filename)
      all_data.append(data_set)
    reader = self.READER_CLS(filenames)

    # Serialize before using
    reader = self.READER_CLS.from_json(reader.to_json())

    for data_set in all_data:
      for record in data_set:
        self.assertEqual(record, reader.next())
        # Serialize after each read
        reader = self.READER_CLS.from_json(reader.to_json())
    self.assertRaises(StopIteration, reader.next)

    # Serialize after StopIteration reached
    reader = self.READER_CLS.from_json(reader.to_json())
    self.assertRaises(StopIteration, reader.next)

if __name__ == "__main__":
  unittest.main()
