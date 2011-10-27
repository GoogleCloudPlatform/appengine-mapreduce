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

import os
import types
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_errors
from google.appengine.api import datastore_file_stub
from google.appengine.ext import db
from mapreduce import control
from mapreduce import hooks
from mapreduce import model


class TestHandler(object):
  """Test handler class."""

  def __call__(self, entity):
    pass

  def process(self, entity):
    pass


class TestHandlerWithArgs(object):
  """Test handler with argument in constructor."""

  def __init__(self, arg_unused):
    """Constructor."""
    pass

  def process(self, entity):
    """Empty process function."""
    pass


class TestHooks(hooks.Hooks):
  """Test hooks class."""
  pass


def test_handler_function(entity):
  """Empty test handler function."""
  pass


class TestJsonType(object):
  """Test class with to_json/from_json methods."""

  def __init__(self, size=0):
    self.size = size

  def to_json(self):
    return {'size': self.size}

  @classmethod
  def from_json(cls, json):
    return cls(json['size'])


class EmptyDictJsonType(object):
  """Test class which serializes to empty json dict."""

  def to_json(self):
    return {}

  @classmethod
  def from_json(cls, json):
    return cls()


class TestEntity(db.Model):
  """Test entity class."""

  json_property = model.JsonProperty(TestJsonType)
  json_property_default_value = model.JsonProperty(
      TestJsonType, default=TestJsonType())
  empty_json_property = model.JsonProperty(EmptyDictJsonType)

ENTITY_KIND = '__main__.TestEntity'


class JsonPropertyTest(unittest.TestCase):
  """Test model.JsonProperty."""

  def testGetValueForDatastore(self):
    """Test get_value_for_datastore method."""
    e = TestEntity()
    self.assertEquals(None, TestEntity.json_property.get_value_for_datastore(e))
    e.json_property = TestJsonType(5)
    self.assertEquals(
        u'{"size": 5}', TestEntity.json_property.get_value_for_datastore(e))

    e.empty_json_property = EmptyDictJsonType()
    self.assertEquals(
        None, TestEntity.empty_json_property.get_value_for_datastore(e))

  def testMakeValueFromDatastore(self):
    """Test make_value_from_datastore method."""
    self.assertEquals(
        None, TestEntity.json_property.make_value_from_datastore(None))
    self.assertEquals(
        TestJsonType,
        type(TestEntity.json_property.make_value_from_datastore('{"size":4}')))
    self.assertTrue(
        4,
        TestEntity.json_property.make_value_from_datastore('{"size":4}').size)

  def testValidate(self):
    """Test validate method."""
    self.assertRaises(
        datastore_errors.BadValueError,
        TestEntity.json_property.validate, 'a')

  def testEmpty(self):
    """Test empty() method."""
    self.assertTrue(TestEntity.json_property.empty(None))
    self.assertFalse(TestEntity.json_property.empty('abcd'))

  def testDefaultValue(self):
    """Test default value."""
    e = TestEntity()
    self.assertEquals(None, e.json_property)
    self.assertTrue(e.json_property_default_value is not None)


class GetDescendingKeyTest(unittest.TestCase):
  """Tests the _get_descending_key function."""

  def testBasic(self):
    """Basic test of the function."""
    now = 1234567890
    os.environ["REQUEST_ID_HASH"] = "12345678"

    self.assertEquals(
        "159453012940012345678",
        model._get_descending_key(
            gettime=lambda: now))


class TestReader(object):
  pass


class TestWriter(object):
  pass


class MapperSpecTest(unittest.TestCase):
  """Tests model.MapperSpec."""

  TEST_HANDLER = __name__ + "." + TestHandler.__name__
  TEST_READER = __name__ + "." + TestReader.__name__
  TEST_WRITER = __name__ + "." + TestWriter.__name__

  def setUp(self):
    self.default_json = {
        "mapper_handler_spec": self.TEST_HANDLER,
        "mapper_input_reader": self.TEST_READER,
        "mapper_params": {"entity_kind": ENTITY_KIND},
        "mapper_shard_count": 8}

  def testToJson(self):
    mapper_spec = model.MapperSpec(
        self.TEST_HANDLER,
        self.TEST_READER,
        {"entity_kind": ENTITY_KIND},
        8)
    self.assertEquals(self.default_json,
                      mapper_spec.to_json())

    mapper_spec = model.MapperSpec(
        self.TEST_HANDLER,
        self.TEST_READER,
        {"entity_kind": ENTITY_KIND},
        8,
        output_writer_spec=self.TEST_WRITER)
    d = dict(self.default_json)
    d["mapper_output_writer"] = self.TEST_WRITER
    self.assertEquals(d, mapper_spec.to_json())

  def testFromJson(self):
    ms = model.MapperSpec.from_json(self.default_json)
    self.assertEquals(self.TEST_READER, ms.input_reader_spec)
    self.assertEquals(TestReader, ms.input_reader_class())
    self.assertEquals(self.default_json["mapper_input_reader"],
                      ms.input_reader_spec)
    self.assertEquals(self.TEST_HANDLER, ms.handler_spec)
    self.assertTrue(isinstance(ms.get_handler(), TestHandler))
    self.assertTrue(isinstance(ms.handler, TestHandler))
    self.assertEquals(8, ms.shard_count)

    d = dict(self.default_json)
    d["mapper_output_writer"] = self.TEST_WRITER
    ms = model.MapperSpec.from_json(d)
    self.assertEquals(self.TEST_WRITER, ms.output_writer_spec)
    self.assertEquals(TestWriter, ms.output_writer_class())


  def specForHandler(self, handler_spec):
    self.default_json["mapper_handler_spec"] = handler_spec
    return model.MapperSpec.from_json(self.default_json)

  def testClassHandler(self):
    """Test class name as handler spec."""
    mapper_spec = self.specForHandler(
        __name__ + "." + TestHandler.__name__)
    self.assertTrue(TestHandler,
                    type(mapper_spec.handler))

  def testInstanceMethodHandler(self):
    """Test instance method as handler spec."""
    mapper_spec = self.specForHandler(
        __name__ + "." + TestHandler.__name__ + ".process")
    self.assertEquals(types.MethodType,
                      type(mapper_spec.handler))
    # call it
    mapper_spec.handler(0)

  def testFunctionHandler(self):
    """Test function name as handler spec."""
    mapper_spec = self.specForHandler(
        __name__ + "." + test_handler_function.__name__)
    self.assertEquals(types.FunctionType,
                      type(mapper_spec.handler))
    # call it
    mapper_spec.handler(0)

  def testHandlerWithConstructorArgs(self):
    """Test class with constructor args as a handler."""
    mapper_spec = self.specForHandler(
        __name__ + "." + TestHandlerWithArgs.__name__)
    self.assertRaises(TypeError, mapper_spec.get_handler)

  def testMethodHandlerWithConstructorArgs(self):
    """Test method from a class with constructor args as a handler."""
    mapper_spec = self.specForHandler(
        __name__ + "." + TestHandlerWithArgs.__name__ + ".process")
    self.assertRaises(TypeError, mapper_spec.get_handler)


class MapreduceSpecTest(unittest.TestCase):
  """Tests model.MapreduceSpec."""

  def testToJson(self):
    """Test to_json method."""
    mapper_spec_dict = {"mapper_handler_spec": "TestHandler",
                        "mapper_input_reader": "TestInputReader",
                        "mapper_params": {"entity_kind": "bar"},
                        "mapper_shard_count": 8}
    mapreduce_spec = model.MapreduceSpec("my job",
                                         "mr0",
                                         mapper_spec_dict,
                                         {"extra": "value"},
                                         __name__+"."+TestHooks.__name__)
    self.assertEquals(
        {"name": "my job",
         "mapreduce_id": "mr0",
         "mapper_spec": mapper_spec_dict,
         "params": {"extra": "value"},
         "hooks_class_name": __name__+"."+TestHooks.__name__,
        },
        mapreduce_spec.to_json())

  def testFromJsonWithoutOptionalArgs(self):
    """Test from_json method without params and hooks_class_name present."""
    mapper_spec_dict = {"mapper_handler_spec": "TestHandler",
                        "mapper_input_reader": "TestInputReader",
                        "mapper_params": {"entity_kind": "bar"},
                        "mapper_shard_count": 8}
    mapreduce_spec = model.MapreduceSpec.from_json(
        {"mapper_spec": mapper_spec_dict,
         "mapreduce_id": "mr0",
         "name": "my job",
        })

    self.assertEquals("my job", mapreduce_spec.name)
    self.assertEquals("mr0", mapreduce_spec.mapreduce_id)
    self.assertEquals(mapper_spec_dict, mapreduce_spec.mapper.to_json())
    self.assertEquals("TestHandler", mapreduce_spec.mapper.handler_spec)
    self.assertEquals(None, mapreduce_spec.params)
    self.assertEquals(None, mapreduce_spec.hooks_class_name)

  def testFromJsonWithOptionalArgs(self):
    """Test from_json method with params and hooks_class_name present."""
    mapper_spec_dict = {"mapper_handler_spec": "TestHandler",
                        "mapper_input_reader": "TestInputReader",
                        "mapper_params": {"entity_kind": "bar"},
                        "mapper_shard_count": 8}
    mapreduce_spec = model.MapreduceSpec.from_json(
        {"mapper_spec": mapper_spec_dict,
         "mapreduce_id": "mr0",
         "name": "my job",
         "params": {"extra": "value"},
         "hooks_class_name": __name__+"."+TestHooks.__name__
        })

    self.assertEquals("my job", mapreduce_spec.name)
    self.assertEquals("mr0", mapreduce_spec.mapreduce_id)
    self.assertEquals(mapper_spec_dict, mapreduce_spec.mapper.to_json())
    self.assertEquals("TestHandler", mapreduce_spec.mapper.handler_spec)
    self.assertEquals({"extra": "value"}, mapreduce_spec.params)
    self.assertEquals(__name__+"."+TestHooks.__name__,
                      mapreduce_spec.hooks_class_name)
    self.assertEquals(mapreduce_spec, mapreduce_spec.get_hooks().mapreduce_spec)


class MapreduceStateTest(unittest.TestCase):
  """Tests model.MapreduceState."""

  def testSetProcessedCounts(self):
    """Test set_processed_counts method."""
    mapreduce_state = model.MapreduceState.create_new()
    mapreduce_state.set_processed_counts([1, 2])
    self.assertEquals(
        "http://chart.apis.google.com/chart?chd=s%3AD6&chco=0000ff&chbh=a&"
        "chs=300x200&cht=bvg", mapreduce_state.chart_url)


class ShardStateTest(unittest.TestCase):
  """Tests model.ShardState."""

  def setUp(self):
    os.environ["APPLICATION_ID"] = "my-app"

  def tearDown(self):
    del os.environ["APPLICATION_ID"]

  def testAccessors(self):
    """Tests simple accessors."""
    shard = model.ShardState.create_new("my-map-job1", 14)
    self.assertEquals(14, shard.shard_number)


class CountersMapTest(unittest.TestCase):
  """Tests model.CountersMap."""

  def testIncrementCounter(self):
    """Test increment_counter method."""
    countres_map = model.CountersMap()

    self.assertEquals(0, countres_map.get("counter1"))
    self.assertEquals(10, countres_map.increment("counter1", 10))
    self.assertEquals(10, countres_map.get("counter1"))
    self.assertEquals(20, countres_map.increment("counter1", 10))
    self.assertEquals(20, countres_map.get("counter1"))

  def testAddSubMap(self):
    """Test add_map and sub_map methods."""
    map1 = model.CountersMap()
    map1.increment("1", 5)
    map1.increment("2", 7)

    map2 = model.CountersMap()
    map2.increment("2", 8)
    map2.increment("3", 11)

    map1.add_map(map2)

    self.assertEquals(5, map1.get("1"))
    self.assertEquals(15, map1.get("2"))
    self.assertEquals(11, map1.get("3"))

    map1.sub_map(map2)

    self.assertEquals(5, map1.get("1"))
    self.assertEquals(7, map1.get("2"))
    self.assertEquals(0, map1.get("3"))

  def testToJson(self):
    """Test to_json method."""
    counters_map = model.CountersMap()
    counters_map.increment("1", 5)
    counters_map.increment("2", 7)

    self.assertEquals({"counters": {"1": 5, "2": 7}}, counters_map.to_json())

  def testFromJson(self):
    """Test from_json method."""
    counters_map = model.CountersMap()
    counters_map.increment("1", 5)
    counters_map.increment("2", 7)

    counters_map = model.CountersMap.from_json(counters_map.to_json())

    self.assertEquals(5, counters_map.get("1"))
    self.assertEquals(7, counters_map.get("2"))

  def testClear(self):
    """Test clear method."""
    counters_map = model.CountersMap()
    counters_map.increment("1", 5)
    counters_map.clear()

    self.assertEquals(0, counters_map.get("1"))


if __name__ == '__main__':
  unittest.main()
