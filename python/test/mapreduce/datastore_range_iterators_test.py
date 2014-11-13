#!/usr/bin/env python




# pylint: disable=g-bad-name

import os
import unittest
from google.appengine.ext import ndb

from google.appengine.api import datastore
from google.appengine.api import namespace_manager
from google.appengine.ext import db
from google.appengine.ext import key_range
from google.appengine.ext import testbed
from mapreduce import datastore_range_iterators as db_iters
from mapreduce import key_ranges
from mapreduce import model
from mapreduce import namespace_range
from mapreduce import property_range
from mapreduce import util


class TestEntity(db.Model):
  bar = db.IntegerProperty()


class NdbTestEntity(ndb.Model):
  bar = ndb.IntegerProperty()


class IteratorTest(unittest.TestCase):

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.app = "foo"
    os.environ["APPLICATION_ID"] = self.app
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    self.namespaces = [str(i) for i in range(5)]
    self.filters = [("bar", "=", 0)]
    self.key_names = [str(i) for i in range(10)]
    self.bar_values = [0]*5 + list(range(1, 6))

  def tearDown(self):
    self.testbed.deactivate()

  def _create_entities(self):
    """In each ns create 10 entities with bar values."""
    entities = []
    old_ns = namespace_manager.get_namespace()
    for namespace in self.namespaces:
      namespace_manager.set_namespace(namespace)
      for key_name, bar in zip(self.key_names, self.bar_values):
        entity = TestEntity(key_name=key_name, bar=bar)
        entities.append(entity)
        entity.put()
    namespace_manager.set_namespace(old_ns)
    return entities

  def _create_ndb_entities(self):
    entities = []
    for namespace in self.namespaces:
      for key_name, bar in zip(self.key_names, self.bar_values):
        key = ndb.Key("NdbTestEntity", key_name, namespace=namespace)
        entity = NdbTestEntity(key=key, bar=bar)
        entities.append(entity)
        entity.put()
    return entities

  def _serializeAndAssertEquals(self, itr, expected, get_key):
    """AssertEquals helper.

    Consume iterator, serialize and deserialize on every next call,
    check returned values are expected.

    Args:
      itr: a serializable object that implement __iter__.
      expected: a list of expected keys.
      get_key: a function that is applied to every iterator returned value
        to get entity key.
    """
    # Perform an initial serialization before you iterate.
    itr = itr.__class__.from_json_str(itr.to_json_str())
    results = []
    for _ in expected:
      key = get_key(iter(itr).next())
      results.append(key)
      # Convert to str to test everything is indeed json compatible.
      itr = itr.__class__.from_json_str(itr.to_json_str())
      # Convert twice to check consistency.
      itr = itr.__class__.from_json_str(itr.to_json_str())
    results.sort()
    self.assertEquals(expected, results)
    self.assertRaises(StopIteration, iter(itr).next)

  def _AssertEquals(self, itr, expected, get_key):
    """AssertEquals helper.

    Consume iterator without serialization.
    Check returned values are expected.

    Args:
      itr: a serializable object that implement __iter__.
      expected: a list of expected keys.
      get_key: a function that is applied to every iterator returned value
        to get entity key.
    """
    results = [get_key(i) for i in itr]
    results.sort()
    self.assertEquals(expected, results)


class PropertyRangeIteratorTest(IteratorTest):

  def setUp(self):
    super(PropertyRangeIteratorTest, self).setUp()
    self.filters = [("bar", "<=", 4), ("bar", ">", 2)]

  def _create_iter(self, entity_kind):
    query_spec = model.QuerySpec(entity_kind=util.get_short_name(entity_kind),
                                 batch_size=10,
                                 filters=self.filters,
                                 model_class_path=entity_kind)
    p_range = property_range.PropertyRange(self.filters,
                                           entity_kind)
    ns_range = namespace_range.NamespaceRange(self.namespaces[0],
                                              self.namespaces[-1])
    itr = db_iters.RangeIteratorFactory.create_property_range_iterator(
        p_range, ns_range, query_spec)
    return itr

  def testE2e(self):
    entities = self._create_entities()
    expected = [e.key() for e in entities if e.bar in [3, 4]]
    expected.sort()

    itr = self._create_iter("__main__.TestEntity")
    self._serializeAndAssertEquals(itr, expected, lambda e: e.key())
    itr = self._create_iter("__main__.TestEntity")
    self._AssertEquals(itr, expected, lambda e: e.key())

  def testE2eNdb(self):
    entities = self._create_ndb_entities()
    expected = [e.key for e in entities if e.bar in [3, 4]]
    expected.sort()

    itr = self._create_iter("__main__.NdbTestEntity")
    self._serializeAndAssertEquals(itr, expected, lambda e: e.key)
    itr = self._create_iter("__main__.NdbTestEntity")
    self._AssertEquals(itr, expected, lambda e: e.key)


class KeyRangesIteratorTest(IteratorTest):
  """Tests for db_iters._KeyRangesIterator."""

  def _create_iter(self, iter_cls, entity_kind):
    kranges = [key_range.KeyRange(namespace=ns) for ns in self.namespaces]
    kranges = key_ranges.KeyRangesFactory.create_from_list(kranges)
    query_spec = model.QuerySpec(entity_kind=util.get_short_name(entity_kind),
                                 batch_size=10,
                                 filters=self.filters,
                                 model_class_path=entity_kind)
    itr = db_iters.RangeIteratorFactory.create_key_ranges_iterator(
        kranges, query_spec, iter_cls)
    return itr

  def testE2e(self):
    entities = self._create_entities()
    expected = [e.key() for e in entities if e.bar == 0]
    expected.sort()

    itr = self._create_iter(db_iters.KeyRangeModelIterator,
                            "__main__.TestEntity")
    self._serializeAndAssertEquals(itr, expected, lambda e: e.key())
    itr = self._create_iter(db_iters.KeyRangeModelIterator,
                            "__main__.TestEntity")
    self._AssertEquals(itr, expected, lambda e: e.key())

    itr = self._create_iter(db_iters.KeyRangeEntityIterator,
                            "TestEntity")
    self._serializeAndAssertEquals(itr, expected, lambda e: e.key())
    itr = self._create_iter(db_iters.KeyRangeEntityIterator,
                            "TestEntity")
    self._AssertEquals(itr, expected, lambda e: e.key())

    itr = self._create_iter(db_iters.KeyRangeKeyIterator,
                            "TestEntity")
    self._serializeAndAssertEquals(itr, expected, lambda e: e)
    itr = self._create_iter(db_iters.KeyRangeKeyIterator,
                            "TestEntity")
    self._AssertEquals(itr, expected, lambda e: e)

  def testE2eNdb(self):
    entities = self._create_ndb_entities()
    expected = [e.key for e in entities if e.bar == 0]
    expected.sort()

    itr = self._create_iter(db_iters.KeyRangeModelIterator,
                            "__main__.NdbTestEntity")
    self._serializeAndAssertEquals(itr, expected, lambda e: e.key)

    itr = self._create_iter(db_iters.KeyRangeModelIterator,
                            "__main__.NdbTestEntity")
    self._AssertEquals(itr, expected, lambda e: e.key)


class KeyRangeIteratorTestBase(IteratorTest):
  """Base class for db_iters.KeyRange*Iterator tests."""

  def setUp(self):
    super(KeyRangeIteratorTestBase, self).setUp()
    self.namespace = "foo_ns"
    self.namespaces = [self.namespace]

  def _create_iter(self, iter_cls, entity_kind):
    key_start = db.Key.from_path(util.get_short_name(entity_kind),
                                 "0",
                                 namespace=self.namespace)
    key_end = db.Key.from_path(util.get_short_name(entity_kind),
                               "999",
                               namespace=self.namespace)
    krange = key_range.KeyRange(key_start,
                                key_end,
                                include_start=True,
                                include_end=True,
                                namespace=self.namespace)

    query_spec = model.QuerySpec(entity_kind=util.get_short_name(entity_kind),
                                 batch_size=10,
                                 filters=self.filters,
                                 model_class_path=entity_kind)
    return iter_cls(krange, query_spec)


class KeyRangeModelIteratorTest(KeyRangeIteratorTestBase):
  """Tests for KeyRangeModelIterator."""

  def testE2e(self):
    entities = self._create_entities()
    expected = [e.key() for e in entities if e.bar == 0]
    expected.sort()

    itr = self._create_iter(db_iters.KeyRangeModelIterator,
                            "__main__.TestEntity")
    self._serializeAndAssertEquals(itr, expected, lambda e: e.key())
    itr = self._create_iter(db_iters.KeyRangeModelIterator,
                            "__main__.TestEntity")
    self._AssertEquals(itr, expected, lambda e: e.key())

  def testE2eNdb(self):
    entities = self._create_ndb_entities()
    expected = [e.key for e in entities if e.bar == 0]
    expected.sort()

    itr = self._create_iter(db_iters.KeyRangeModelIterator,
                            "__main__.NdbTestEntity")
    self._serializeAndAssertEquals(itr, expected, lambda e: e.key)
    itr = self._create_iter(db_iters.KeyRangeModelIterator,
                            "__main__.NdbTestEntity")
    self._AssertEquals(itr, expected, lambda e: e.key)


class KeyRangeRawEntityIteratorTest(KeyRangeIteratorTestBase):
  """Tests for KeyRangeEntityIterator and KeyRangeKeyIterator."""

  def testE2e(self):
    entities = self._create_entities()
    expected = [e.key() for e in entities if e.bar == 0]
    expected.sort()

    def get_key(item):
      # Test the returned value is low level datastore API's Entity.
      self.assertTrue(isinstance(item, datastore.Entity))
      return item.key()
    itr = self._create_iter(db_iters.KeyRangeEntityIterator, "TestEntity")
    self._serializeAndAssertEquals(itr, expected, get_key)
    itr = self._create_iter(db_iters.KeyRangeEntityIterator, "TestEntity")
    self._AssertEquals(itr, expected, get_key)

    itr = self._create_iter(db_iters.KeyRangeKeyIterator, "TestEntity")
    self._serializeAndAssertEquals(itr, expected, lambda e: e)
    itr = self._create_iter(db_iters.KeyRangeKeyIterator, "TestEntity")
    self._AssertEquals(itr, expected, lambda e: e)


class KeyRangeEntityProtoIteratorTest(KeyRangeIteratorTestBase):
  """Tests for KeyRangeEntityProtoIterator."""

  def testE2e(self):
    entities = self._create_entities()
    expected = [e.key() for e in entities if e.bar == 0]
    expected.sort()

    def get_key(item):
      return datastore.Entity._FromPb(item).key()
    itr = self._create_iter(db_iters.KeyRangeEntityProtoIterator, "TestEntity")
    self._serializeAndAssertEquals(itr, expected, get_key)
    itr = self._create_iter(db_iters.KeyRangeEntityProtoIterator, "TestEntity")
    self._AssertEquals(itr, expected, get_key)


if __name__ == "__main__":
  unittest.main()
