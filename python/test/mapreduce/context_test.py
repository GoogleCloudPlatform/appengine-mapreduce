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




from testlib import mox
import unittest

from google.appengine.ext import ndb
import unittest
from google.appengine.api import datastore
from google.appengine.ext import db
from mapreduce import context
from testlib import testutil


class TestEntity(db.Model):
  """Test entity class to test db operations."""

  tag = db.TextProperty()


class NdbTestEntity(ndb.Model):
  """Test entity class to test ndb operations."""

  tag = ndb.TextProperty()


def new_datastore_entity(key_name=None):
  return datastore.Entity('TestEntity', name=key_name)


class ItemListTest(unittest.TestCase):
  """Tests for context.ItemList class."""

  def setUp(self):
    self.list = context.ItemList()

  def testAppend(self):
    """Test append method."""
    self.assertEquals([], self.list.items)
    self.assertEquals(0, self.list.size)

    self.list.append('abc', 100)

    self.assertEquals(['abc'], self.list.items)
    self.assertEquals(100, self.list.size)

  def testClear(self):
    """Test clear method."""
    self.list.append('abc', 100)
    self.assertEquals(['abc'], self.list.items)
    self.assertEquals(100, self.list.size)

    self.list.clear()

    self.assertEquals([], self.list.items)
    self.assertEquals(0, self.list.size)

  def testBackwardsCompat(self):
    """Old class name and 'entities' property should still work."""
    self.list = context.EntityList()
    self.list.append('abc', 100)
    self.assertEquals(['abc'], self.list.entities)
    self.assertEquals(self.list.items, self.list.entities)
    self.assertEquals(100, self.list.size)


class MutationPoolTest(testutil.HandlerTestBase):
  """Tests for context.MutationPool class."""

  def setUp(self):
    super(MutationPoolTest, self).setUp()
    self.pool = context.MutationPool()

  def record_put(self, entities, force_writes=False):
    datastore.Put(
        entities,
        config=testutil.MatchesDatastoreConfig(
            deadline=context.DATASTORE_DEADLINE,
            force_writes=force_writes))

  def record_delete(self, entities, force_writes=False):
    datastore.Delete(
        entities,
        config=testutil.MatchesDatastoreConfig(
            deadline=context.DATASTORE_DEADLINE,
            force_writes=force_writes))

  def testPoolWithForceWrites(self):
    class MapreduceSpec:
      def __init__(self):
        self.params = {'force_ops_writes':True}
    pool = context.MutationPool(mapreduce_spec=MapreduceSpec())
    m = mox.Mox()
    m.StubOutWithMock(datastore, 'Put', use_mock_anything=True)
    m.StubOutWithMock(datastore, 'Delete', use_mock_anything=True)
    e1 = TestEntity()
    e2 = TestEntity(key_name='key2')
    self.record_put([e1._populate_internal_entity()], True)
    self.record_delete([e2.key()], True)
    m.ReplayAll()
    try:  # test, verify
      pool.put(e1)
      pool.delete(e2.key())
      self.assertEquals([e1._populate_internal_entity()], pool.puts.items)
      self.assertEquals([e2.key()], pool.deletes.items)
      pool.flush()
      m.VerifyAll()
    finally:
      m.UnsetStubs()

  def testPut(self):
    """Test put method."""
    e = TestEntity()
    self.pool.put(e)
    self.assertEquals([e._populate_internal_entity()], self.pool.puts.items)
    self.assertEquals([], self.pool.deletes.items)

  def testPutWithForceWrite(self):
    e = TestEntity()
    self.pool.put(e)
    self.assertEquals([e._populate_internal_entity()], self.pool.puts.items)
    self.assertEquals([], self.pool.deletes.items)

  def testPutNdbEntity(self):
    """Test put() using an NDB entity."""
    e = NdbTestEntity()
    self.pool.put(e)
    self.assertEquals([e], self.pool.ndb_puts.items)
    self.assertEquals([], self.pool.ndb_deletes.items)
    self.assertEquals([], self.pool.puts.items)
    self.assertEquals([], self.pool.deletes.items)

  def testPutEntity(self):
    """Test put method using a datastore Entity directly."""
    e = new_datastore_entity()
    self.pool.put(e)
    self.assertEquals([e], self.pool.puts.items)
    self.assertEquals([], self.pool.deletes.items)
    # Mix in a model instance.
    e2 = TestEntity()
    self.pool.put(e2)
    self.assertEquals([e, e2._populate_internal_entity()], self.pool.puts.items)

  def testDelete(self):
    """Test delete method with a model instance."""
    e = TestEntity(key_name='goingaway')
    self.pool.delete(e)
    self.assertEquals([], self.pool.puts.items)
    self.assertEquals([e.key()], self.pool.deletes.items)

  def testDeleteNdbEntity(self):
    """Test delete method with an NDB model instance."""
    e = NdbTestEntity(id='goingaway')
    self.pool.delete(e)
    self.assertEquals([], self.pool.ndb_puts.items)
    self.assertEquals([e.key], self.pool.ndb_deletes.items)
    self.assertEquals([], self.pool.puts.items)
    self.assertEquals([], self.pool.deletes.items)

  def testDeleteNdbKey(self):
    """Test delete method with an NDB key."""
    e = NdbTestEntity(id='goingaway')
    self.pool.delete(e.key)
    self.assertEquals([], self.pool.ndb_puts.items)
    self.assertEquals([e.key], self.pool.ndb_deletes.items)
    self.assertEquals([], self.pool.puts.items)
    self.assertEquals([], self.pool.deletes.items)

  def testDeleteEntity(self):
    """Test delete method with a datastore entity"""
    e = new_datastore_entity(key_name='goingaway')
    self.pool.delete(e)
    self.assertEquals([], self.pool.puts.items)
    self.assertEquals([e.key()], self.pool.deletes.items)

  def testDeleteKey(self):
    """Test delete method with a key instance."""
    k = db.Key.from_path('MyKind', 'MyKeyName', _app='myapp')
    self.pool.delete(k)
    self.assertEquals([], self.pool.puts.items)
    self.assertEquals([k], self.pool.deletes.items)
    # String of keys should work too. No, there's no collapsing of dupes.
    self.pool.delete(str(k))
    self.assertEquals([k, k], self.pool.deletes.items)

  def testPutOverPoolSize(self):
    """Test putting more than pool size."""
    self.pool = context.MutationPool(1000)

    m = mox.Mox()
    m.StubOutWithMock(datastore, 'Put', use_mock_anything=True)

    e1 = TestEntity()
    e2 = TestEntity(tag=' ' * 1000)

    # Record Calls
    self.record_put([e1._populate_internal_entity()])

    m.ReplayAll()
    try:  # test, verify
      self.pool.put(e1)
      self.assertEquals([e1._populate_internal_entity()], self.pool.puts.items)

      self.pool.put(e2)
      self.assertEquals([e2._populate_internal_entity()], self.pool.puts.items)

      m.VerifyAll()
    finally:
      m.UnsetStubs()

  def testPutTooManyEntities(self):
    """Test putting more than allowed entity count."""
    max_entity_count = 100
    self.pool = context.MutationPool(max_entity_count=max_entity_count)

    m = mox.Mox()
    m.StubOutWithMock(datastore, 'Put', use_mock_anything=True)

    entities = []
    for i in range(max_entity_count + 50):
      entities.append(TestEntity())

    # Record Calls
    self.record_put([e._populate_internal_entity()
                     for e in entities[:max_entity_count]])

    m.ReplayAll()
    try:  # test, verify
      for e in entities:
        self.pool.put(e)

      # only 50 entities should be left.
      self.assertEquals(50, self.pool.puts.length)

      m.VerifyAll()
    finally:
      m.UnsetStubs()

  def testDeleteOverPoolSize(self):
    """Test deleting more than pool size."""
    self.pool = context.MutationPool(500)

    m = mox.Mox()
    m.StubOutWithMock(datastore, 'Delete', use_mock_anything=True)

    e1 = TestEntity(key_name='goingaway')
    e2 = TestEntity(key_name='x' * 500)

    # Record Calls
    self.record_delete([e1.key()])

    m.ReplayAll()
    try:  # test, verify
      self.pool.delete(e1)
      self.assertEquals([e1.key()], self.pool.deletes.items)

      self.pool.delete(e2)
      self.assertEquals([e2.key()], self.pool.deletes.items)

      m.VerifyAll()
    finally:
      m.UnsetStubs()

  def testDeleteTooManyEntities(self):
    """Test putting more than allowed entity count."""
    max_entity_count = context.MAX_ENTITY_COUNT
    self.pool = context.MutationPool() # default size is MAX_ENTITY_COUNT

    m = mox.Mox()
    m.StubOutWithMock(datastore, 'Delete', use_mock_anything=True)

    entities = []
    for i in range(max_entity_count + 50):
      entities.append(TestEntity(key_name='die%d' % i))

    # Record Calls
    self.record_delete([e.key() for e in entities[:max_entity_count]])

    m.ReplayAll()
    try:  # test, verify
      for e in entities:
        self.pool.delete(e)

      # only 50 entities should be left.
      self.assertEquals(50, self.pool.deletes.length)

      m.VerifyAll()
    finally:
      m.UnsetStubs()

  def testFlush(self):
    """Test flush method."""
    self.pool = context.MutationPool(1000)

    m = mox.Mox()
    m.StubOutWithMock(datastore, 'Delete', use_mock_anything=True)
    m.StubOutWithMock(datastore, 'Put', use_mock_anything=True)

    e1 = TestEntity()
    e2 = TestEntity(key_name='flushme')

    # Record Calls
    self.record_put([e1._populate_internal_entity()])
    self.record_delete([e2.key()])

    m.ReplayAll()
    try:  # test, verify
      self.pool.put(e1)
      self.assertEquals([e1._populate_internal_entity()], self.pool.puts.items)

      self.pool.delete(e2)
      self.assertEquals([e2.key()], self.pool.deletes.items)

      self.pool.flush()

      self.assertEquals([], self.pool.puts.items)
      self.assertEquals([], self.pool.deletes.items)

      m.VerifyAll()
    finally:
      m.UnsetStubs()

  def testNdbFlush(self):
    # Combined test for all NDB implicit and explicit flushing.
    self.pool = context.MutationPool(max_pool_size=500, max_entity_count=3)

    for i in range(8):
      self.pool.put(NdbTestEntity())
      self.assertEquals(len(self.pool.ndb_puts.items), (i%3) + 1)

    for i in range(5):
      self.pool.delete(ndb.Key(NdbTestEntity, 'x%d' % i))
      self.assertEquals(len(self.pool.ndb_deletes.items), (i%3) + 1)

    self.pool.put(NdbTestEntity(tag='x'*500))
    self.assertEquals(len(self.pool.ndb_puts.items), 1)  # Down from 2

    self.pool.delete(ndb.Key(NdbTestEntity, 'x'*500))
    self.assertEquals(len(self.pool.ndb_deletes.items), 1)  # Down from 2

    self.pool.flush()
    self.assertEquals(len(self.pool.ndb_puts.items), 0)
    self.assertEquals(len(self.pool.ndb_deletes.items), 0)


class CountersTest(unittest.TestCase):
  """Test for context.Counters class."""

  def testIncrement(self):
    """Test increment() method."""
    m = mox.Mox()

    # Set up mocks
    shard_state = m.CreateMockAnything()
    counters_map = m.CreateMockAnything()
    shard_state.counters_map = counters_map
    counters = context.Counters(shard_state)

    # Record call
    counters_map.increment('test', 19)

    m.ReplayAll()
    try:  # test, verify
      counters.increment('test', 19)

      m.VerifyAll()
    finally:
      m.UnsetStubs()

  def testFlush(self):
    """Test flush() method."""
    counters = context.Counters(None)
    counters.flush()


class ContextTest(testutil.HandlerTestBase):
  """Test for context.Context class."""

  def testGetSetContext(self):
    """Test module's get_context and _set functions."""
    ctx = context.Context(None, None)
    self.assertFalse(context.get())
    context.Context._set(ctx)
    self.assertEquals(ctx, context.get())
    context.Context._set(None)
    self.assertEquals(None, context.get())

  def testArbitraryPool(self):
    """Test arbitrary pool registration."""
    m = mox.Mox()

    ctx = context.Context(None, None)
    self.assertFalse(ctx.get_pool("test"))
    pool = m.CreateMockAnything()
    ctx.register_pool("test", pool)
    self.assertEquals(pool, ctx.get_pool("test"))

    # Record calls
    pool.flush()

    m.ReplayAll()
    try:  # test, verify
      ctx.flush()
      m.VerifyAll()
    finally:
      m.UnsetStubs()

  def testMutationPoolSize(self):
    ctx = context.Context(None, None)
    self.assertEquals(context.MAX_ENTITY_COUNT,
                      ctx.mutation_pool.max_entity_count)
    self.assertEquals(context.MAX_POOL_SIZE,
                      ctx.mutation_pool.max_pool_size)

    ctx = context.Context(None, None, task_retry_count=0)
    self.assertEquals(context.MAX_ENTITY_COUNT,
                      ctx.mutation_pool.max_entity_count)
    self.assertEquals(context.MAX_POOL_SIZE,
                      ctx.mutation_pool.max_pool_size)

    ctx = context.Context(None, None, task_retry_count=1)
    self.assertEquals(context.MAX_ENTITY_COUNT / 2,
                      ctx.mutation_pool.max_entity_count)
    self.assertEquals(context.MAX_POOL_SIZE / 2,
                      ctx.mutation_pool.max_pool_size)

    ctx = context.Context(None, None, task_retry_count=4)
    self.assertEquals(context.MAX_ENTITY_COUNT / 16,
                      ctx.mutation_pool.max_entity_count)
    self.assertEquals(context.MAX_POOL_SIZE / 16,
                      ctx.mutation_pool.max_pool_size)


if __name__ == "__main__":
  unittest.main()
