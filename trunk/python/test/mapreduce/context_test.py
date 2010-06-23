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

"""Tests for google.appengine.ext.mapreduce.context."""


import google

from testlib import mox
import os

import unittest
from google.appengine.ext import db
from mapreduce import context


class TestEntity(db.Model):
  """Test entity class to test db operations."""

  tag = db.TextProperty()


class EntityListTest(unittest.TestCase):
  """Tests for context.EntityList class."""

  def setUp(self):
    self.list = context.EntityList()

  def testAppend(self):
    """Test append method."""
    self.assertEquals([], self.list.entities)
    self.assertEquals(0, self.list.size)

    self.list.append('abc', 100)

    self.assertEquals(['abc'], self.list.entities)
    self.assertEquals(100, self.list.size)

  def testClear(self):
    """Test clear method."""
    self.list.append('abc', 100)
    self.assertEquals(['abc'], self.list.entities)
    self.assertEquals(100, self.list.size)

    self.list.clear()

    self.assertEquals([], self.list.entities)
    self.assertEquals(0, self.list.size)


class MutationPoolTest(unittest.TestCase):
  """Tests for context.MutationPool class."""

  def setUp(self):
    self.appid = 'testapp'
    os.environ['APPLICATION_ID'] = self.appid
    self.pool = context.MutationPool()

  def testPut(self):
    """Test put method."""
    e = TestEntity()
    self.pool.put(e)
    self.assertEquals([e], self.pool.puts.entities)
    self.assertEquals([], self.pool.deletes.entities)

  def testDelete(self):
    """Test delete method."""
    e = TestEntity()
    self.pool.delete(e)
    self.assertEquals([], self.pool.puts.entities)
    self.assertEquals([e], self.pool.deletes.entities)

  def testPutOverPoolSize(self):
    """Test putting more than pool size."""
    self.pool = context.MutationPool(1000)

    m = mox.Mox()
    m.StubOutWithMock(db, 'put', use_mock_anything=True)

    e1 = TestEntity()
    e2 = TestEntity(tag=' ' * 1000)

    db.put([e1])

    m.ReplayAll()
    try:
      self.pool.put(e1)
      self.assertEquals([e1], self.pool.puts.entities)

      self.pool.put(e2)
      self.assertEquals([e2], self.pool.puts.entities)

      m.VerifyAll()
    finally:
      m.UnsetStubs()

  def testPutTooManyEntities(self):
    """Test putting more than allowed entity count."""
    self.pool = context.MutationPool()

    m = mox.Mox()
    m.StubOutWithMock(db, 'put', use_mock_anything=True)

    entities = []
    for i in range(context.MAX_ENTITY_COUNT + 50):
      entities.append(TestEntity())

    db.put(entities[:context.MAX_ENTITY_COUNT])

    m.ReplayAll()
    try:
      for e in entities:
        self.pool.put(e)

      self.assertEquals(50, self.pool.puts.length)

      m.VerifyAll()
    finally:
      m.UnsetStubs()

  def testDeleteOverPoolSize(self):
    """Test deleting more than pool size."""
    self.pool = context.MutationPool(1000)

    m = mox.Mox()
    m.StubOutWithMock(db, 'delete', use_mock_anything=True)

    e1 = TestEntity()
    e2 = TestEntity(tag=' ' * 1000)

    db.delete([e1])

    m.ReplayAll()
    try:
      self.pool.delete(e1)
      self.assertEquals([e1], self.pool.deletes.entities)

      self.pool.delete(e2)
      self.assertEquals([e2], self.pool.deletes.entities)

      m.VerifyAll()
    finally:
      m.UnsetStubs()

  def testDeleteTooManyEntities(self):
    """Test putting more than allowed entity count."""
    self.pool = context.MutationPool()

    m = mox.Mox()
    m.StubOutWithMock(db, 'delete', use_mock_anything=True)

    entities = []
    for i in range(context.MAX_ENTITY_COUNT + 50):
      entities.append(TestEntity())

    db.delete(entities[:context.MAX_ENTITY_COUNT])

    m.ReplayAll()
    try:
      for e in entities:
        self.pool.delete(e)

      self.assertEquals(50, self.pool.deletes.length)

      m.VerifyAll()
    finally:
      m.UnsetStubs()

  def testFlush(self):
    """Test flush method."""
    self.pool = context.MutationPool(1000)

    m = mox.Mox()
    m.StubOutWithMock(db, 'delete', use_mock_anything=True)
    m.StubOutWithMock(db, 'put', use_mock_anything=True)

    e1 = TestEntity()
    e2 = TestEntity()

    db.put([e1])
    db.delete([e2])

    m.ReplayAll()
    try:
      self.pool.put(e1)
      self.assertEquals([e1], self.pool.puts.entities)

      self.pool.delete(e2)
      self.assertEquals([e2], self.pool.deletes.entities)

      self.pool.flush()

      self.assertEquals([], self.pool.puts.entities)
      self.assertEquals([], self.pool.deletes.entities)

      m.VerifyAll()
    finally:
      m.UnsetStubs()


class CountersTest(unittest.TestCase):
  """Test for context.Counters class."""

  def testIncrement(self):
    """Test increment() method."""
    m = mox.Mox()

    shard_state = m.CreateMockAnything()
    counters_map = m.CreateMockAnything()
    shard_state.counters_map = counters_map
    counters = context.Counters(shard_state)

    counters_map.increment('test', 19)

    m.ReplayAll()
    try:
      counters.increment('test', 19)

      m.VerifyAll()
    finally:
      m.UnsetStubs()

  def testFlush(self):
    """Test flush() method."""
    counters = context.Counters(None)
    counters.flush()


class ContextTest(unittest.TestCase):
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

    pool.flush()

    m.ReplayAll()
    try:
      ctx.flush()
      m.VerifyAll()
    finally:
      m.UnsetStubs()


if __name__ == "__main__":
  unittest.main()
