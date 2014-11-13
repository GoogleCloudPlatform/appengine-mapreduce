#!/usr/bin/env python
# Copyright 2010 Google Inc. All Rights Reserved.
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

"""Tests for mapreduce.namespace_range."""



import os
import unittest


import mox

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore
from google.appengine.api import datastore_file_stub
from google.appengine.api import namespace_manager
from google.appengine.ext import db
from mapreduce import namespace_range


class Pet(db.Model):
  """A test model used to populate namespaces."""
  pass


class TestOrdinalization(unittest.TestCase):
  """Tests NamespaceRange.(_ord_to_namespace|_namespace_to_ord)."""

  def setUp(self):
    unittest.TestCase.setUp(self)

  def tearDown(self):
    namespace_range._setup_constants()
    unittest.TestCase.tearDown(self)

  def testWithSmallRange(self):
    namespace_range._setup_constants('ab', 2)
    self.assertEquals('', namespace_range._ord_to_namespace(0))
    self.assertEquals(0, namespace_range._namespace_to_ord(''))

    self.assertEquals('a', namespace_range._ord_to_namespace(1))
    self.assertEquals(1, namespace_range._namespace_to_ord('a'))

    self.assertEquals('aa', namespace_range._ord_to_namespace(2))
    self.assertEquals(2, namespace_range._namespace_to_ord('aa'))

    self.assertEquals('ab', namespace_range._ord_to_namespace(3))
    self.assertEquals(3, namespace_range._namespace_to_ord('ab'))

    self.assertEquals('b', namespace_range._ord_to_namespace(4))
    self.assertEquals(4, namespace_range._namespace_to_ord('b'))

    self.assertEquals('ba', namespace_range._ord_to_namespace(5))
    self.assertEquals(5, namespace_range._namespace_to_ord('ba'))

    self.assertEquals('bb', namespace_range._ord_to_namespace(6))
    self.assertEquals(6, namespace_range._namespace_to_ord('bb'))


class JsonTest(unittest.TestCase):
  """Tests JSON serialization and deserialization."""

  def testToJsonObject(self):
    self.assertEqual(
        dict(namespace_start='a',
             namespace_end='b'),
        namespace_range.NamespaceRange('a', 'b').to_json_object())

  def testToJsonObjectWithApp(self):
    self.assertEqual(
        dict(namespace_start='a',
             namespace_end='b',
             app='myapp'),
        namespace_range.NamespaceRange('a', 'b', _app='myapp').to_json_object())

  def testFromJsonObject(self):
    self.assertEqual(
        namespace_range.NamespaceRange('a', 'b'),
        namespace_range.NamespaceRange.from_json_object(
            dict(namespace_start='a',
                 namespace_end='b')))

  def testFromJsonObjectWithApp(self):
    self.assertEqual(
        namespace_range.NamespaceRange('a', 'b', _app='myapp'),
        namespace_range.NamespaceRange.from_json_object(
            dict(namespace_start='a',
                 namespace_end='b',
                 app='myapp')))


class NamespaceRangeIterationTest(unittest.TestCase):
  """Tests iterating over a NamespaceRange."""

  def setUp(self):
    unittest.TestCase.setUp(self)
    namespace_range._setup_constants('abc', 3, 3)
    self.app_id = 'testapp'
    os.environ['APPLICATION_ID'] = self.app_id
    self.mox = mox.Mox()

  def tearDown(self):
    namespace_range._setup_constants()
    unittest.TestCase.tearDown(self)
    try:
      self.mox.VerifyAll()
    finally:
      self.mox.UnsetStubs()

  def testQueryPaging(self):
    self.mox.StubOutClassWithMocks(datastore, 'Query')
    ns_range = namespace_range.NamespaceRange(
        namespace_start='a', namespace_end='b', _app=self.app_id)
    ns_kind = '__namespace__'
    ns_key = lambda ns: db.Key.from_path(ns_kind, ns)
    filters = {'__key__ >= ': ns_key('a'), '__key__ <= ': ns_key('b')}

    def ExpectQuery(cursor):
      return datastore.Query(
          ns_kind, filters=filters, keys_only=True, cursor=cursor,
          _app=self.app_id)

    query = ExpectQuery(None)
    query.Run(limit=3).AndReturn([ns_key(ns) for ns in ['a', 'aa', 'aaa']])
    query.GetCursor().AndReturn('c1')

    query = ExpectQuery('c1')
    query.Run(limit=3).AndReturn([ns_key(ns) for ns in ['aab', 'ab', 'ac']])
    query.GetCursor().AndReturn('c2')

    query = ExpectQuery('c2')
    query.Run(limit=3).AndReturn([ns_key('b')])

    self.mox.ReplayAll()
    self.assertEqual(7, len(list(ns_range)))


class NamespaceRangeSplitTest(unittest.TestCase):
  """Tests namespace_range.NamespaceRange.split()."""

  def setUp(self):
    unittest.TestCase.setUp(self)

    namespace_range._setup_constants('abc', 3, 3)
    self.app_id = 'testapp'
    os.environ['APPLICATION_ID'] = self.app_id
    self.datastore = datastore_file_stub.DatastoreFileStub(
        self.app_id, '/dev/null', '/dev/null')

    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', self.datastore)
    namespace_manager.set_namespace(None)

  def tearDown(self):
    namespace_range._setup_constants()
    unittest.TestCase.tearDown(self)

  def CreateInNamespace(self, namespace):
    old_ns = namespace_manager.get_namespace()
    try:
      namespace_manager.set_namespace(namespace)
      pet = Pet()
      pet.put()
    finally:
      namespace_manager.set_namespace(old_ns)

  def testSplitMultipleNamespacesPerShard(self):
    ns = [x + y + z
          for x in list('cba') for y in list('abc') for z in list('bac')]
    db.put([Pet(key=db.Key.from_path('Pet', '1', namespace=x)) for x in ns])
    ranges = namespace_range.NamespaceRange.split(
        3, contiguous=False, can_query=lambda: True)
    self.assertEqual(3, len(ranges))
    self.assertEqual(27, sum([len(list(r)) for r in ranges]))

  def testSplitWithoutQueries(self):
    self.assertEqual(
        [namespace_range.NamespaceRange(namespace_start='',
                                        namespace_end='abc',
                                        _app=self.app_id),
         namespace_range.NamespaceRange(namespace_start='ac',
                                        namespace_end='bb',
                                        _app=self.app_id),
         namespace_range.NamespaceRange(namespace_start='bba',
                                        namespace_end='caa',
                                        _app=self.app_id),
         namespace_range.NamespaceRange(namespace_start='cab',
                                        namespace_end='ccc',
                                        _app=self.app_id)],
        namespace_range.NamespaceRange.split(4,
                                             contiguous=False,
                                             can_query=lambda: False,
                                             _app=self.app_id))

  def testSplitWithoutQueriesWithContiguous(self):
    self.assertEqual(
        [namespace_range.NamespaceRange(namespace_start='',
                                        namespace_end='abc',
                                        _app=self.app_id),
         namespace_range.NamespaceRange(namespace_start='ac',
                                        namespace_end='bb',
                                        _app=self.app_id),
         namespace_range.NamespaceRange(namespace_start='bba',
                                        namespace_end='caa',
                                        _app=self.app_id),
         namespace_range.NamespaceRange(namespace_start='cab',
                                        namespace_end='ccc',
                                        _app=self.app_id)],
        namespace_range.NamespaceRange.split(4,
                                             contiguous=True,
                                             can_query=lambda: False,
                                             _app=self.app_id))

  def testSplitWithNoNamespacesInDatastore(self):
    self.assertEqual(
        [],
        namespace_range.NamespaceRange.split(10,
                                             contiguous=False,
                                             can_query=lambda: True,
                                             _app=self.app_id))

  def testSplitWithNoNamespacesInDatastoreWithContiguous(self):
    self.assertEqual(
        [namespace_range.NamespaceRange(_app=self.app_id)],
        namespace_range.NamespaceRange.split(10,
                                             contiguous=True,
                                             can_query=lambda: True,
                                             _app=self.app_id))

  def testSplitWithOnlyDefaultNamespace(self):
    self.CreateInNamespace('')

    self.assertEqual(
        [namespace_range.NamespaceRange(namespace_start='',
                                        namespace_end='',
                                        _app=self.app_id)],
        namespace_range.NamespaceRange.split(10,
                                             contiguous=False,
                                             can_query=lambda: True,
                                             _app=self.app_id))

  def testSplitWithOnlyDefaultNamespaceWithContiguous(self):
    self.CreateInNamespace('')

    self.assertEqual(
        [namespace_range.NamespaceRange(namespace_start='',
                                        namespace_end='ccc',
                                        _app=self.app_id)],
        namespace_range.NamespaceRange.split(10,
                                             contiguous=True,
                                             can_query=lambda: True,
                                             _app=self.app_id))

  def testSplitWithInfiniteQueriesSmallerSplitThanNamespaces(self):
    # Create 6 namespaces and split by 3 ranges. Use contiguous data for this
    # test (although we are not testing contiguous) and since the mid-point is
    # rounded down, skip 'aa' and 'aba' so that start and end of each range
    # will match exactly and contain 2 items.
    self.CreateInNamespace('a')
    self.CreateInNamespace('aaa')
    self.CreateInNamespace('aab')
    self.CreateInNamespace('aac')
    self.CreateInNamespace('ab')
    self.CreateInNamespace('abb')

    self.assertEqual(
        [namespace_range.NamespaceRange(namespace_start='a',
                                        namespace_end='aaa'),
         namespace_range.NamespaceRange(namespace_start='aab',
                                        namespace_end='aac'),
         namespace_range.NamespaceRange(namespace_start='ab',
                                        namespace_end='abb')],
        namespace_range.NamespaceRange.split(3,
                                             contiguous=False,
                                             can_query=lambda: True))

  def testSplitWithInfiniteQueriesEqualSplitToNamespaces(self):
    # Create 6 namespaces and split by 6 ranges.
    self.CreateInNamespace('a')
    self.CreateInNamespace('aa')
    self.CreateInNamespace('aab')
    self.CreateInNamespace('b')
    self.CreateInNamespace('bac')
    self.CreateInNamespace('cca')

    self.assertEqual(
        [namespace_range.NamespaceRange(namespace_start='a',
                                        namespace_end='a'),
         namespace_range.NamespaceRange(namespace_start='aa',
                                        namespace_end='aa'),
         namespace_range.NamespaceRange(namespace_start='aab',
                                        namespace_end='aab'),
         namespace_range.NamespaceRange(namespace_start='b',
                                        namespace_end='b'),
         namespace_range.NamespaceRange(namespace_start='bac',
                                        namespace_end='bac'),
         namespace_range.NamespaceRange(namespace_start='cca',
                                        namespace_end='cca')],
        namespace_range.NamespaceRange.split(6,
                                             contiguous=False,
                                             can_query=lambda: True))

  def testSplitWithInfiniteQueriesLargerSplitThanNamespaces(self):
    # Create 6 namespaces and split by 10 ranges.
    self.CreateInNamespace('a')
    self.CreateInNamespace('aa')
    self.CreateInNamespace('aab')
    self.CreateInNamespace('b')
    self.CreateInNamespace('bac')
    self.CreateInNamespace('cca')

    self.assertEqual(
        [namespace_range.NamespaceRange(namespace_start='a',
                                        namespace_end='a'),
         namespace_range.NamespaceRange(namespace_start='aa',
                                        namespace_end='aa'),
         namespace_range.NamespaceRange(namespace_start='aab',
                                        namespace_end='aab'),
         namespace_range.NamespaceRange(namespace_start='b',
                                        namespace_end='b'),
         namespace_range.NamespaceRange(namespace_start='bac',
                                        namespace_end='bac'),
         namespace_range.NamespaceRange(namespace_start='cca',
                                        namespace_end='cca')],
        namespace_range.NamespaceRange.split(10,
                                             contiguous=False,
                                             can_query=lambda: True))

  def testSplitWithInfiniteQueriesWithContiguous(self):
    self.CreateInNamespace('a')
    self.CreateInNamespace('aa')
    self.CreateInNamespace('aab')
    self.CreateInNamespace('b')
    self.CreateInNamespace('bac')
    self.CreateInNamespace('cca')

    self.assertEqual(
        [namespace_range.NamespaceRange(namespace_start='',
                                        namespace_end='a'),
         namespace_range.NamespaceRange(namespace_start='aa',
                                        namespace_end='aaa'),
         namespace_range.NamespaceRange(namespace_start='aab',
                                        namespace_end='acc'),
         namespace_range.NamespaceRange(namespace_start='b',
                                        namespace_end='bab'),
         namespace_range.NamespaceRange(namespace_start='bac',
                                        namespace_end='cc'),
         namespace_range.NamespaceRange(namespace_start='cca',
                                        namespace_end='ccc')],
        namespace_range.NamespaceRange.split(10,
                                             contiguous=True,
                                             can_query=lambda: True))

if __name__ == '__main__':
  unittest.main()
