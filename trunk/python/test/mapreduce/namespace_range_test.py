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

"""Tests for mapreduce.namespace_range."""



import os
import unittest

from google.appengine.api import apiproxy_stub_map
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


class NamespaceRangeSplitTest(unittest.TestCase):
  """Tests namespace_range.NamespaceRange.split() with contiguous=False"""

  def setUp(self):
    unittest.TestCase.setUp(self)

    namespace_range._setup_constants('abc', 3)
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
                                        namespace_end='',
                                        _app=self.app_id)],
        namespace_range.NamespaceRange.split(10,
                                             contiguous=False,
                                             can_query=lambda: True,
                                             _app=self.app_id))

  def testSplitWithInfiniteQueries(self):
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
