#!/usr/bin/env python




# pylint: disable=g-bad-name

import os
import unittest

from google.appengine.api import namespace_manager
from google.appengine.ext import db
from google.appengine.ext import key_range
from google.appengine.ext import testbed
from mapreduce import key_ranges
from mapreduce import namespace_range


class TestEntity(db.Model):
  foo = db.StringProperty(default="something")


class KeyRangesTest(unittest.TestCase):
  """Tests for key_ranges.KeyRanges."""

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.app = "foo"
    os.environ["APPLICATION_ID"] = self.app
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()

  def tearDown(self):
    self.testbed.deactivate()

  def testKeyRangesFromList(self):
    list_of_key_ranges = [key_range.KeyRange(db.Key.from_path("TestEntity", 1)),
                          key_range.KeyRange(db.Key.from_path("TestEntity", 2)),
                          key_range.KeyRange(db.Key.from_path("TestEntity", 3))]
    kranges = key_ranges.KeyRangesFactory.create_from_list(
        list(list_of_key_ranges))
    self._assertEqualsAndSerialize(list_of_key_ranges, kranges)

  def testKeyRangesFromNSRange(self):
    namespaces = ["1", "3", "5"]
    self.create_entities_in_multiple_ns(namespaces)
    ns_range = namespace_range.NamespaceRange("0", "5", _app=self.app)
    kranges = key_ranges.KeyRangesFactory.create_from_ns_range(ns_range)

    expected = [key_range.KeyRange(namespace="1", _app=self.app),
                key_range.KeyRange(namespace="3", _app=self.app),
                key_range.KeyRange(namespace="5", _app=self.app)]
    self._assertEqualsAndSerialize(expected, kranges)

  def _assertEqualsAndSerialize(self, expected, kranges):
    results = []
    while True:
      try:
        results.append(kranges.next())
        kranges = kranges.__class__.from_json(kranges.to_json())
      except StopIteration:
        break
    self.assertRaises(StopIteration, kranges.next)
    expected.sort()
    results.sort()
    self.assertEquals(expected, results)

  def create_entities_in_multiple_ns(self, names):
    ns = namespace_manager.get_namespace()
    for name in names:
      namespace_manager.set_namespace(name)
      TestEntity().put()
    namespace_manager.set_namespace(ns)


if __name__ == "__main__":
  unittest.main()
