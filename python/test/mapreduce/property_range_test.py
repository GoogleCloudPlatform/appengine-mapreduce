#!/usr/bin/env python




# pylint: disable=g-bad-name

import datetime
import os
import string
import unittest

from google.appengine.ext import ndb

from google.appengine.api import namespace_manager
from google.appengine.ext import db
from google.appengine.ext import testbed
from mapreduce import property_range


class TestEntity(db.Model):
  """Test entity class."""

  datetime_property = db.DateTimeProperty(auto_now=True)

  a = db.IntegerProperty()
  b = db.IntegerProperty()
  c = db.FloatProperty()


class NdbTestEntity(ndb.Model):
  a = ndb.IntegerProperty()
  b = ndb.IntegerProperty()


class PropertyRangeTest(unittest.TestCase):

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.app = "foo"
    os.environ["APPLICATION_ID"] = self.app
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    self._original_alphabet = property_range._ALPHABET
    self._original_length = property_range._STRING_LENGTH

  def tearDown(self):
    self.testbed.deactivate()
    property_range._ALPHABET = self._original_alphabet
    property_range._STRING_LENGTH = self._original_length

  def testGetRangeFromFilters(self):
    """Tests validating filters parameter."""
    new = datetime.datetime.now()
    old = new.replace(year=new.year-1)
    filters = [("a", "=", 1), ("b", "=", 2)]
    prop, start, end = property_range.PropertyRange._get_range_from_filters(
        filters, TestEntity)
    self.assertEquals(prop, None)
    self.assertEquals(start, None)
    self.assertEquals(end, None)

    filters = [["a", ">", 1], ["a", "<", 2]]
    prop, start, end = property_range.PropertyRange._get_range_from_filters(
        filters, TestEntity)
    self.assertEquals(prop, TestEntity.a)
    self.assertEquals(start, ["a", ">", 1])
    self.assertEquals(end, ["a", "<", 2])

    filters = [["datetime_property", ">", old],
               ["datetime_property", "<=", new],
               ["a", "=", 1]]
    prop, start, end = property_range.PropertyRange._get_range_from_filters(
        filters, TestEntity)
    self.assertEquals(prop, TestEntity.datetime_property)
    self.assertEquals(start, ["datetime_property", ">", old])
    self.assertEquals(end, ["datetime_property", "<=", new])

    # Expect a range.
    filters = [("a", "<=", 1)]
    self.assertRaises(property_range.errors.BadReaderParamsError,
                      property_range.PropertyRange._get_range_from_filters,
                      filters,
                      TestEntity)

    # Expect a closed range.
    filters = [["datetime_property", ">", new],
               ["datetime_property", "<=", old]]
    self.assertRaises(property_range.errors.BadReaderParamsError,
                      property_range.PropertyRange._get_range_from_filters,
                      filters,
                      TestEntity)

  def testShouldShardByPropertyRange(self):
    filters = [("a", "=", 1)]
    self.assertFalse(property_range.should_shard_by_property_range(filters))
    filters = [("a", "<=", 1)]
    self.assertTrue(property_range.should_shard_by_property_range(filters))

  def testSplit(self):
    r = property_range.PropertyRange(
        [("a", ">=", 1), ("a", "<=", 4), ("b", "=", 1)],
        "__main__.TestEntity")
    results = r.split(10)
    self.assertEquals(4, len(results))
    for r in results:
      self.assertEquals(TestEntity, r.model_class)
    self.assertEquals([("a", ">=", 1), ("a", "<", 2), ("b", "=", 1)],
                      results[0].filters)
    self.assertEquals([("a", ">=", 2), ("a", "<", 3), ("b", "=", 1)],
                      results[1].filters)
    self.assertEquals([("a", ">=", 3), ("a", "<", 4), ("b", "=", 1)],
                      results[2].filters)
    self.assertEquals([("a", ">=", 4), ("a", "<", 5), ("b", "=", 1)],
                      results[3].filters)

    r = property_range.PropertyRange(
        [("c", ">=", 1), ("c", "<=", 4), ("b", "=", 1)],
        "__main__.TestEntity")
    results = r.split(4)
    self.assertEquals(4, len(results))
    for r in results:
      self.assertEquals(TestEntity, r.model_class)
    self.assertEquals([("c", ">=", 1), ("c", "<", 1.75), ("b", "=", 1)],
                      results[0].filters)
    self.assertEquals([("c", ">=", 1.75), ("c", "<", 2.5), ("b", "=", 1)],
                      results[1].filters)
    self.assertEquals([("c", ">=", 2.5), ("c", "<", 3.25), ("b", "=", 1)],
                      results[2].filters)
    self.assertEquals([("c", ">=", 3.25), ("c", "<=", 4), ("b", "=", 1)],
                      results[3].filters)

  def testSplit_reallyBigN(self):
    r = property_range.PropertyRange(
        [("a", ">", 1), ("a", "<=", 4), ("b", "=", 1)],
        "__main__.TestEntity")
    results = r.split(100)
    self.assertEquals(3, len(results))
    for r in results:
      self.assertEquals(TestEntity, r.model_class)
    self.assertEquals([("a", ">=", 2), ("a", "<", 3), ("b", "=", 1)],
                      results[0].filters)
    self.assertEquals([("a", ">=", 3), ("a", "<", 4), ("b", "=", 1)],
                      results[1].filters)
    self.assertEquals([("a", ">=", 4), ("a", "<", 5), ("b", "=", 1)],
                      results[2].filters)

  def testMakeQuery(self):
    keys = [str(i) for i in range(30)]
    a_vals = list(range(10))
    b_vals = list(range(3))
    ns = "foo"
    namespace_manager.set_namespace(ns)
    for k in keys:
      for a in a_vals:
        for b in b_vals:
          TestEntity(key_name=k, a=a, b=b).put()
    namespace_manager.set_namespace(None)

    r = property_range.PropertyRange(
        [("a", ">", 2), ("a", "<", 4), ("b", "=", 1)], "__main__.TestEntity")
    query = r.make_query(ns)
    results = set()
    for i in query.run():
      self.assertEquals(3, i.a)
      self.assertEquals(1, i.b)
      self.assertFalse(i.key().name() in results)
      results.add(i.key().name())

  def testMakeNdbQuery(self):
    keys = [str(i) for i in range(30)]
    a_vals = list(range(10))
    b_vals = list(range(3))
    ns = "foo"
    for k in keys:
      for a in a_vals:
        for b in b_vals:
          key = ndb.Key("NdbTestEntity", k, namespace=ns)
          NdbTestEntity(key=key, a=a, b=b).put()

    r = property_range.PropertyRange(
        [("a", ">", 2), ("a", "<", 4), ("b", "=", 1)], "__main__.NdbTestEntity")
    query = r.make_query(ns)
    results = set()
    for i in query.iter():
      self.assertEquals(3, i.a)
      self.assertEquals(1, i.b)
      self.assertFalse(i.key.name() in results)
      results.add(i.key.name())

  def testSplitDateTimeProperty(self):
    # year, month, day, hour, mim, sec.
    start = datetime.datetime(1, 1, 1, 0, 0, 0, 0)
    end = datetime.datetime(1, 1, 1, 0, 0, 0, 3)

    # Test combinations of inclusion and exclusion of start and end.
    self.assertEquals(
        [start,
         datetime.datetime(1, 1, 1, 0, 0, 0, 1),
         datetime.datetime(1, 1, 1, 0, 0, 0, 2),
         end,
         datetime.datetime(1, 1, 1, 0, 0, 0, 4),
        ],
        property_range._split_datetime_property(start, end, 4, True, True))

    self.assertEquals(
        [start,
         datetime.datetime(1, 1, 1, 0, 0, 0, 1),
         datetime.datetime(1, 1, 1, 0, 0, 0, 2),
         end,
        ],
        property_range._split_datetime_property(start, end, 3, True, False))

    self.assertEquals(
        [datetime.datetime(1, 1, 1, 0, 0, 0, 1),
         datetime.datetime(1, 1, 1, 0, 0, 0, 2),
         end,
         datetime.datetime(1, 1, 1, 0, 0, 0, 4),
        ],
        property_range._split_datetime_property(start, end, 3, False, True))

    # points >> shards
    end = datetime.datetime(1, 1, 1, 0, 0, 0, 100)
    self.assertEquals(
        [datetime.datetime(1, 1, 1, 0, 0, 0, 1),
         datetime.datetime(1, 1, 1, 0, 0, 0, 34),
         datetime.datetime(1, 1, 1, 0, 0, 0, 67),
         datetime.datetime(1, 1, 1, 0, 0, 0, 101),
        ],
        property_range._split_datetime_property(start, end, 3, False, True))

    end = datetime.datetime(1, 1, 1, 0, 0, 0, 1)
    self.assertRaises(ValueError, property_range._split_datetime_property,
                      start, end, 10, False, False)

  def testSplitFloatProperty(self):
    start = -9.1
    delta = 3.356
    end = start + 11 * delta

    result = property_range._split_float_property(start, end, 11)
    for point in result:
      self.assertTrue(isinstance(point, float))
    self.assertEquals([start + i * delta for i in range(1, 11)], result)

    end = .9
    self.assertEquals([-4.1],
                      property_range._split_float_property(start, end, 2))

    end = -10.1
    self.assertRaises(ValueError, property_range._split_float_property,
                      start, end, 11)

  def testSplitIntegerProperty(self):
    start = 1
    end = 10

    # Test combinations of inclusion and exclusion of start and end.
    result = property_range._split_integer_property(start, end, 10, True, True)
    for point in result:
      self.assertTrue(isinstance(point, int))
    self.assertEquals(list(range(1, 12)), result)

    result = property_range._split_integer_property(start, end, 9, False, True)
    for point in result:
      self.assertTrue(isinstance(point, int))
    self.assertEquals(list(range(2, 12)), result)

    result = property_range._split_integer_property(start, end, 9, True, False)
    for point in result:
      self.assertTrue(isinstance(point, int))
    self.assertEquals(list(range(1, 11)), result)

    result = property_range._split_integer_property(start, end, 8, False, False)
    for point in result:
      self.assertTrue(isinstance(point, int))
    self.assertEquals(list(range(2, 11)), result)

    # Irregular splits
    end = 11
    result = property_range._split_integer_property(start, end, 10, True, True)
    for point in result:
      self.assertTrue(isinstance(point, int))
    self.assertEquals([1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12], result)

    # Points >> shards
    end = 100
    result = property_range._split_integer_property(start, end, 3, True, True)
    self.assertEquals([1, 34, 68, 101], result)

    # Tricky edge cases.
    end = 1
    result = property_range._split_integer_property(start, end, 20, True, True)
    for point in result:
      self.assertTrue(isinstance(point, int))
    self.assertEquals([1, 2], result)

    end = 2
    self.assertRaises(ValueError, property_range._split_integer_property,
                      start, end, 2, False, False)

  def testOrdAndStrConversion(self):
    contents = ["abcdefg",
                ",=37   {%",
                string.printable,
                string.ascii_letters,
                string.punctuation,
                # Invisible control chars in hex.
                "\x01\x02\x03"]
    for c in contents:
      weights = property_range._get_weights(len(c))
      self.assertEquals(
          c, property_range._ord_to_str(
              property_range._str_to_ord(c, weights), weights))

  def testStrOrder(self):
    contents = ["", "a", "aa", "aaa", "aab", "ab", "aba", "abb", "b", "ba",
                "baa", "bab", "bb", "bba", "bbb"]
    contents.sort()

    weights = property_range._get_weights(3)
    previous_ordinal = -1
    for c in contents:
      ordinal = property_range._str_to_ord(c, weights)
      self.assertTrue(previous_ordinal < ordinal)
      previous_ordinal = ordinal

  def testSplitByteStringPropertySimple(self):
    start = "a\x01"
    end = "a\x05"

    # Test combinations of inclusion and exclusion of start and end.

    # This should produce intuitive splitting.
    result = property_range._split_byte_string_property(start, end, 4,
                                                        True, False)
    self.assertEquals(["a\x01", "a\x02", "a\x03", "a\x04", "a\x05"], result)

    property_range._STRING_LENGTH = 1
    result = property_range._split_byte_string_property(start, end, 4,
                                                        False, True)
    self.assertEquals(["a\x02", "a\x03", "a\x04", "a\x05", "a\x06"], result)

    result = property_range._split_byte_string_property(start, end, 5,
                                                        True, True)
    self.assertEquals(["a\x01", "a\x02", "a\x03", "a\x04", "a\x05", "a\x06"],
                      result)

    result = property_range._split_byte_string_property(start, end, 3,
                                                        False, False)
    self.assertEquals(["a\x02", "a\x03", "a\x04", "a\x05"], result)

    # points >> shards
    # 100 in decimal
    end = "a\x64"
    result = property_range._split_byte_string_property(start, end, 3,
                                                        True, True)
    # ord('"') = 34
    # ord("D") = 68
    # ord("e") = 101
    self.assertEquals(["a\x01", "a\"", "aD", "ae"], result)

    # Tricky edge cases.
    end = "a\x01"
    result = property_range._split_byte_string_property(start, end, 6,
                                                        True, True)
    self.assertEquals(["a\x01", "a\x02"], result)

    self.assertRaises(ValueError, property_range._split_byte_string_property,
                      start, end, 6, False, True)

  def testSplitByteStringPropertyComplexLargeRange(self):
    """User specified a large range.

    Large range has lots of splitpoints so the split looks intuitive.
    """
    property_range._ALPHABET = "ab"
    start = "aa"
    end = "aa" "bbb"
    # With "aa" as commom prefix. we are really doing a split on this space.
    # space = ["", "a", "aa", "aaa", "aab", "ab", "aba", "abb", "b", "ba",
    #          "baa", "bab", "bb", "bba", "bbb"]

    result = property_range._split_byte_string_property(start, end, 2,
                                                        True, False)
    self.assertEquals(["aa", "aaabb", "aabbb"], result)

    result = property_range._split_byte_string_property(start, end, 3,
                                                        True, False)
    self.assertEquals(["aa", "aaab", "aaba", "aabbb"], result)

    result = property_range._split_byte_string_property(start, end, 4,
                                                        True, False)
    self.assertEquals(["aa", "aaaab", "aaabb", "aabab", "aabbb"], result)

  def testSplitByteStringPropertySmallRange(self):
    """User specified a small range.

    Small range has few splitpoints comparing to shards. So
    property_range._STRING_LENGTH should kick in to provide more precision.
    """
    property_range._ALPHABET = "ab"
    property_range._STRING_LENGTH = 3
    start = "aa" "a"
    end = "aa" "b"
    # With "aa" as commom prefix. we are really doing a split on this space.
    # space = ["a", "aa", "aaa", "aab", "ab", "aba", "abb", "b"]

    result = property_range._split_byte_string_property(start, end, 2,
                                                        True, False)
    self.assertEquals(["aa" "a", "aa" "ab", "aa" "b"], result)

    result = property_range._split_byte_string_property(start, end, 3,
                                                        True, False)
    self.assertEquals(["aa" "a", "aa" "aaa", "aa" "aba", "aa" "b"], result)

    result = property_range._split_byte_string_property(start, end, 4,
                                                        True, False)
    self.assertEquals(["aa" "a", "aa" "aaa", "aa" "ab", "aa" "aba", "aa" "b"],
                      result)

  def testSplitByteStringPropertyInputWithSuffix(self):
    """Verify suffix is correctly appended."""
    property_range._ALPHABET = "ab"
    property_range._STRING_LENGTH = 3
    # With "aa" as commom prefix.
    # "bbb" and "aaa" are suffixes.
    start = "aa" "aaa" "bbb"
    end = "aa" "baa" "aaa"
    # we are doing a split on this space.
    # space = ["aaa", "aab", "ab", "aba", "abb", "b", "ba", "baa", "bab"]

    result = property_range._split_byte_string_property(start, end, 2,
                                                        True, False)
    self.assertEquals([start, "aa" "abb", end], result)

    result = property_range._split_byte_string_property(start, end, 2,
                                                        True, True)
    self.assertEquals([start, "aa" "abb", "aa" "bab" "aaa"], result)

  def testSplitStringProperty(self):
    start = "\x8f"
    end = "\xff"
    # Doesn't support unicode.
    self.assertRaises(ValueError, property_range._split_string_property,
                      start, end, 1, True, True)


if __name__ == "__main__":
  unittest.main()
