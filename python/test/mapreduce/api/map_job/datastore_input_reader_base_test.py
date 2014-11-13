#!/usr/bin/env python
"""Datastore Input Reader Base Test for the map_job API."""
import unittest

from google.appengine.api import datastore_types
from google.appengine.api import namespace_manager
from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import testbed
from testlib import testutil
from mapreduce.api import map_job

# pylint: disable=invalid-name


class SkipTestsMeta(type):
  """Enables skipping tests from the base class but not when sub-classed."""

  def __init__(cls, name, bases, dct):
    super(SkipTestsMeta, cls).__init__(name, bases, dct)
    if cls.__name__ == "DatastoreInputReaderBaseTest":
      unittest.skip("Skip tests when testing from the base class.")(cls)
    else:
      # Since there is no unittest.unskip(), do it manually.
      cls.__unittest_skip__ = False
      cls.__unittest_skip_why__ = None


class DatastoreInputReaderBaseTest(unittest.TestCase):
  """Base test class used by concrete DatastoreInputReaders."""

  # Enable the meta class to skip all tests.
  __metaclass__ = SkipTestsMeta

  TEST_JOB_NAME = "TestJobHandlerName"

  # Subclass should override with its own create entities function.
  @property
  def _create_entities(self):
    return testutil._create_entities

  # Subclass should override with its own entity kind or model class path
  @property
  def entity_kind(self):
    return "TestEntity"

  # Subclass should override with its own reader class.
  @property
  def reader_cls(self):
    raise NotImplementedError("reader_cls() not implemented in %s"
                              % self.__class__)

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
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=2)
    results = self.reader_cls.split_input(conf)
    self.assertEquals(2, len(results))
    self._assertEqualsForAllShards_splitInput(["0", "1", "2"], None, *results)

  def testSplitInput_withNs_moreShardThanScatter(self):
    self._create_entities(range(3), {"1": 1}, "f")
    params = {
        "entity_kind": self.entity_kind,
        "namespace": "f",
        }
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=4)
    results = self.reader_cls.split_input(conf)
    self.assertTrue(len(results) >= 2)
    self._assertEqualsForAllShards_splitInput(["0", "1", "2"], None, *results)

  def testSplitInput_noEntity(self):
    params = {
        "entity_kind": self.entity_kind,
        }
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    results = self.reader_cls.split_input(conf)
    self.assertEquals(None, results)

  def testSplitInput_moreThanOneNS(self):
    self._create_entities(range(3), {"1": 1}, "1")
    self._create_entities(range(10, 13), {"11": 11}, "2")
    params = {
        "entity_kind": self.entity_kind,
        }
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=4)
    results = self.reader_cls.split_input(conf)
    self.assertTrue(len(results) >= 2)
    self._assertEqualsForAllShards_splitInput(
        ["0", "1", "2", "10", "11", "12"], None, *results)

  def testSplitInput_moreThanOneUnevenNS(self):
    self._create_entities(range(5), {"1": 1, "3": 3}, "1")
    self._create_entities(range(10, 13), {"11": 11}, "2")
    params = {
        "entity_kind": self.entity_kind,
        }
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=4)
    results = self.reader_cls.split_input(conf)
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
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=3)
    results = self.reader_cls.split_input(conf)
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
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=shards)
    results = self.reader_cls.split_input(conf)
    self.assertEqual(shards, len(results))
    all_keys = empty_ns_keys + non_empty_ns_keys
    self._assertEqualsForAllShards_splitInput(all_keys,
                                              len(all_keys),
                                              *results)

if __name__ == "__main__":
  unittest.main()

