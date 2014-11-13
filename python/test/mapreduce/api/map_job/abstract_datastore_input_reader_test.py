#!/usr/bin/env python
"""Abstract Datastore Input Reader Test Helper for the map_job API."""
import os
import unittest

from google.appengine.api import datastore_types
from google.appengine.api import namespace_manager
from google.appengine.datastore import datastore_stub_util
from google.appengine.ext import key_range
from google.appengine.ext import testbed
from mapreduce import errors
from mapreduce import model
from testlib import testutil
from mapreduce.api import map_job
from mapreduce.api.map_job import abstract_datastore_input_reader

# pylint: disable=invalid-name


class AbstractDatastoreInputReaderTest(unittest.TestCase):
  """Tests for AbstractDatastoreInputReader."""

  TEST_JOB_NAME = "TestJobHandlerName"

  @property
  def reader_cls(self):
    return abstract_datastore_input_reader.AbstractDatastoreInputReader

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
        "entity_kind": testutil.ENTITY_KIND,
        }
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    self.reader_cls.validate(conf)

  def testValidate_NoEntityFails(self):
    """Test validate function raises exception with no entity parameter."""
    params = {}
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    self.assertRaises(errors.BadReaderParamsError,
                      self.reader_cls.validate,
                      conf)

  def testValidate_EntityKindWithNoModel(self):
    """Test validate function with bad entity kind."""
    params = {
        "entity_kind": "foo",
        }
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    self.reader_cls.validate(conf)

  def testValidate_BadBatchSize(self):
    """Test validate function rejects bad batch sizes."""
    params = {
        "entity_kind": testutil.ENTITY_KIND,
        "batch_size": "xxx"
        }
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    self.assertRaises(errors.BadReaderParamsError,
                      self.reader_cls.validate,
                      conf)
    # Batch size of 0 is invalid.
    params = {
        "entity_kind": testutil.ENTITY_KIND,
        "batch_size": "0"
        }
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    self.assertRaises(errors.BadReaderParamsError,
                      self.reader_cls.validate,
                      conf)

    # Batch size of -1 is invalid.
    params = {
        "entity_kind": testutil.ENTITY_KIND,
        "batch_size": "-1"
        }
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    self.assertRaises(errors.BadReaderParamsError,
                      self.reader_cls.validate,
                      conf)

  def testValidate_WrongTypeNamespace(self):
    """Tests validate function rejects namespace of incorrect type."""
    params = {
        "entity_kind": testutil.ENTITY_KIND,
        "namespace": 5
        }
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    self.assertRaises(errors.BadReaderParamsError,
                      self.reader_cls.validate,
                      conf)

  def testChooseSplitPoints(self):
    """Tests Abstract Datastore Input Reader._choose_split_points."""
    self.assertEquals(
        [5],
        self.reader_cls._choose_split_points(
            sorted([0, 9, 8, 7, 1, 2, 3, 4, 5, 6]), 2))

    self.assertEquals(
        [3, 7],
        self.reader_cls._choose_split_points(
            sorted([0, 1, 7, 8, 9, 3, 2, 4, 6, 5]), 3))

    self.assertEquals(
        range(1, 10),
        self.reader_cls._choose_split_points(
            sorted([0, 1, 7, 8, 9, 3, 2, 4, 6, 5]), 10))

    # Too few random keys
    self.assertRaises(
        AssertionError,
        self.reader_cls._choose_split_points,
        sorted([0, 1, 7, 8, 9, 3, 2, 4, 6, 5]), 11)

  def _assertEquals_splitNSByScatter(self, shards, expected, ns=""):
    results = self.reader_cls._split_ns_by_scatter(
        shards, ns, "TestEntity", self.appid)
    self.assertEquals(expected, results)

  def testSplitNSByScatter_NotEnoughData(self):
    """Splits should not intersect, if there's not enough data for each."""
    testutil._create_entities(range(2), {"1": 1})

    expected = [key_range.KeyRange(key_start=None,
                                   key_end=testutil.key("1"),
                                   direction="ASC",
                                   include_start=False,
                                   include_end=False,
                                   namespace="",
                                   _app=self.appid),
                key_range.KeyRange(key_start=testutil.key("1"),
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
    testutil._create_entities(range(10), {"2": 2, "4": 4})
    expected = [key_range.KeyRange(key_start=None,
                                   key_end=testutil.key("2"),
                                   direction="ASC",
                                   include_start=False,
                                   include_end=False,
                                   namespace="",
                                   _app=self.appid),
                key_range.KeyRange(key_start=testutil.key("2"),
                                   key_end=testutil.key("4"),
                                   direction="ASC",
                                   include_start=True,
                                   include_end=False,
                                   namespace="",
                                   _app=self.appid),
                key_range.KeyRange(key_start=testutil.key("4"),
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
    testutil._create_entities(range(100),
                              {"80": 80, "50": 50, "30": 30, "10": 10},
                              ns="google")
    expected = [key_range.KeyRange(key_start=None,
                                   key_end=testutil.key("30",
                                                        namespace="google"),
                                   direction="ASC",
                                   include_start=False,
                                   include_end=False,
                                   namespace="google",
                                   _app=self.appid),
                key_range.KeyRange(key_start=testutil.key("30",
                                                          namespace="google"),
                                   key_end=testutil.key("80",
                                                        namespace="google"),
                                   direction="ASC",
                                   include_start=True,
                                   include_end=False,
                                   namespace="google",
                                   _app=self.appid),
                key_range.KeyRange(key_start=testutil.key("80",
                                                          namespace="google"),
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
      testutil._create_entities(range(10), {"5": 5}, ns)
    shards = 2

    expected = [
        key_range.KeyRange(key_start=None,
                           key_end=testutil.key("5", namespace="0"),
                           direction="ASC",
                           include_start=False,
                           include_end=False,
                           namespace="0",
                           _app=self.appid),
        key_range.KeyRange(key_start=None,
                           key_end=testutil.key("5", namespace="1"),
                           direction="ASC",
                           include_start=False,
                           include_end=False,
                           namespace="1",
                           _app=self.appid),
        key_range.KeyRange(key_start=None,
                           key_end=testutil.key("5", namespace="2"),
                           direction="ASC",
                           include_start=False,
                           include_end=False,
                           namespace="2",
                           _app=self.appid),

        key_range.KeyRange(key_start=testutil.key("5", namespace="0"),
                           key_end=None,
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="0",
                           _app=self.appid),
        key_range.KeyRange(key_start=testutil.key("5", namespace="1"),
                           key_end=None,
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="1",
                           _app=self.appid),
        key_range.KeyRange(key_start=testutil.key("5", namespace="2"),
                           key_end=None,
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="2",
                           _app=self.appid),
    ]

    kranges_by_shard = (
        self.reader_cls._to_key_ranges_by_shard(
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
    testutil._create_entities(range(10), {"5": 5}, namespaces[0])
    testutil._create_entities(range(10), {"5": 5, "6": 6}, namespaces[1])
    testutil._create_entities(range(10), {"5": 5, "6": 6, "7": 7},
                              namespaces[2])
    shards = 3

    expected = [
        # shard 1
        key_range.KeyRange(key_start=None,
                           key_end=testutil.key("5", namespace="0"),
                           direction="ASC",
                           include_start=False,
                           include_end=False,
                           namespace="0",
                           _app=self.appid),
        key_range.KeyRange(key_start=None,
                           key_end=testutil.key("5", namespace="1"),
                           direction="ASC",
                           include_start=False,
                           include_end=False,
                           namespace="1",
                           _app=self.appid),
        key_range.KeyRange(key_start=None,
                           key_end=testutil.key("6", namespace="2"),
                           direction="ASC",
                           include_start=False,
                           include_end=False,
                           namespace="2",
                           _app=self.appid),
        # shard 2
        key_range.KeyRange(key_start=testutil.key("5", namespace="0"),
                           key_end=None,
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="0",
                           _app=self.appid),
        key_range.KeyRange(key_start=testutil.key("5", namespace="1"),
                           key_end=testutil.key("6", namespace="1"),
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="1",
                           _app=self.appid),
        key_range.KeyRange(key_start=testutil.key("6", namespace="2"),
                           key_end=testutil.key("7", namespace="2"),
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="2",
                           _app=self.appid),
        # shard 3
        key_range.KeyRange(key_start=testutil.key("6", namespace="1"),
                           key_end=None,
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="1",
                           _app=self.appid),
        key_range.KeyRange(key_start=testutil.key("7", namespace="2"),
                           key_end=None,
                           direction="ASC",
                           include_start=True,
                           include_end=False,
                           namespace="2",
                           _app=self.appid),
    ]
    kranges_by_shard = (self.reader_cls._to_key_ranges_by_shard(
        self.appid, namespaces, shards,
        model.QuerySpec(entity_kind="TestEntity")))
    self.assertEquals(shards, len(kranges_by_shard))

    expected.sort()
    results = []
    for kranges in kranges_by_shard:
      results.extend(list(kranges))
    results.sort()
    self.assertEquals(expected, results)


if __name__ == "__main__":
  unittest.main()
