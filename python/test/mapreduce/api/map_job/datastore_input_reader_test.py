#!/usr/bin/env python
"""Datastore Input Reader tests for the map_job API."""
# os_compat must be first to ensure timezones are UTC.
# pylint: disable=g-bad-import-order
from google.appengine.tools import os_compat  # pylint: disable=unused-import

import datetime
import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore
from mapreduce import errors
from testlib import testutil
from mapreduce.api import map_job
from mapreduce.api.map_job import datastore_input_reader_base_test
from mapreduce.api.map_job import datastore_input_reader

# pylint: disable=invalid-name


class DatastoreInputReaderTest(datastore_input_reader_base_test
                               .DatastoreInputReaderBaseTest):
  """DatastoreInputReader specific tests."""

  @property
  def reader_cls(self):
    return datastore_input_reader.DatastoreInputReader

  def testValidate_Filters(self):
    """Tests validating filters parameter."""
    params = {
        "entity_kind": self.entity_kind,
        "filters": [("a", "=", 1), ("b", "=", 2)],
        }
    new = datetime.datetime.now()
    old = new.replace(year=new.year-1)
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    conf.input_reader_cls.validate(conf)

    # Only equality filters supported.
    params["filters"] = [["datetime_property", ">", old],
                         ["datetime_property", "<=", new],
                         ["a", "=", 1]]
    self.assertRaises(errors.BadReaderParamsError,
                      conf.input_reader_cls.validate,
                      conf)

  def testEntityKindWithDot(self):
    self._create_entities(range(3), {"1": 1}, "", testutil.TestEntityWithDot)

    params = {
        "entity_kind": testutil.TestEntityWithDot.kind(),
        "namespace": "",
        }
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=2)
    results = conf.input_reader_cls.split_input(conf)
    self.assertEquals(2, len(results))
    self._assertEqualsForAllShards_splitInput(["0", "1", "2"], None, *results)

  def testRawEntityTypeFromOtherApp(self):
    """Test reading from other app."""
    OTHER_KIND = "bar"
    OTHER_APP = "foo"
    apiproxy_stub_map.apiproxy.GetStub("datastore_v3").SetTrusted(True)
    expected_keys = [str(i) for i in range(10)]
    for k in expected_keys:
      datastore.Put(datastore.Entity(OTHER_KIND, name=k, _app=OTHER_APP))

    params = {
        "entity_kind": OTHER_KIND,
        "_app": OTHER_APP,
    }
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    itr = conf.input_reader_cls.split_input(conf)[0]
    self._assertEquals_splitInput(itr, expected_keys)
    apiproxy_stub_map.apiproxy.GetStub("datastore_v3").SetTrusted(False)

if __name__ == "__main__":
  unittest.main()
