#!/usr/bin/env python
"""Model Datastore Input Reader tests for the map_job API."""
# os_compat must be first to ensure timezones are UTC.
# pylint: disable=g-bad-import-order
from google.appengine.tools import os_compat  # pylint: disable=unused-import

import datetime
import unittest

from google.appengine.ext import ndb
from mapreduce import errors
from testlib import testutil
from mapreduce.api import map_job
from mapreduce.api.map_job import datastore_input_reader_base_test
from mapreduce.api.map_job import model_datastore_input_reader

# pylint: disable=invalid-name


class ModelDBDatastoreInputReaderTest(datastore_input_reader_base_test
                                      .DatastoreInputReaderBaseTest):
  """Test ModelDatastoreInputReader using Model.db."""

  @property
  def reader_cls(self):
    return model_datastore_input_reader.ModelDatastoreInputReader

  @property
  def entity_kind(self):
    return testutil.ENTITY_KIND

  def testValidate_EntityKindWithNoModel(self):
    """Test validate function with no model."""
    params = {
        "entity_kind": "foo",
        }
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=1)
    self.assertRaises(errors.BadReaderParamsError,
                      conf.input_reader_cls.validate,
                      conf)

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

    conf.input_reader_params["filters"] = [["a", ">", 1], ["a", "<", 2]]
    conf.input_reader_cls.validate(conf)

    conf.input_reader_params["filters"] = [["datetime_property", ">", old],
                                           ["datetime_property", "<=", new],
                                           ["a", "=", 1]]
    conf.input_reader_cls.validate(conf)

    conf.input_reader_params["filters"] = [["a", "=", 1]]
    conf.input_reader_cls.validate(conf)

    # Invalid field c
    conf.input_reader_params["filters"] = [("c", "=", 1)]
    self.assertRaises(errors.BadReaderParamsError,
                      conf.input_reader_cls.validate,
                      conf)

    # Expect a range.
    conf.input_reader_params["filters"] = [("a", "<=", 1)]
    self.assertRaises(errors.BadReaderParamsError,
                      conf.input_reader_cls.validate,
                      conf)

    # Value should be a datetime.
    conf.input_reader_params["filters"] = [["datetime_property", ">", 1],
                                           ["datetime_property", "<=",
                                            datetime.datetime.now()]]
    self.assertRaises(errors.BadReaderParamsError,
                      conf.input_reader_cls.validate,
                      conf)

    # Expect a closed range.
    params["filters"] = [["datetime_property", ">", new],
                         ["datetime_property", "<=", old]]
    self.assertRaises(errors.BadReaderParamsError,
                      conf.input_reader_cls.validate,
                      conf)

  def _set_vals(self, entities, a_vals, b_vals):
    """Set a, b values for entities."""
    vals = []
    for a in a_vals:
      for b in b_vals:
        vals.append((a, b))
    for e, val in zip(entities, vals):
      e.a = val[0]
      e.b = val[1]
      e.put()

  def testSplitInput_shardByFilters_withNs(self):
    entities = self._create_entities(range(12), {}, "f")
    self._set_vals(entities, list(range(6)), list(range(2)))
    params = {
        "entity_kind": self.entity_kind,
        "namespace": "f",
        "filters": [("a", ">", 0),
                    ("a", "<=", 3),
                    ("b", "=", 1)],
        }
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=2)
    results = conf.input_reader_cls.split_input(conf)
    self.assertEquals(2, len(results))
    self._assertEquals_splitInput(results[0], ["3", "5"])
    self._assertEquals_splitInput(results[1], ["7"])

  def testSplitInput_shardByFilters_noEntity(self):
    params = {
        "entity_kind": self.entity_kind,
        "namespace": "f",
        "filters": [("a", ">", 0), ("a", "<=", 3), ("b", "=", 1)]
        }
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=100)
    results = conf.input_reader_cls.split_input(conf)
    self.assertEquals(3, len(results))
    self._assertEquals_splitInput(results[0], [])
    self._assertEquals_splitInput(results[1], [])
    self._assertEquals_splitInput(results[2], [])

  def testSplitInput_shardByFilters_bigShardNumber(self):
    entities = self._create_entities(range(12), {}, "f")
    self._set_vals(entities, list(range(6)), list(range(2)))
    params = {
        "entity_kind": self.entity_kind,
        "namespace": "f",
        "filters": [("a", ">", 0), ("a", "<=", 3), ("b", "=", 1)]
        }
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=100)
    results = conf.input_reader_cls.split_input(conf)
    self.assertEquals(3, len(results))
    self._assertEquals_splitInput(results[0], ["3"])
    self._assertEquals_splitInput(results[1], ["5"])
    self._assertEquals_splitInput(results[2], ["7"])

  def testSplitInput_shardByFilters_lotsOfNS(self):
    """Lots means more than 2 in test cases."""
    entities = self._create_entities(range(12), {}, "f")
    self._set_vals(entities, list(range(6)), list(range(2)))
    entities = self._create_entities(range(12, 24), {}, "g")
    self._set_vals(entities, list(range(6)), list(range(2)))
    entities = self._create_entities(range(24, 36), {}, "h")
    self._set_vals(entities, list(range(6)), list(range(2)))
    entities = self._create_entities(range(36, 48), {}, "h")
    self._set_vals(entities, [0]*6, list(range(2)))

    params = {
        "entity_kind": self.entity_kind,
        "filters": [("a", ">", 0), ("a", "<=", 3), ("b", "=", 1)]
        }
    conf = map_job.JobConfig(
        job_name=self.TEST_JOB_NAME,
        mapper=map_job.Mapper,
        input_reader_cls=self.reader_cls,
        input_reader_params=params,
        shard_count=100)
    results = conf.input_reader_cls.split_input(conf)
    self.assertEquals(3, len(results))
    self._assertEquals_splitInput(results[0], ["3", "5", "7"])
    self._assertEquals_splitInput(results[1], ["15", "17", "19"])
    self._assertEquals_splitInput(results[2], ["27", "29", "31"])


class ModelNDBDatastoreInputReaderTest(datastore_input_reader_base_test
                                       .DatastoreInputReaderBaseTest):
  """Test ModelDatastoreInputReader using Model.ndb."""

  @property
  def reader_cls(self):
    return model_datastore_input_reader.ModelDatastoreInputReader

  @property
  def entity_kind(self):
    return testutil.NDB_ENTITY_KIND

  def _create_entities(self,
                       keys_itr,
                       key_to_scatter_val,
                       ns=None,
                       entity_model_cls=testutil.NdbTestEntity):
    """Create ndb entities for tests.

    Args:
      keys_itr: an iterator that contains all the key names.
        Will be casted to str.
      key_to_scatter_val: a dict that maps key names to its scatter values.
      ns: the namespace to create the entity at.
      entity_model_cls: entity model class.

    Returns:
      A list of entities created.
    """
    testutil.set_scatter_setter(key_to_scatter_val)
    entities = []
    for i in keys_itr:
      k = ndb.Key(entity_model_cls._get_kind(), str(i), namespace=ns)
      entity = entity_model_cls(key=k)
      entities.append(entity)
      entity.put()
    return entities

  def _get_keyname(self, entity):
    return entity.key.id()

if __name__ == "__main__":
  unittest.main()
