#!/usr/bin/env python
import unittest

from mapreduce import parameters
from mapreduce.api import map_job
from mapreduce.api.map_job import sample_input_reader


class MapJobConfigTest(unittest.TestCase):
  """Test for MapJobConfig.

  MapJobConfig is declarative. Thus most functional tests are already
  done by its parent class.
  """

  def testSmoke(self):
    conf = map_job.JobConfig(
        job_name="foo",
        mapper=map_job.Mapper,
        input_reader_cls=sample_input_reader.SampleInputReader,
        input_reader_params={"foo": 1})
    self.assertEqual("foo", conf.job_name)
    self.assertTrue(conf.job_id)
    self.assertEqual(map_job.Mapper, conf.mapper)
    self.assertEqual(sample_input_reader.SampleInputReader,
                     conf.input_reader_cls)
    self.assertEqual({"foo": 1}, conf.input_reader_params)
    self.assertEqual(parameters.config.SHARD_COUNT, conf.shard_count)

  def testUserProvidesJobID(self):
    conf = map_job.JobConfig(
        job_name="foo",
        job_id="id",
        mapper=map_job.Mapper,
        input_reader_cls=sample_input_reader.SampleInputReader,
        input_reader_params={"foo": 1})
    self.assertEqual("id", conf.job_id)


if __name__ == "__main__":
  unittest.main()
