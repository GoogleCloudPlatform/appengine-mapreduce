#!/usr/bin/env python
"""Tests for context interface."""

import unittest

from mapreduce import parameters
from mapreduce import test_support
from testlib import testutil
from mapreduce.api import map_job
from mapreduce.api.map_job import sample_input_reader

# pylint: disable=invalid-name

# The count to specify for the sample input reader.
TEST_SAMPLE_INPUT_READER_COUNT = 100


class MyMapper(map_job.Mapper):

  mappers = {}
  original_conf = None

  def begin_shard(self, ctx):
    self.assertShardCtx(ctx)

  def end_shard(self, ctx):
    self.assertShardCtx(ctx)

  def begin_slice(self, ctx):
    self.assertSliceCtx(ctx)

  def end_slice(self, ctx):
    self.assertSliceCtx(ctx)

  def __call__(self, ctx, val):
    self.assertSliceCtx(ctx)

  @classmethod
  def reset(cls):
    cls.mappers = {}
    cls.original_conf = None

  def assertShardCtx(self, ctx):
    self.original_conf.shard_count = ctx.job_context.job_config.shard_count
    assert ctx.job_context.job_config == self.original_conf
    assert ctx.id is not None
    assert ctx.number is not None
    assert ctx.attempt is not None
    assert ctx.job_context.job_config.user_params == {"foo": 1, "bar": 2}

  def assertSliceCtx(self, ctx):
    self.assertShardCtx(ctx.shard_context)
    assert ctx.number is not None
    assert ctx.attempt is not None


class MapperTest(testutil.HandlerTestBase):
  """Test mapper interface."""

  def setUp(self):
    super(MapperTest, self).setUp()
    MyMapper.reset()
    self.original_slice_duration = parameters.config._SLICE_DURATION_SEC

  def tearDown(self):
    parameters.config._SLICE_DURATION_SEC = self.original_slice_duration

  def testSmoke(self):
    # Force handler to serialize on every call.
    parameters.config._SLICE_DURATION_SEC = 0

    job_config = map_job.JobConfig(
        job_name="test_map",
        mapper=MyMapper,
        input_reader_cls=sample_input_reader.SampleInputReader,
        input_reader_params={"count": TEST_SAMPLE_INPUT_READER_COUNT},
        user_params={"foo": 1, "bar": 2})
    MyMapper.original_conf = job_config
    map_job.Job.submit(job_config)
    test_support.execute_until_empty(self.taskqueue)
    job = map_job.Job.get_job_by_id(job_config.job_id)
    self.assertEqual(map_job.Job.SUCCESS, job.get_status())


if __name__ == "__main__":
  unittest.main()

