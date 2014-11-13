#!/usr/bin/env python
"""Tests for mapper interface."""

import collections
import unittest

from mapreduce import output_writers
from mapreduce import parameters
from mapreduce import test_support
from testlib import testutil
from mapreduce.api import map_job
from mapreduce.api.map_job import sample_input_reader

# pylint: disable=g-import-not-at-top
# TODO(user): Cleanup imports if/when cloudstorage becomes part of runtime.
try:
  import cloudstorage
except ImportError:
  cloudstorage = None

# pylint: disable=g-bad-name


class MyMapper(map_job.Mapper):

  mappers = {}
  slices = 0

  def __init__(self):
    super(MyMapper, self).__init__()
    self.processed = 0
    self._started = False
    self._slice_started = False
    self._pickled_once = False

  def begin_shard(self, ctx):
    assert ctx is not None
    self._started = True

  def end_shard(self, ctx):
    assert self._pickled_once
    self.mappers[ctx.id] = self

  def begin_slice(self, ctx):
    assert self._slice_started is False
    self._slice_started = True
    self.__class__.slices += 1
    ctx.emit("begin_slice\n")
    ctx.incr("SLICES")

  def end_slice(self, ctx):
    assert self._slice_started
    self._slice_started = False
    ctx.emit("end_slice\n")

  def __call__(self, ctx, val):
    assert self._started
    self.processed += 1
    ctx.incr("FOO_COUNTER", 2)
    ctx.incr("BAR_COUNTER", -1)
    ctx.emit("foo\n")
    ctx.emit("bar\n")

  def __getstate__(self):
    self._pickled_once = True
    return self.__dict__

  @classmethod
  def reset(cls):
    cls.mappers = {}
    cls.slices = 0


class MapperTest(testutil.HandlerTestBase):
  """Test mapper interface."""

  def setUp(self):
    super(MapperTest, self).setUp()
    MyMapper.reset()
    self.original_slice_duration = parameters.config._SLICE_DURATION_SEC

  def tearDown(self):
    parameters.config._SLICE_DURATION_SEC = self.original_slice_duration

  # TODO(user): After mr release is done via pypi, we can start assume
  # cloudstorage library is present.
  @unittest.skipIf(cloudstorage is None,
                   "Cloudstorage library is not available.")
  def testSmoke(self):
    entity_count = 10

    # Force handler to serialize on every call.
    parameters.config._SLICE_DURATION_SEC = 0

    job = map_job.Job.submit(map_job.JobConfig(
        job_name="test_map",
        mapper=MyMapper,
        input_reader_cls=sample_input_reader.SampleInputReader,
        input_reader_params={"count": entity_count},
        output_writer_cls=output_writers._GoogleCloudStorageOutputWriter,
        output_writer_params={"bucket_name": "bucket"}))
    test_support.execute_until_empty(self.taskqueue)
    total = 0
    for m in MyMapper.mappers.values():
      total += m.processed
    self.assertEqual(entity_count, total)

    # Verify counters.
    counters = dict(job.get_counters())
    self.assertEqual(counters["FOO_COUNTER"], 2 * entity_count)
    self.assertEqual(counters["BAR_COUNTER"], -1 * entity_count)
    self.assertEqual(counters["SLICES"], MyMapper.slices)

    # Verify outputs.
    files = output_writers._GoogleCloudStorageOutputWriter.get_filenames(
        job._state)
    outputs = collections.defaultdict(lambda: 0)
    expected = {"foo\n": entity_count,
                "bar\n": entity_count,
                "end_slice\n": MyMapper.slices,
                "begin_slice\n": MyMapper.slices}
    for fn in files:
      f = cloudstorage.open(fn)
      for line in f:
        outputs[line] += 1
    self.assertEqual(expected, outputs)


if __name__ == "__main__":
  unittest.main()

