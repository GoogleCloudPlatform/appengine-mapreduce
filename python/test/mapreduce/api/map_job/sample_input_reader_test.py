#!/usr/bin/env python
import unittest

from mapreduce import errors
from mapreduce.api import map_job
from mapreduce.api.map_job import sample_input_reader


class SampleInputReaderTest(unittest.TestCase):
  """Tests for SampleInputReader."""

  def testIter(self):
    input_reader = sample_input_reader.SampleInputReader(10, 9)
    i = 0
    for content in input_reader:
      i += 1
      self.assertEquals(9, len(content))
    self.assertEquals(10, i)

  def testEndToEnd(self):
    conf = map_job.JobConfig(
        job_name="test_handler",
        mapper=map_job.Mapper,
        input_reader_cls=sample_input_reader.SampleInputReader,
        input_reader_params={"count": 1000},
        shard_count=99)
    readers = sample_input_reader.SampleInputReader.split_input(conf)
    i = 0
    for reader in readers:
      for _ in reader:
        i += 1
    self.assertEquals(1000, i)

  def testValidate(self):
    # Check that some input reader params are required.
    conf = map_job.JobConfig(
        job_name="test_handler",
        mapper=map_job.Mapper,
        input_reader_cls=sample_input_reader.SampleInputReader,
        input_reader_params={},
        shard_count=99)
    self.assertRaises(errors.BadReaderParamsError,
                      sample_input_reader.SampleInputReader.validate,
                      conf)

    # Check that count is an integer.
    conf = map_job.JobConfig(
        job_name="test_handler",
        mapper=map_job.Mapper,
        input_reader_cls=sample_input_reader.SampleInputReader,
        input_reader_params={"count": "1000"},
        shard_count=99)
    self.assertRaises(errors.BadReaderParamsError,
                      sample_input_reader.SampleInputReader.validate,
                      conf)

    # Check that count is a positive integer.
    conf = map_job.JobConfig(
        job_name="test_handler",
        mapper=map_job.Mapper,
        input_reader_cls=sample_input_reader.SampleInputReader,
        input_reader_params={"count": -1},
        shard_count=99)
    self.assertRaises(errors.BadReaderParamsError,
                      sample_input_reader.SampleInputReader.validate,
                      conf)

    # Check that string_length is an integer.
    conf = map_job.JobConfig(
        job_name="test_handler",
        mapper=map_job.Mapper,
        input_reader_cls=sample_input_reader.SampleInputReader,
        input_reader_params={"count": 10, "string_length": 1.5},
        shard_count=99)
    self.assertRaises(errors.BadReaderParamsError,
                      sample_input_reader.SampleInputReader.validate,
                      conf)

  def testToFromJson(self):
    input_reader = sample_input_reader.SampleInputReader(10, 9)
    reader_in_json = input_reader.to_json()
    self.assertEquals({"count": 10, "string_length": 9}, reader_in_json)
    sample_input_reader.SampleInputReader.from_json(reader_in_json)
    self.assertEquals(10, input_reader._count)


if __name__ == "__main__":
  unittest.main()
