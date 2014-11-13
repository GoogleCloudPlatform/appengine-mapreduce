#!/usr/bin/env python
import unittest

from mapreduce import hooks
from mapreduce import model
from mapreduce import test_support
from testlib import testutil
from mapreduce import util
from mapreduce.api import map_job
from mapreduce.api.map_job import map_job_config
from mapreduce.api.map_job import sample_input_reader

# pylint: disable=invalid-name
# pylint: disable=protected-access

# The count to specify for the sample input reader.
TEST_SAMPLE_INPUT_READER_COUNT = 100


class TestHooks(hooks.Hooks):
  """Test hooks class."""

  enqueue_kickoff_task_calls = []

  def enqueue_kickoff_task(self, task, queue_name):
    TestHooks.enqueue_kickoff_task_calls.append((task, queue_name))
    task.add(queue_name=queue_name)


class MapJobStartTest(testutil.HandlerTestBase):
  """Test For start."""

  def setUp(self):
    super(MapJobStartTest, self).setUp()
    TestHooks.enqueue_kickoff_task_calls = []
    self.config = map_job.JobConfig(
        job_name="test_map",
        shard_count=1,
        mapper=map_job.Mapper,
        input_reader_cls=sample_input_reader.SampleInputReader,
        input_reader_params={"count": TEST_SAMPLE_INPUT_READER_COUNT},
        queue_name="crazy-queue",
        _base_path="/mr_base",
        _force_writes=True,
        shard_max_attempts=5,
        _task_max_attempts=6,
        done_callback_url="www.google.com",
        _hooks_cls=TestHooks)

  def validate_map_started(self):
    # Only one kickoff task.
    tasks = self.taskqueue.GetTasks(self.config.queue_name)
    self.assertEqual(1, len(tasks))
    self.taskqueue.FlushQueue(self.config.queue_name)
    # Hook was run.
    self.assertEqual(1, len(TestHooks.enqueue_kickoff_task_calls))

    # Check the task.
    task = tasks[0]
    self.assertTrue(task["url"].startswith(self.config._base_path))
    # Check task header.
    headers = dict(task["headers"])
    self.assertEqual(self.config.job_id, headers[util._MR_ID_TASK_HEADER])
    # Check task payload.
    task_mr_id = test_support.decode_task_payload(task).get("mapreduce_id")
    self.assertEqual(self.config.job_id, task_mr_id)

    # Check state.
    state = model.MapreduceState.get_by_job_id(self.config.job_id)
    self.assertTrue(state.active)
    self.assertEqual(0, state.active_shards)

    test_support.execute_task(task)

    # controller + shard tasks.
    tasks = self.taskqueue.GetTasks(self.config.queue_name)
    self.assertEqual(1 + self.config.shard_count, len(tasks))

    state = model.MapreduceState.get_by_job_id(self.config.job_id)
    self.assertEqual("__main__.TestHooks",
                     state.mapreduce_spec.hooks_class_name)

    # Verify mapreduce_spec.mapper_spec
    mapper_spec = state.mapreduce_spec.mapper
    self.assertEqual("mapreduce.api.map_job."
                     "mapper.Mapper",
                     mapper_spec.handler_spec)
    self.assertEqual("mapreduce.api.map_job."
                     "sample_input_reader.SampleInputReader",
                     mapper_spec.input_reader_spec)
    self.assertEqual(self.config.input_reader_params,
                     {"count": TEST_SAMPLE_INPUT_READER_COUNT})
    # Verify mapreduce_spec.params.
    self.assertEqual(self.config.queue_name,
                     state.mapreduce_spec.params["queue_name"])
    self.assertEqual(self.config._force_writes,
                     state.mapreduce_spec.params["force_writes"])
    self.assertEqual(self.config.done_callback_url,
                     state.mapreduce_spec.params["done_callback"])
    self.assertEqual(self.config._base_path,
                     state.mapreduce_spec.params["base_path"])
    self.assertEqual(self.config.shard_max_attempts,
                     state.mapreduce_spec.params["shard_max_attempts"])
    self.assertEqual(self.config._task_max_attempts,
                     state.mapreduce_spec.params["task_max_attempts"])
    self.assertEqual(self.config._api_version,
                     state.mapreduce_spec.params["api_version"])
    self.assertEqual(self.config._api_version, map_job_config._API_VERSION)

  def testStartMap(self):
    map_job.Job.submit(self.config)
    self.validate_map_started()


class MapJobStatusTest(testutil.HandlerTestBase):
  """Test For Job status."""

  def setUp(self):
    super(MapJobStatusTest, self).setUp()
    self.config = map_job.JobConfig(
        job_name="test_map",
        shard_count=1,
        mapper=map_job.Mapper,
        input_reader_cls=sample_input_reader.SampleInputReader,
        input_reader_params={"count": TEST_SAMPLE_INPUT_READER_COUNT})

  def testGetStatus(self):
    job = map_job.Job.submit(self.config)
    self.assertEqual(map_job.Job.RUNNING, job.get_status())
    self.assertEqual(self.config, job.job_config)

    # Execute Kickoff task.
    tasks = self.taskqueue.GetTasks(self.config.queue_name)
    self.taskqueue.FlushQueue(self.config.queue_name)
    task = tasks[0]
    test_support.execute_task(task)

    job = map_job.Job.get_job_by_id(job_id=self.config.job_id)
    self.assertEqual(map_job.Job.RUNNING, job.get_status())
    self.assertEqual(self.config, job.job_config)

    # Execute all tasks.
    test_support.execute_until_empty(self.taskqueue)
    # Old job instance will get most up to date status from db.
    self.assertEqual(map_job.Job.SUCCESS, job.get_status())

  def testAbort(self):
    job = map_job.Job.submit(self.config)
    self.assertEqual(map_job.Job.RUNNING, job.get_status())
    job.abort()
    self.assertEqual(map_job.Job.RUNNING, job.get_status())

    # Execute all tasks.
    test_support.execute_until_empty(self.taskqueue)
    job = map_job.Job.get_job_by_id(job_id=self.config.job_id)
    self.assertEqual(map_job.Job.ABORTED, job.get_status())


if __name__ == "__main__":
  unittest.main()
