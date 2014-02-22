#!/usr/bin/env python
"""User API for controlling Map job execution."""

from google.appengine.api import taskqueue
from google.appengine.datastore import datastore_rpc
from google.appengine.ext import db
from mapreduce import model
from mapreduce import util

# pylint: disable=g-bad-name
# pylint: disable=protected-access


def start(job_config=None,
          in_xg_transaction=False):
  """Start a new map job.

  Args:
    job_config: an instance of map_job.MapJobConfig.
    in_xg_transaction: controls what transaction scope to use to start this MR
      job. If True, there has to be an already opened cross-group transaction
      scope. MR will use one entity group from it.
      If False, MR will create an independent transaction to start the job
      regardless of any existing transaction scopes.
  """
  # Validate input reader and output writer.
  mapper_spec = job_config._get_mapper_spec()
  job_config.input_reader_cls.validate(mapper_spec)
  if job_config.output_writer_cls:
    job_config.output_writer_cls.validate(mapper_spec)

  # Create mr spec.
  mapreduce_params = job_config._get_mr_params()
  mapreduce_spec = model.MapreduceSpec(
      job_config.job_name,
      job_config.job_id,
      mapper_spec.to_json(),
      mapreduce_params,
      util._obj_to_path(job_config._hooks_cls))

  # Save states and enqueue task.
  if in_xg_transaction:
    propagation = db.MANDATORY
  else:
    propagation = db.INDEPENDENT

  @db.transactional(propagation=propagation)
  def _txn():
    _create_and_save_state(job_config, mapreduce_spec)
    _add_kickoff_task(job_config, mapreduce_spec)

  _txn()


def _create_and_save_state(job_config, mapreduce_spec):
  """Save mapreduce state to datastore.

  Save state to datastore so that UI can see it immediately.

  Args:
    job_config: map_job.JobConfig.
    mapreduce_spec: model.MapreduceSpec,
  """
  state = model.MapreduceState.create_new(job_config.job_id)
  state.mapreduce_spec = mapreduce_spec
  state.active = True
  state.active_shards = 0
  state.app_id = job_config._app
  config = datastore_rpc.Configuration(force_writes=job_config._force_writes)
  state.put(config=config)


def _add_kickoff_task(job_config, mapreduce_spec):
  """Add kickoff task to taskqueue.

  Args:
    job_config: map_job.JobConfig.
    mapreduce_spec: model.MapreduceSpec,
  """
  params = {"mapreduce_id": job_config.job_id}
  # Task is not named so that it can be added within a transaction.
  kickoff_task = taskqueue.Task(
      # TODO(user): Perhaps make this url a computed field of job_config.
      url=job_config._base_path + "/kickoffjob_callback/" + job_config.job_id,
      headers=util._get_task_headers(job_config.job_id),
      params=params)
  if job_config._hooks_cls:
    hooks = job_config._hooks_cls(mapreduce_spec)
    try:
      hooks.enqueue_kickoff_task(kickoff_task, job_config.queue_name)
      return
    except NotImplementedError:
      pass
  kickoff_task.add(job_config.queue_name, transactional=True)
