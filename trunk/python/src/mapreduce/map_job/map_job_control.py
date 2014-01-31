#!/usr/bin/env python
"""User API for controlling Map job execution."""

from google.appengine.ext import db
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

  Returns:
    the id of this map job.

  Raises:
    ValueError: when in_xg_transaction is True but no transaction scope is
      detected.
  """
  if in_xg_transaction and not db.is_in_transaction():
    raise ValueError("Expects an opened xg transaction to start mapreduce.")

  # Break circular dependency.
  # pylint: disable=g-import-not-at-top
  from mapreduce import handlers

  return handlers.StartJobHandler._start_map(
      name=job_config.job_name,
      mapper_spec=job_config._get_mapper_spec(),
      mapreduce_params=job_config._get_mr_params(),
      queue_name=job_config.queue_name,
      hooks_class_name=util._obj_to_path(job_config._hooks_cls),
      _app=job_config._app,
      in_xg_transaction=in_xg_transaction)
