#!/usr/bin/env python
# Copyright 2010 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Defines executor tasks handlers for MapReduce implementation."""



# pylint: disable=protected-access
# pylint: disable=g-bad-name

import datetime
import gc
import logging
import math
import os
import sys
import time
import traceback

from google.appengine.api import datastore_errors
from google.appengine.api import taskqueue
from google.appengine.ext import db
from mapreduce import base_handler
from mapreduce import context
from mapreduce import errors
from mapreduce import input_readers
from mapreduce import model
from mapreduce import operation
from mapreduce import parameters
from mapreduce import util

try:
  from google.appengine.ext import ndb
except ImportError:
  ndb = None


# The amount of time to perform scanning in one slice. New slice will be
# scheduled as soon as current one takes this long.
_SLICE_DURATION_SEC = 15

# Delay between consecutive controller callback invocations.
_CONTROLLER_PERIOD_SEC = 2

# How many times to cope with a RetrySliceError before totally
# giving up and aborting the whole job.
_RETRY_SLICE_ERROR_MAX_RETRIES = 10

# Set of strings of various test-injected faults.
_TEST_INJECTED_FAULTS = set()


def _run_task_hook(hooks, method, task, queue_name):
  """Invokes hooks.method(task, queue_name).

  Args:
    hooks: A hooks.Hooks instance or None.
    method: The name of the method to invoke on the hooks class e.g.
        "enqueue_kickoff_task".
    task: The taskqueue.Task to pass to the hook method.
    queue_name: The name of the queue to pass to the hook method.

  Returns:
    True if the hooks.Hooks instance handled the method, False otherwise.
  """
  if hooks is not None:
    try:
      getattr(hooks, method)(task, queue_name)
    except NotImplementedError:
      # Use the default task addition implementation.
      return False

    return True
  return False


class MapperWorkerCallbackHandler(base_handler.HugeTaskHandler):
  """Callback handler for mapreduce worker task.

  Request Parameters:
    mapreduce_spec: MapreduceSpec of the mapreduce serialized to json.
    shard_id: id of the shard.
    slice_id: id of the slice.
  """

  def __init__(self, *args):
    """Constructor."""
    super(MapperWorkerCallbackHandler, self).__init__(*args)
    self._time = time.time

  def handle(self):
    """Handle request."""
    tstate = model.TransientShardState.from_request(self.request)
    spec = tstate.mapreduce_spec
    self._start_time = self._time()
    shard_id = tstate.shard_id

    shard_state, control = db.get([
        model.ShardState.get_key_by_shard_id(shard_id),
        model.MapreduceControl.get_key_by_job_id(spec.mapreduce_id),
    ])
    if not shard_state:
      # We're letting this task to die. It's up to controller code to
      # reinitialize and restart the task.
      logging.error("State not found for shard %s; Possible spurious task "
                    "execution. Dropping this task.",
                    shard_id)
      return

    if not shard_state.active:
      logging.error("Shard %s is not active. Possible spurious task "
                    "execution. Dropping this task.", shard_id)
      logging.error(str(shard_state))
      return
    if shard_state.retries > tstate.retries:
      logging.error(
          "Got shard %s from previous shard retry %s. Possible spurious "
          "task execution. Dropping this task.",
          shard_id,
          tstate.retries)
      logging.error(str(shard_state))
      return
    elif shard_state.retries < tstate.retries:
      # This happens when the transaction that updates shardstate and enqueues
      # task fails after the task has been added. That transaction will
      # be retried. Adding the same task will result in
      # TaskAlreadyExistsError but the error is ignored.
      raise ValueError(
          "ShardState for %s is behind slice. Waiting for it to catch up",
          shard_state.shard_id)

    ctx = context.Context(spec, shard_state,
                          task_retry_count=self.task_retry_count())

    if control and control.command == model.MapreduceControl.ABORT:
      logging.info("Abort command received by shard %d of job '%s'",
                   shard_state.shard_number, shard_state.mapreduce_id)
      # NOTE: When aborting, specifically do not finalize the output writer
      # because it might be in a bad state.
      shard_state.active = False
      shard_state.result_status = model.ShardState.RESULT_ABORTED
      shard_state.put(config=util.create_datastore_write_config(spec))
      return

    input_reader = tstate.input_reader

    # Tell NDB to never cache anything in memcache or in-process. This ensures
    # that entities fetched from Datastore input_readers via NDB will not bloat
    # up the request memory size and Datastore Puts will avoid doing calls
    # to memcache. Without this you get soft memory limit exits, which hurts
    # overall throughput.
    if ndb is not None:
      ndb_ctx = ndb.get_context()
      ndb_ctx.set_cache_policy(lambda key: False)
      ndb_ctx.set_memcache_policy(lambda key: False)

    context.Context._set(ctx)
    retry_shard = False

    try:
      self.process_inputs(
          input_reader, shard_state, tstate, ctx)

      if not shard_state.active:
        # shard is going to stop. Finalize output writer only when shard is
        # successful because writer might be stuck in some bad state otherwise.
        if (shard_state.result_status == model.ShardState.RESULT_SUCCESS and
            tstate.output_writer):
          # It's possible that finalization is successful but
          # saving state failed. In this case this shard will retry upon
          # finalization error.
          # TODO(user): make finalize method idempotent!
          tstate.output_writer.finalize(ctx, shard_state)
    # pylint: disable=broad-except
    except Exception, e:
      retry_shard = self._retry_logic(e, shard_state, tstate, spec.mapreduce_id)
    finally:
      context.Context._set(None)

    self._save_state_and_schedule_next(shard_state, tstate, retry_shard)

  def process_inputs(self,
                     input_reader,
                     shard_state,
                     tstate,
                     ctx):
    """Read inputs, process them, and write out outputs.

    This is the core logic of MapReduce. It reads inputs from input reader,
    invokes user specified mapper function, and writes output with
    output writer. It also updates shard_state accordingly.
    e.g. if shard processing is done, set shard_state.active to False.

    If errors.FailJobError is caught, it will fail this MR job.
    All other exceptions will be logged and raised to taskqueue for retry
    until the number of retries exceeds a limit.

    Args:
      input_reader: input reader.
      shard_state: shard state.
      tstate: transient shard state.
      ctx: mapreduce context.
    """
    processing_limit = self._processing_limit(tstate.mapreduce_spec)
    if processing_limit == 0:
      return

    finished_shard = True

    for entity in input_reader:
      if isinstance(entity, db.Model):
        shard_state.last_work_item = repr(entity.key())
      elif ndb and isinstance(entity, ndb.Model):
        shard_state.last_work_item = repr(entity.key)
      else:
        shard_state.last_work_item = repr(entity)[:100]

      processing_limit -= 1

      if not self.process_data(
          entity, input_reader, ctx, tstate):
        finished_shard = False
        break
      elif processing_limit == 0:
        finished_shard = False
        break

    # Flush context and its pools.
    operation.counters.Increment(
        context.COUNTER_MAPPER_WALLTIME_MS,
        int((self._time() - self._start_time)*1000))(ctx)
    ctx.flush()

    if finished_shard:
      shard_state.active = False
      shard_state.result_status = model.ShardState.RESULT_SUCCESS

  def process_data(self, data, input_reader, ctx, transient_shard_state):
    """Process a single data piece.

    Call mapper handler on the data.

    Args:
      data: a datum to process.
      input_reader: input reader.
      ctx: mapreduce context
      transient_shard_state: transient shard state.

    Returns:
      True if scan should be continued, False if scan should be stopped.
    """
    if data is not input_readers.ALLOW_CHECKPOINT:
      ctx.counters.increment(context.COUNTER_MAPPER_CALLS)

      handler = transient_shard_state.handler

      if input_reader.expand_parameters:
        result = handler(*data)
      else:
        result = handler(data)

      if util.is_generator(result):
        for output in result:
          if isinstance(output, operation.Operation):
            output(ctx)
          else:
            output_writer = transient_shard_state.output_writer
            if not output_writer:
              logging.error(
                  "Handler yielded %s, but no output writer is set.", output)
            else:
              output_writer.write(output, ctx)

    if self._time() - self._start_time > _SLICE_DURATION_SEC:
      return False
    return True

  def _save_state_and_schedule_next(self, shard_state, tstate, retry_shard):
    """Save state to datastore and schedule next task for this shard.

    Args:
      shard_state: model.ShardState for current shard.
      tstate: model.TransientShardState for current shard.
      retry_shard: whether to retry shard.
    """
    spec = tstate.mapreduce_spec
    config = util.create_datastore_write_config(spec)

    # We don't want shard state to override active state, since that
    # may stuck job execution (see issue 116). Do a transactional
    # verification for status.
    @db.transactional(retries=5)
    def _tx():
      fresh_shard_state = db.get(
          model.ShardState.get_key_by_shard_id(tstate.shard_id))
      if not fresh_shard_state:
        raise db.Rollback()
      if (not fresh_shard_state.active or
          "worker_active_state_collision" in _TEST_INJECTED_FAULTS):
        logging.error("Shard %s is not active. Possible spurious task "
                      "execution. Dropping this task.", tstate.shard_id)
        logging.error("Datastore's %s", str(fresh_shard_state))
        logging.error("Slice's %s", str(shard_state))
        return
      fresh_shard_state.copy_from(shard_state)
      fresh_shard_state.put(config=config)
      if retry_shard:
        self._schedule_slice(fresh_shard_state, tstate)
      elif shard_state.active:
        self.reschedule(fresh_shard_state, tstate)

    try:
      _tx()
    except (datastore_errors.Timeout,
            datastore_errors.TransactionFailedError,
            datastore_errors.InternalError), e:
      logging.error(
          "Can't reach datastore. Will retry slice %s %s for the %s time.",
          tstate.shard_id,
          tstate.slice_id,
          self.task_retry_count() + 1)
      raise e
    finally:
      gc.collect()

  def _retry_logic(self, e, shard_state, tstate, mr_id):
    """Handle retry for this slice.

    This method may modify shard_state and tstate to prepare for retry or fail.

    Args:
      e: the exception caught.
      shard_state: model.ShardState for current shard.
      tstate: model.TransientShardState for current shard.
      mr_id: mapreduce id.

    Returns:
      True if shard should be retried. False otherwise.

    Raises:
      errors.RetrySliceError: in order to trigger a slice retry.
    """
    logging.error("Shard %s got error.", shard_state.shard_id)
    # This logs the callstack leading up to the exception. Thus it excludes the
    # call frames generated to handle the retry.
    logging.error(traceback.format_exc())

    # Fail fast.
    if type(e) is errors.FailJobError:
      logging.error("Got FailJobError. Shard %s failed permanently.",
                    shard_state.shard_id)
      shard_state.active = False
      shard_state.result_status = model.ShardState.RESULT_FAILED
      return False

    if type(e) in errors.SHARD_RETRY_ERRORS:
      return self._attempt_shard_retry(shard_state, tstate, mr_id)
    else:
      return self._attempt_slice_retry(shard_state, tstate)

  def _attempt_shard_retry(self, shard_state, tstate, mr_id):
    """Whether to retry shard.

    This method may modify shard_state and tstate to prepare for retry or fail.

    Args:
      shard_state: model.ShardState for current shard.
      tstate: model.TransientShardState for current shard.
      mr_id: mapreduce id.

    Returns:
      True if shard should be retried. False otherwise.
    """
    shard_retry = shard_state.retries
    permanent_shard_failure = False
    if shard_retry >= parameters.DEFAULT_SHARD_RETRY_LIMIT:
      logging.error(
          "Shard has been retried %s times. Shard %s will fail permanently.",
          shard_retry, shard_state.shard_id)
      permanent_shard_failure = True

    if tstate.output_writer and (
        not tstate.output_writer._can_be_retried(tstate)):
      logging.error("Can not retry shard. Shard %s failed permanently.",
                    shard_state.shard_id)
      permanent_shard_failure = True

    if permanent_shard_failure:
      shard_state.active = False
      shard_state.result_status = model.ShardState.RESULT_FAILED
      return False

    shard_state.reset_for_retry()
    logging.error("Shard %s will be retried for the %s time.",
                  shard_state.shard_id,
                  shard_state.retries)
    output_writer = None
    if tstate.output_writer:
      mr_state = model.MapreduceState.get_by_job_id(mr_id)
      output_writer = tstate.output_writer.create(
          mr_state, shard_state)
    tstate.reset_for_retry(output_writer)
    return True

  def _attempt_slice_retry(self, shard_state, tstate):
    """Attempt to retry this slice.

    This method may modify shard_state and tstate to prepare for retry or fail.

    Args:
      shard_state: model.ShardState for current shard.
      tstate: model.TransientShardState for current shard.

    Returns:
      False when slice can't be retried anymore.

    Raises:
      errors.RetrySliceError: in order to trigger a slice retry.
    """
    slice_retry = self.task_retry_count()
    if slice_retry < _RETRY_SLICE_ERROR_MAX_RETRIES:
      logging.error(
          "Will retry slice %s %s for the %s time.",
          tstate.shard_id,
          tstate.slice_id,
          slice_retry + 1)
      # Clear info related to current exception. Otherwise, the real
      # callstack that includes a frame for this method will show up
      # in log.
      sys.exc_clear()
      raise errors.RetrySliceError("Raise an error to trigger slice retry")

    logging.error("Slice reached max retry limit of %s. "
                  "Shard %s failed permanently.",
                  self.task_retry_count(),
                  shard_state.shard_id)
    shard_state.active = False
    shard_state.result_status = model.ShardState.RESULT_FAILED
    return False

  @staticmethod
  def get_task_name(shard_id, slice_id, retry=0):
    """Compute single worker task name.

    Args:
      transient_shard_state: An instance of TransientShardState.

    Returns:
      task name which should be used to process specified shard/slice.
    """
    # Prefix the task name with something unique to this framework's
    # namespace so we don't conflict with user tasks on the queue.
    return "appengine-mrshard-%s-%s-retry-%s" % (
        shard_id, slice_id, retry)

  def reschedule(self, shard_state, tstate):
    """Reschedule worker task to continue scanning work.

    Args:
      tstate: an instance of TransientShardState.
    """
    tstate.slice_id += 1
    spec = tstate.mapreduce_spec
    countdown = 0
    if self._processing_limit(spec) != -1:
      countdown = max(
          int(_SLICE_DURATION_SEC - (self._time() - self._start_time)), 0)
    MapperWorkerCallbackHandler._schedule_slice(
        shard_state, tstate, countdown=countdown)

  @classmethod
  def _schedule_slice(cls,
                      shard_state,
                      transient_shard_state,
                      queue_name=None,
                      eta=None,
                      countdown=None):
    """Schedule slice scanning by adding it to the task queue.

    Args:
      shard_state: An instance of ShardState.
      transient_shard_state: An instance of TransientShardState.
      queue_name: Optional queue to run on; uses the current queue of
        execution or the default queue if unspecified.
      eta: Absolute time when the MR should execute. May not be specified
        if 'countdown' is also supplied. This may be timezone-aware or
        timezone-naive.
      countdown: Time in seconds into the future that this MR should execute.
        Defaults to zero.
    """
    base_path = transient_shard_state.base_path
    mapreduce_spec = transient_shard_state.mapreduce_spec

    task_name = MapperWorkerCallbackHandler.get_task_name(
        transient_shard_state.shard_id,
        transient_shard_state.slice_id,
        transient_shard_state.retries)
    queue_name = queue_name or os.environ.get("HTTP_X_APPENGINE_QUEUENAME",
                                              "default")

    worker_task = util.HugeTask(url=base_path + "/worker_callback",
                                params=transient_shard_state.to_dict(),
                                name=task_name,
                                eta=eta,
                                countdown=countdown)

    if not _run_task_hook(mapreduce_spec.get_hooks(),
                          "enqueue_worker_task",
                          worker_task,
                          queue_name):
      try:
        worker_task.add(queue_name, parent=shard_state)
      except (taskqueue.TombstonedTaskError,
              taskqueue.TaskAlreadyExistsError), e:
        logging.warning("Task %r already exists. %s: %s",
                        task_name,
                        e.__class__,
                        e)
      except (taskqueue.TransientError,
              taskqueue.InternalError), e:
        logging.error(
            "Slice %s got error."
            "Can't reach taskqueue service. Slice will be retried.", task_name)
        raise e

  def _processing_limit(self, spec):
    """Get the limit on the number of map calls allowed by this slice.

    Args:
      spec: a Mapreduce spec.

    Returns:
      The limit as a positive int if specified by user. -1 otherwise.
    """
    processing_rate = float(spec.mapper.params.get("processing_rate", 0))
    slice_processing_limit = -1
    if processing_rate > 0:
      slice_processing_limit = int(math.ceil(
          _SLICE_DURATION_SEC*processing_rate/int(spec.mapper.shard_count)))
    return slice_processing_limit


class ControllerCallbackHandler(base_handler.HugeTaskHandler):
  """Supervises mapreduce execution.

  Is also responsible for gathering execution status from shards together.

  This task is "continuously" running by adding itself again to taskqueue if
  mapreduce is still active.
  """

  def __init__(self, *args):
    """Constructor."""
    super(ControllerCallbackHandler, self).__init__(*args)
    self._time = time.time

  def handle(self):
    """Handle request."""
    spec = model.MapreduceSpec.from_json_str(
        self.request.get("mapreduce_spec"))
    state, control = db.get([
        model.MapreduceState.get_key_by_job_id(spec.mapreduce_id),
        model.MapreduceControl.get_key_by_job_id(spec.mapreduce_id),
    ])

    if not state:
      logging.error("State not found for MR '%s'; dropping controller task.",
                    spec.mapreduce_id)
      return
    if not state.active:
      logging.info(
          "MR %r is not active. Looks like spurious controller task execution.",
          spec.mapreduce_id)
      self._clean_up_mr(spec, self.base_path())
      return

    shard_states = model.ShardState.find_by_mapreduce_state(state)
    if len(shard_states) != spec.mapper.shard_count:
      logging.error("Found %d shard states. Expect %d. "
                    "Issuing abort command to job '%s'",
                    len(shard_states), spec.mapper.shard_count,
                    spec.mapreduce_id)
      # We issue abort command to allow shards to stop themselves.
      model.MapreduceControl.abort(spec.mapreduce_id)

    self._update_state_from_shard_states(state, shard_states, control)

    if state.active:
      ControllerCallbackHandler.reschedule(
          state, self.base_path(), spec, self.serial_id() + 1)

  def _update_state_from_shard_states(self, state, shard_states, control):
    """Update mr state by examing shard states.

    Args:
      state: current mapreduce state as MapreduceState.
      shard_states: all shard states (active and inactive). list of ShardState.
      control: model.MapreduceControl entity.
    """
    active_shards = [s for s in shard_states if s.active]
    failed_shards = [s for s in shard_states
                     if s.result_status == model.ShardState.RESULT_FAILED]
    aborted_shards = [s for s in shard_states
                     if s.result_status == model.ShardState.RESULT_ABORTED]
    spec = state.mapreduce_spec

    # If any shard is active then the mr is active.
    # This way, controller won't prematurely stop before all the shards have.
    state.active = bool(active_shards)
    state.active_shards = len(active_shards)
    state.failed_shards = len(failed_shards)
    state.aborted_shards = len(aborted_shards)
    if not control and (failed_shards or aborted_shards):
      # Issue abort command if there are failed shards.
      model.MapreduceControl.abort(spec.mapreduce_id)

    self._aggregate_stats(state, shard_states)
    state.last_poll_time = datetime.datetime.utcfromtimestamp(self._time())

    if not state.active:
      # Set final result status derived from shard states.
      if failed_shards or not shard_states:
        state.result_status = model.MapreduceState.RESULT_FAILED
      # It's important failed shards is checked before aborted shards
      # because failed shards will trigger other shards to abort.
      elif aborted_shards:
        state.result_status = model.MapreduceState.RESULT_ABORTED
      else:
        state.result_status = model.MapreduceState.RESULT_SUCCESS
      self._finalize_job(spec, state, self.base_path())
    else:
      # We don't need a transaction here, since we change only statistics data,
      # and we don't care if it gets overwritten/slightly inconsistent.
      config = util.create_datastore_write_config(spec)
      state.put(config=config)

  def _aggregate_stats(self, mapreduce_state, shard_states):
    """Update stats in mapreduce state by aggregating stats from shard states.

    Args:
      mapreduce_state: current mapreduce state as MapreduceState.
      shard_states: all shard states (active and inactive). list of ShardState.
    """
    processed_counts = []
    mapreduce_state.counters_map.clear()

    for shard_state in shard_states:
      mapreduce_state.counters_map.add_map(shard_state.counters_map)
      processed_counts.append(shard_state.counters_map.get(
          context.COUNTER_MAPPER_CALLS))

    mapreduce_state.set_processed_counts(processed_counts)

  def serial_id(self):
    """Get serial unique identifier of this task from request.

    Returns:
      serial identifier as int.
    """
    return int(self.request.get("serial_id"))

  @classmethod
  def _finalize_job(cls, mapreduce_spec, mapreduce_state, base_path):
    """Finalize job execution.

    Finalizes output writer, invokes done callback and save mapreduce state
    in a transaction, and schedule necessary clean ups.

    Args:
      mapreduce_spec: an instance of MapreduceSpec
      mapreduce_state: an instance of MapreduceState
      base_path: handler_base path.
    """
    config = util.create_datastore_write_config(mapreduce_spec)

    # Only finalize the output writers if the job is successful.
    if (mapreduce_spec.mapper.output_writer_class() and
        mapreduce_state.result_status == model.MapreduceState.RESULT_SUCCESS):
      mapreduce_spec.mapper.output_writer_class().finalize_job(mapreduce_state)

    queue_name = mapreduce_spec.params.get(
        model.MapreduceSpec.PARAM_DONE_CALLBACK_QUEUE,
        "default")
    done_callback = mapreduce_spec.params.get(
        model.MapreduceSpec.PARAM_DONE_CALLBACK)
    done_task = None
    if done_callback:
      done_task = taskqueue.Task(
          url=done_callback,
          headers={"Mapreduce-Id": mapreduce_spec.mapreduce_id},
          method=mapreduce_spec.params.get("done_callback_method", "POST"))

    def put_state(state):
      state.put(config=config)
      # Enqueue done_callback if needed.
      if done_task and not _run_task_hook(
          mapreduce_spec.get_hooks(),
          "enqueue_done_task",
          done_task,
          queue_name):
        done_task.add(queue_name, transactional=True)

    logging.info("Final result for job '%s' is '%s'",
                 mapreduce_spec.mapreduce_id, mapreduce_state.result_status)
    db.run_in_transaction_custom_retries(5, put_state, mapreduce_state)
    cls._clean_up_mr(mapreduce_spec, base_path)

  @classmethod
  def _clean_up_mr(cls, mapreduce_spec, base_path):
    FinalizeJobHandler.schedule(base_path, mapreduce_spec)

  @staticmethod
  def get_task_name(mapreduce_spec, serial_id):
    """Compute single controller task name.

    Args:
      transient_shard_state: an instance of TransientShardState.

    Returns:
      task name which should be used to process specified shard/slice.
    """
    # Prefix the task name with something unique to this framework's
    # namespace so we don't conflict with user tasks on the queue.
    return "appengine-mrcontrol-%s-%s" % (
        mapreduce_spec.mapreduce_id, serial_id)

  @staticmethod
  def controller_parameters(mapreduce_spec, serial_id):
    """Fill in  controller task parameters.

    Returned parameters map is to be used as task payload, and it contains
    all the data, required by controller to perform its function.

    Args:
      mapreduce_spec: specification of the mapreduce.
      serial_id: id of the invocation as int.

    Returns:
      string->string map of parameters to be used as task payload.
    """
    return {"mapreduce_spec": mapreduce_spec.to_json_str(),
            "serial_id": str(serial_id)}

  @classmethod
  def reschedule(cls,
                 mapreduce_state,
                 base_path,
                 mapreduce_spec,
                 serial_id,
                 queue_name=None):
    """Schedule new update status callback task.

    Args:
      mapreduce_state: mapreduce state as model.MapreduceState
      base_path: mapreduce handlers url base path as string.
      mapreduce_spec: mapreduce specification as MapreduceSpec.
      serial_id: id of the invocation as int.
      queue_name: The queue to schedule this task on. Will use the current
        queue of execution if not supplied.
    """
    task_name = ControllerCallbackHandler.get_task_name(
        mapreduce_spec, serial_id)
    task_params = ControllerCallbackHandler.controller_parameters(
        mapreduce_spec, serial_id)
    if not queue_name:
      queue_name = os.environ.get("HTTP_X_APPENGINE_QUEUENAME", "default")

    controller_callback_task = util.HugeTask(
        url=base_path + "/controller_callback",
        name=task_name, params=task_params,
        countdown=_CONTROLLER_PERIOD_SEC)

    if not _run_task_hook(mapreduce_spec.get_hooks(),
                          "enqueue_controller_task",
                          controller_callback_task,
                          queue_name):
      try:
        controller_callback_task.add(queue_name, parent=mapreduce_state)
      except (taskqueue.TombstonedTaskError,
              taskqueue.TaskAlreadyExistsError), e:
        logging.warning("Task %r with params %r already exists. %s: %s",
                        task_name, task_params, e.__class__, e)


class KickOffJobHandler(base_handler.HugeTaskHandler):
  """Taskqueue handler which kicks off a mapreduce processing.

  Request Parameters:
    mapreduce_spec: MapreduceSpec of the mapreduce serialized to json.
    input_readers: List of InputReaders objects separated by semi-colons.
  """

  def handle(self):
    """Handles kick off request."""
    spec = model.MapreduceSpec.from_json_str(
        self._get_required_param("mapreduce_spec"))

    app_id = self.request.get("app", None)
    queue_name = os.environ.get("HTTP_X_APPENGINE_QUEUENAME", "default")
    mapper_input_reader_class = spec.mapper.input_reader_class()

    # StartJobHandler might have already saved the state, but it's OK
    # to override it because we're using the same mapreduce id.
    state = model.MapreduceState.create_new(spec.mapreduce_id)
    state.mapreduce_spec = spec
    state.active = True
    if app_id:
      state.app_id = app_id

    input_readers = mapper_input_reader_class.split_input(spec.mapper)
    if not input_readers:
      # We don't have any data. Finish map.
      logging.warning("Found no mapper input data to process.")
      state.active = False
      state.active_shards = 0
      ControllerCallbackHandler._finalize_job(spec, state, self.base_path())
      return

    # Update state and spec with actual shard count.
    spec.mapper.shard_count = len(input_readers)
    state.active_shards = len(input_readers)
    state.mapreduce_spec = spec

    output_writer_class = spec.mapper.output_writer_class()
    if output_writer_class:
      output_writer_class.init_job(state)

    state.put(config=util.create_datastore_write_config(spec))

    KickOffJobHandler._schedule_shards(
        spec, input_readers, queue_name, self.base_path(), state)

    ControllerCallbackHandler.reschedule(
        state, self.base_path(), spec, queue_name=queue_name, serial_id=0)

  def _get_required_param(self, param_name):
    """Get a required request parameter.

    Args:
      param_name: name of request parameter to fetch.

    Returns:
      parameter value

    Raises:
      errors.NotEnoughArgumentsError: if parameter is not specified.
    """
    value = self.request.get(param_name)
    if not value:
      raise errors.NotEnoughArgumentsError(param_name + " not specified")
    return value

  @classmethod
  def _schedule_shards(cls,
                       spec,
                       input_readers,
                       queue_name,
                       base_path,
                       mr_state):
    """Prepares shard states and schedules their execution.

    Args:
      spec: mapreduce specification as MapreduceSpec.
      input_readers: list of InputReaders describing shard splits.
      queue_name: The queue to run this job on.
      base_path: The base url path of mapreduce callbacks.
      mr_state: The MapReduceState of current job.
    """
    # Note: it's safe to re-attempt this handler because:
    # - shard state has deterministic and unique key.
    # - _schedule_slice will fall back gracefully if a task already exists.
    shard_states = []
    writer_class = spec.mapper.output_writer_class()
    output_writers = [None] * len(input_readers)
    for shard_number, input_reader in enumerate(input_readers):
      shard_state = model.ShardState.create_new(spec.mapreduce_id, shard_number)
      shard_state.shard_description = str(input_reader)
      if writer_class:
        output_writers[shard_number] = writer_class.create(
            mr_state, shard_state)
      shard_states.append(shard_state)

    # Retrievs already existing shards.
    existing_shard_states = db.get(shard.key() for shard in shard_states)
    existing_shard_keys = set(shard.key() for shard in existing_shard_states
                              if shard is not None)

    # Puts only non-existing shards.
    db.put((shard for shard in shard_states
            if shard.key() not in existing_shard_keys),
           config=util.create_datastore_write_config(spec))

    # Schedule shard tasks.
    for shard_number, (input_reader, output_writer) in enumerate(
        zip(input_readers, output_writers)):
      shard_id = model.ShardState.shard_id_from_number(
          spec.mapreduce_id, shard_number)
      MapperWorkerCallbackHandler._schedule_slice(
          shard_states[shard_number],
          model.TransientShardState(
              base_path, spec, shard_id, 0, input_reader, input_reader,
              output_writer=output_writer),
          queue_name=queue_name)


class StartJobHandler(base_handler.PostJsonHandler):
  """Command handler starts a mapreduce job."""

  def handle(self):
    """Handles start request."""
    # Mapper spec as form arguments.
    mapreduce_name = self._get_required_param("name")
    mapper_input_reader_spec = self._get_required_param("mapper_input_reader")
    mapper_handler_spec = self._get_required_param("mapper_handler")
    mapper_output_writer_spec = self.request.get("mapper_output_writer")
    mapper_params = self._get_params(
        "mapper_params_validator", "mapper_params.")
    params = self._get_params(
        "params_validator", "params.")

    # Set some mapper param defaults if not present.
    mapper_params["processing_rate"] = int(mapper_params.get(
          "processing_rate") or model._DEFAULT_PROCESSING_RATE_PER_SEC)
    queue_name = mapper_params["queue_name"] = mapper_params.get(
        "queue_name", "default")

    # Validate the Mapper spec, handler, and input reader.
    mapper_spec = model.MapperSpec(
        mapper_handler_spec,
        mapper_input_reader_spec,
        mapper_params,
        int(mapper_params.get("shard_count", model._DEFAULT_SHARD_COUNT)),
        output_writer_spec=mapper_output_writer_spec)

    mapreduce_id = type(self)._start_map(
        mapreduce_name,
        mapper_spec,
        params,
        base_path=self.base_path(),
        queue_name=queue_name,
        _app=mapper_params.get("_app"))
    self.json_response["mapreduce_id"] = mapreduce_id

  def _get_params(self, validator_parameter, name_prefix):
    """Retrieves additional user-supplied params for the job and validates them.

    Args:
      validator_parameter: name of the request parameter which supplies
        validator for this parameter set.
      name_prefix: common prefix for all parameter names in the request.

    Raises:
      Any exception raised by the 'params_validator' request parameter if
      the params fail to validate.
    """
    params_validator = self.request.get(validator_parameter)

    user_params = {}
    for key in self.request.arguments():
      if key.startswith(name_prefix):
        values = self.request.get_all(key)
        adjusted_key = key[len(name_prefix):]
        if len(values) == 1:
          user_params[adjusted_key] = values[0]
        else:
          user_params[adjusted_key] = values

    if params_validator:
      resolved_validator = util.for_name(params_validator)
      resolved_validator(user_params)

    return user_params

  def _get_required_param(self, param_name):
    """Get a required request parameter.

    Args:
      param_name: name of request parameter to fetch.

    Returns:
      parameter value

    Raises:
      errors.NotEnoughArgumentsError: if parameter is not specified.
    """
    value = self.request.get(param_name)
    if not value:
      raise errors.NotEnoughArgumentsError(param_name + " not specified")
    return value

  @classmethod
  def _start_map(cls,
                 name,
                 mapper_spec,
                 mapreduce_params,
                 base_path=None,
                 queue_name=None,
                 eta=None,
                 countdown=None,
                 hooks_class_name=None,
                 _app=None,
                 transactional=False,
                 parent_entity=None):
    queue_name = queue_name or os.environ.get("HTTP_X_APPENGINE_QUEUENAME",
                                              "default")
    if queue_name[0] == "_":
      # We are currently in some special queue. E.g. __cron.
      queue_name = "default"

    if not transactional and parent_entity:
      raise Exception("Parent shouldn't be specfied "
                      "for non-transactional starts.")

    # Check that reader can be instantiated and is configured correctly
    mapper_input_reader_class = mapper_spec.input_reader_class()
    mapper_input_reader_class.validate(mapper_spec)

    mapper_output_writer_class = mapper_spec.output_writer_class()
    if mapper_output_writer_class:
      mapper_output_writer_class.validate(mapper_spec)

    mapreduce_id = model.MapreduceState.new_mapreduce_id()
    mapreduce_spec = model.MapreduceSpec(
        name,
        mapreduce_id,
        mapper_spec.to_json(),
        mapreduce_params,
        hooks_class_name)

    # Check that handler can be instantiated.
    ctx = context.Context(mapreduce_spec, None)
    context.Context._set(ctx)
    try:
      # pylint: disable=pointless-statement
      mapper_spec.handler
    finally:
      context.Context._set(None)

    kickoff_params = {"mapreduce_spec": mapreduce_spec.to_json_str()}
    if _app:
      kickoff_params["app"] = _app
    kickoff_worker_task = util.HugeTask(
        url=base_path + "/kickoffjob_callback",
        params=kickoff_params,
        eta=eta,
        countdown=countdown)

    hooks = mapreduce_spec.get_hooks()
    config = util.create_datastore_write_config(mapreduce_spec)

    def start_mapreduce():
      parent = parent_entity
      if not transactional:
        # Save state in datastore so that UI can see it.
        # We can't save state in foreign transaction, but conventional UI
        # doesn't ask for transactional starts anyway.
        state = model.MapreduceState.create_new(mapreduce_spec.mapreduce_id)
        state.mapreduce_spec = mapreduce_spec
        state.active = True
        state.active_shards = mapper_spec.shard_count
        if _app:
          state.app_id = _app
        state.put(config=config)
        parent = state

      if hooks is not None:
        try:
          hooks.enqueue_kickoff_task(kickoff_worker_task, queue_name)
        except NotImplementedError:
          # Use the default task addition implementation.
          pass
        else:
          return
      kickoff_worker_task.add(queue_name, transactional=True, parent=parent)

    if transactional:
      start_mapreduce()
    else:
      db.run_in_transaction(start_mapreduce)

    return mapreduce_id


class FinalizeJobHandler(base_handler.TaskQueueHandler):
  """Finalize map job by deleting all temporary entities."""

  def handle(self):
    mapreduce_id = self.request.get("mapreduce_id")
    mapreduce_state = model.MapreduceState.get_by_job_id(mapreduce_id)
    if mapreduce_state:
      config=util.create_datastore_write_config(mapreduce_state.mapreduce_spec)
      db.delete(model.MapreduceControl.get_key_by_job_id(mapreduce_id),
              config=config)
      shard_states = model.ShardState.find_by_mapreduce_state(mapreduce_state)
      for shard_state in shard_states:
        db.delete(util._HugeTaskPayload.all().ancestor(shard_state),
                  config=config)
      db.delete(util._HugeTaskPayload.all().ancestor(mapreduce_state),
                config=config)

  @classmethod
  def schedule(cls, base_path, mapreduce_spec):
    """Schedule finalize task.

    Args:
      mapreduce_spec: mapreduce specification as MapreduceSpec.
    """
    task_name = mapreduce_spec.mapreduce_id + "-finalize"
    finalize_task = taskqueue.Task(
        name=task_name,
        url=base_path + "/finalizejob_callback",
        params={"mapreduce_id": mapreduce_spec.mapreduce_id})
    queue_name = os.environ.get("HTTP_X_APPENGINE_QUEUENAME", "default")
    if not _run_task_hook(mapreduce_spec.get_hooks(),
                          "enqueue_controller_task",
                          finalize_task,
                          queue_name):
      try:
        finalize_task.add(queue_name)
      except (taskqueue.TombstonedTaskError,
              taskqueue.TaskAlreadyExistsError), e:
        logging.warning("Task %r already exists. %s: %s",
                        task_name, e.__class__, e)


class CleanUpJobHandler(base_handler.PostJsonHandler):
  """Command to kick off tasks to clean up a job's data."""

  def handle(self):
    mapreduce_id = self.request.get("mapreduce_id")

    mapreduce_state = model.MapreduceState.get_by_job_id(mapreduce_id)
    if mapreduce_state:
      shard_keys = model.ShardState.calculate_keys_by_mapreduce_state(
          mapreduce_state)
      db.delete(shard_keys)
      db.delete(mapreduce_state)
    self.json_response["status"] = ("Job %s successfully cleaned up." %
                                    mapreduce_id)


class AbortJobHandler(base_handler.PostJsonHandler):
  """Command to abort a running job."""

  def handle(self):
    model.MapreduceControl.abort(self.request.get("mapreduce_id"))
    self.json_response["status"] = "Abort signal sent."
