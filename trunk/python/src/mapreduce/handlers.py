#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
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




import google



import datetime
import logging
import math
import os
from mapreduce.lib import simplejson
import time

from google.appengine.api import memcache
from google.appengine.api.labs import taskqueue
from google.appengine.ext import db
from mapreduce import base_handler
from mapreduce import context
from mapreduce import quota
from mapreduce import model
from mapreduce import quota
from mapreduce import util


_QUOTA_BATCH_SIZE = 20

_SLICE_DURATION_SEC = 15


class Error(Exception):
  """Base class for exceptions in this module."""


class NotEnoughArgumentsError(Error):
  """Required argument is missing."""


class NoDataError(Error):
  """There is no data present for a desired input."""


class MapperWorkerCallbackHandler(base_handler.BaseHandler):
  """Callback handler for mapreduce worker task.

  Request Parameters:
    mapreduce_spec: MapreduceSpec of the mapreduce serialized to json.
    shard_id: id of the shard.
    slice_id: id of the slice.
  """

  def __init__(self, time_function=time.time):
    """Constructor.

    Args:
      time_function: time function to use to obtain current time.
    """
    base_handler.BaseHandler.__init__(self)
    self._time = time_function

  def post(self):
    """Handle post request."""
    spec = model.MapreduceSpec.from_json_str(
        self.request.get("mapreduce_spec"))
    self._start_time = self._time()
    shard_id = self.shard_id()

    logging.debug("post: shard=%s slice=%s headers=%s",
                  shard_id, self.slice_id(), self.request.headers)

    shard_state, control = db.get([
        model.ShardState.get_key_by_shard_id(shard_id),
        model.MapreduceControl.get_key_by_job_id(spec.mapreduce_id),
    ])
    if not shard_state:
      logging.error("State not found for shard ID %r; shutting down",
                    shard_id)
      return

    if control and control.command == model.MapreduceControl.ABORT:
      logging.info("Abort command received by shard %d of job '%s'",
                   shard_state.shard_number, shard_state.mapreduce_id)
      shard_state.active = False
      shard_state.result_status = model.ShardState.RESULT_ABORTED
      shard_state.put()
      model.MapreduceControl.abort(spec.mapreduce_id)
      return

    input_reader = self.input_reader(spec.mapper)

    quota_consumer = quota.QuotaConsumer(
        quota.QuotaManager(memcache.Client()),
        shard_id,
        _QUOTA_BATCH_SIZE)

    ctx = context.Context(spec, shard_state)
    context.Context._set(ctx)

    try:
      if quota_consumer.check():
        scan_aborted = False
        entity = None

        if quota_consumer.consume():
          for entity in input_reader:
            if isinstance(entity, db.Model):
              shard_state.last_work_item = repr(entity.key())
            else:
              shard_state.last_work_item = repr(entity)[:100]

            scan_aborted = not self.process_entity(entity, quota_consumer, ctx)

            if not scan_aborted and not quota_consumer.consume():
              scan_aborted = True
            if scan_aborted:
              break
        else:
          scan_aborted = True


        if not scan_aborted:
          logging.info("Processing done for shard %d of job '%s'",
                       shard_state.shard_number, shard_state.mapreduce_id)
          quota_consumer.put(1)
          shard_state.active = False
          shard_state.result_status = model.ShardState.RESULT_SUCCESS

      ctx.flush()
    finally:
      context.Context._set(None)
      quota_consumer.dispose()

    if shard_state.active:
      self.reschedule(spec, input_reader)

  def process_entity(self, entity, quota_consumer, ctx):
    """Process a single entity.

    Call mapper handler on the entity.

    Args:
      entity: an entity to process.
      quota_consumer: an instance of quota.QuotaConsumer for current run.
      ctx: current execution context.

    Returns:
      True if scan should be continued, False if scan should be aborted.
    """
    ctx.counters.increment(context.COUNTER_MAPPER_CALLS)

    handler = ctx.mapreduce_spec.mapper.handler
    if util.is_generator_function(handler):
      for result in handler(entity):
        if callable(result):
          result(ctx)
        else:
          try:
            if len(result) == 2:
              logging.error("Collectors not implemented yet")
            else:
              logging.error("Got bad output tuple of length %d", len(result))
          except TypeError:
            logging.error(
                "Handler yielded type %s, expected a callable or a tuple",
                result.__class__.__name__)
    else:
      handler(entity)

    if self._time() - self._start_time > _SLICE_DURATION_SEC:
      logging.debug("Spent %s seconds. Rescheduling",
                    self._time() - self._start_time)
      return False
    return True

  def shard_id(self):
    """Get shard unique identifier of this task from request.

    Returns:
      shard identifier as string.
    """
    return str(self.request.get("shard_id"))

  def slice_id(self):
    """Get slice unique identifier of this task from request.

    Returns:
      slice identifier as int.
    """
    return int(self.request.get("slice_id"))

  def input_reader(self, mapper_spec):
    """Get the reader from mapper_spec initialized with the request's state.

    Args:
      mapper_spec: a mapper spec containing the immutable mapper state.

    Returns:
      An initialized InputReader.
    """
    input_reader_spec_dict = simplejson.loads(
        self.request.get("input_reader_state"))
    return mapper_spec.input_reader_class().from_json(
        input_reader_spec_dict)

  @staticmethod
  def worker_parameters(mapreduce_spec,
                        shard_id,
                        slice_id,
                        input_reader):
    """Fill in mapper worker task parameters.

    Returned parameters map is to be used as task payload, and it contains
    all the data, required by mapper worker to perform its function.

    Args:
      mapreduce_spec: specification of the mapreduce.
      shard_id: id of the shard (part of the whole dataset).
      slice_id: id of the slice (part of the shard).
      input_reader: InputReader containing the remaining inputs for this
        shard.

    Returns:
      string->string map of parameters to be used as task payload.
    """
    return {"mapreduce_spec": mapreduce_spec.to_json_str(),
            "shard_id": shard_id,
            "slice_id": str(slice_id),
            "input_reader_state": input_reader.to_json_str()}

  @staticmethod
  def get_task_name(shard_id, slice_id):
    """Compute single worker task name.

    Args:
      shard_id: id of the shard (part of the whole dataset) as string.
      slice_id: id of the slice (part of the shard) as int.

    Returns:
      task name which should be used to process specified shard/slice.
    """
    return "appengine-mrshard-%s-%s" % (shard_id, slice_id)

  def reschedule(self, mapreduce_spec, input_reader):
    """Reschedule worker task to continue scanning work.

    Args:
      mapreduce_spec: mapreduce specification.
      input_reader: remaining input reader to process.
    """
    MapperWorkerCallbackHandler.schedule_slice(
        self.base_path(), mapreduce_spec, self.shard_id(),
        self.slice_id() + 1, input_reader)

  @classmethod
  def schedule_slice(cls,
                     base_path,
                     mapreduce_spec,
                     shard_id,
                     slice_id,
                     input_reader,
                     queue_name=None):
    """Schedule slice scanning by adding it to the task queue.

    Args:
      base_path: base_path of mapreduce request handlers as string.
      mapreduce_spec: mapreduce specification as MapreduceSpec.
      shard_id: current shard id as string.
      slice_id: slice id as int.
      input_reader: remaining InputReader for given shard.
      queue_name: Optional queue to run on; uses the current queue of
        execution or the default queue if unspecified.
    """
    task_params = MapperWorkerCallbackHandler.worker_parameters(
        mapreduce_spec, shard_id, slice_id, input_reader)
    task_name = MapperWorkerCallbackHandler.get_task_name(shard_id, slice_id)
    queue_name = os.environ.get("HTTP_X_APPENGINE_QUEUENAME",
                                queue_name or "default")
    try:
      taskqueue.Task(url=base_path + "/worker_callback",
                     params=task_params,
                     name=task_name).add(queue_name)
    except (taskqueue.TombstonedTaskError, taskqueue.TaskAlreadyExistsError), e:
      logging.warning("Task %r with params %r already exists. %s: %s",
                      task_name, task_params, e.__class__, e)


class ControllerCallbackHandler(base_handler.BaseHandler):
  """Supervises mapreduce execution.

  Is also responsible for gathering execution status from shards together.

  This task is "continuously" running by adding itself again to taskqueue if
  mapreduce is still active.
  """

  def __init__(self, time_function=time.time):
    """Constructor.

    Args:
      time_function: time function to use to obtain current time.
    """
    base_handler.BaseHandler.__init__(self)
    self._time = time_function

  def post(self):
    """Handle post request."""
    spec = model.MapreduceSpec.from_json_str(
        self.request.get("mapreduce_spec"))

    logging.debug("post: id=%s headers=%s",
                  spec.mapreduce_id, self.request.headers)

    state, control = db.get([
        model.MapreduceState.get_key_by_job_id(spec.mapreduce_id),
        model.MapreduceControl.get_key_by_job_id(spec.mapreduce_id),
    ])
    if not state:
      logging.error("State not found for mapreduce_id '%s'; skipping",
                    spec.mapreduce_id)
      return

    shard_states = model.ShardState.find_by_mapreduce_id(spec.mapreduce_id)
    if state.active and len(shard_states) != spec.mapper.shard_count:
      logging.error("Incorrect number of shard states: %d vs %d; "
                    "aborting job '%s'",
                    len(shard_states), spec.mapper.shard_count,
                    spec.mapreduce_id)
      state.active = False
      state.result_status = model.MapreduceState.RESULT_FAILED
      model.MapreduceControl.abort(spec.mapreduce_id)

    active_shards = [s for s in shard_states if s.active]
    if state.active:
      state.active = bool(active_shards)
      state.active_shards = len(active_shards)

    if (not state.active and control and
        control.command == model.MapreduceControl.ABORT):
      logging.info("Abort signal received for job '%s'", spec.mapreduce_id)
      state.result_status = model.MapreduceState.RESULT_ABORTED

    if not state.active:
      state.active_shards = 0
      if not state.result_status:
        if [s for s in shard_states
            if s.result_status != model.ShardState.RESULT_SUCCESS]:
          state.result_status = model.MapreduceState.RESULT_FAILED
        else:
          state.result_status = model.MapreduceState.RESULT_SUCCESS
        logging.info("Final result for job '%s' is '%s'",
                     spec.mapreduce_id, state.result_status)

    self.aggregate_state(state, shard_states)
    poll_time = state.last_poll_time
    state.last_poll_time = datetime.datetime.utcfromtimestamp(self._time())
    state.put()
    if not state.active:
      return

    processing_rate = int(spec.mapper.params.get(
        "processing_rate") or model._DEFAULT_PROCESSING_RATE_PER_SEC)
    self.refill_quotas(poll_time, processing_rate, active_shards)
    ControllerCallbackHandler.schedule(self.base_path(), spec)

  def aggregate_state(self, mapreduce_state, shard_states):
    """Update current mapreduce state by aggregating shard states.

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

  def refill_quotas(self,
                    last_poll_time,
                    processing_rate,
                    active_shard_states):
    """Refill quotas for all active shards.

    Args:
      last_poll_time: Datetime with the last time the job state was updated.
      processing_rate: How many items to process per second overall.
      active_shard_states: All active shard states, list of ShardState.
    """
    if not active_shard_states:
      return
    quota_manager = quota.QuotaManager(memcache.Client())

    current_time = int(self._time())
    last_poll_time = time.mktime(last_poll_time.timetuple())
    total_quota_refill = processing_rate * max(0, current_time - last_poll_time)
    quota_refill = int(math.ceil(
        1.0 * total_quota_refill / len(active_shard_states)))

    if not quota_refill:
      return

    for shard_state in active_shard_states:
      quota_manager.put(shard_state.shard_id, quota_refill)

  @classmethod
  def schedule(cls, base_path, mapreduce_spec, queue_name=None):
    """Schedule new update status callback task.

    Args:
      base_path: mapreduce handlers url base path as string.
      mapreduce_spec: mapreduce specification as MapreduceSpec.
      queue_name: The queue to schedule this task on. Will use the current
        queue of execution if not supplied.
    """
    queue_name = (
        os.environ.get("HTTP_X_APPENGINE_QUEUENAME", queue_name) or "default")
    taskqueue.Task(url=base_path + "/controller_callback",
                   params={"mapreduce_spec": mapreduce_spec.to_json_str()},
                   countdown=2).add(queue_name)


class StartJobHandler(base_handler.JsonHandler):
  """Command handler starts a mapreduce job."""

  def handle(self):
    mapreduce_name = self._get_required_param("name")
    mapper_input_reader_spec = self._get_required_param("mapper_input_reader")
    mapper_handler_spec = self._get_required_param("mapper_handler")
    mapper_params = self._get_mapper_params()

    mapper_params["processing_rate"] = int(mapper_params.get(
          "processing_rate") or model._DEFAULT_PROCESSING_RATE_PER_SEC)
    queue_name = mapper_params["queue_name"] = mapper_params.get(
        "queue_name", "default")

    mapper_spec = model.MapperSpec(
        mapper_handler_spec,
        mapper_input_reader_spec,
        mapper_params,
        int(mapper_params.get("shard_count", model._DEFAULT_SHARD_COUNT)))
    mapper_spec.get_handler()
    mapper_input_reader_class = mapper_spec.input_reader_class()
    mapper_input_readers = mapper_input_reader_class.split_input(mapper_spec)
    if not mapper_input_readers:
      raise NoDataError("Found no mapper input readers to process.")
    mapper_spec.shard_count = len(mapper_input_readers)

    state = model.MapreduceState.create_new()
    mapreduce_spec = model.MapreduceSpec(
        mapreduce_name,
        state.key().id_or_name(),
        mapper_spec.to_json())
    state.mapreduce_spec = mapreduce_spec
    state.active = True

    state.char_url = ""
    state.sparkline_url = ""

    state.put()
    self._schedule_shards(mapreduce_spec, mapper_input_readers, queue_name)
    ControllerCallbackHandler.schedule(self.base_path(), mapreduce_spec,
                                       queue_name=queue_name)
    self.json_response["mapreduce_id"] = state.key().id_or_name()

  def _get_mapper_params(self):
    """Retrieves additional user-supplied params for the job and validates them.

    Raises:
      Any exception raised by the 'params_validator' request parameter if
      the params fail to validate.
    """
    params_validator = self.request.get("mapper_params_validator")

    user_params = {}
    for key in self.request.arguments():
      if key.startswith("mapper_params."):
        values = self.request.get_all(key)
        adjusted_key = key[len("mapper_params."):]
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
      NotEnoughArgumentsError: if parameter is not specified.
    """
    value = self.request.get(param_name)
    if not value:
      raise NotEnoughArgumentsError(param_name + " not specified")
    return value

  def _schedule_shards(self, spec, input_readers, queue_name):
    """Prepares shard states and schedules their execution.

    Args:
      spec: mapreduce specification as MapreduceSpec.
      input_readers: list of InputReaders describing shard splits.
      queue_name: The queue to run this job on.
    """
    shard_states = []
    for shard_number, input_reader in enumerate(input_readers):
      shard = model.ShardState.create_new(spec.mapreduce_id, shard_number)
      shard.shard_description = str(input_reader)
      shard_states.append(shard)
    db.put(shard_states)

    for shard_number, input_reader in enumerate(input_readers):
      shard_id = model.ShardState.shard_id_from_number(
          spec.mapreduce_id, shard_number)
      MapperWorkerCallbackHandler.schedule_slice(
          self.base_path(), spec, shard_id, 0, input_reader,
          queue_name=queue_name)


class CleanUpJobHandler(base_handler.JsonHandler):
  """Command to kick off tasks to clean up a job's data."""

  def handle(self):
    self.json_response["status"] = "This does nothing yet."


class AbortJobHandler(base_handler.JsonHandler):
  """Command to abort a running job."""

  def handle(self):
    model.MapreduceControl.abort(self.request.get("mapreduce_id"))
    self.json_response["status"] = "Abort signal sent."
