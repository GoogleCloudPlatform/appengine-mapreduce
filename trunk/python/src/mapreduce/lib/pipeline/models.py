#!/usr/bin/env python
#
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

"""Datastore models used by the Google App Engine Pipeline API."""

# Relative imports
from mapreduce.lib import simplejson

from google.appengine.ext import db


class JsonProperty(db.UnindexedProperty):
  """Property type for storing JSON-serialized data."""

  data_type = unicode

  def __init__(self, **kwargs):
    """Constructor.

    Args:
      default: default value for the property. The value is deep copied
        for each model instance.
      kwargs: remaining arguments.
    """
    super(JsonProperty, self).__init__(**kwargs)

  def get_value_for_datastore(self, model_instance):
    """Gets a value to store in the Datastore.

    Args:
      model_instance: instance of the model class.

    Returns:
      datastore-compatible value.
    """
    value = super(JsonProperty, self).get_value_for_datastore(model_instance)
    if value is None:
      return None
    return db.Text(simplejson.dumps(value, sort_keys=True))

  def make_value_from_datastore(self, value):
    """Convert a value from its Datastore representation.

    Args:
      value: Datastore value.

    Returns:
      value to return to user code.
    """
    if value is None:
      return None
    return simplejson.loads(value)

  def validate(self, value):
    """Pass-through since only JSON serialization will validate."""
    return value


class ParamsProperty(JsonProperty):
  """Property for storing Pipeline parameters."""

  def make_value_from_datastore(self, value):
    """Sanitizes and normalizes certain parameter keys."""
    value = super(ParamsProperty, self).make_value_from_datastore(value)
    if isinstance(value, dict):
      kwargs = value.get('kwargs')
      if kwargs:
        adjusted_kwargs = {}
        for arg_key, arg_value in kwargs.iteritems():
          # Python only allows non-unicode strings as keyword arguments.
          adjusted_kwargs[str(arg_key)] = arg_value
        value['kwargs'] = adjusted_kwargs
    return value


class _PipelineRecord(db.Model):
  """Represents a Pipeline.

  Properties:
    class_path: Path of the Python class to use for this pipeline.
    root_pipeline: The root of the whole workflow; set to itself this pipeline
      is its own root.
    fanned_out: List of child _PipelineRecords that were started when this
      generator pipeline moved from WAITING to RUN.
    start_time: For pipelines with no start _BarrierRecord, when this pipeline
      was enqueued to run immediately.
    finalized_time: When this pipeline moved from WAITING or RUN to DONE.
    params: Serialized parameter dictionary.
    status: The current status of the pipeline.
    current_attempt: The current attempt (starting at 0) to run.
    max_attempts: Maximum number of attempts (starting at 0) to run.
    next_retry_time: ETA of the next retry attempt.
    retry_message: Why the last attempt failed; None or empty if no message.

  Root pipeline properties:
    is_root_pipeline: This is a root pipeline.
    abort_message: Why the whole pipeline was aborted; only saved on
      root pipelines.
    abort_requested: If an abort signal has been requested for this root
      pipeline; only saved on root pipelines
  """

  WAITING = 'waiting'
  RUN = 'run'
  DONE = 'done'
  ABORTED = 'aborted'

  class_path = db.StringProperty()
  root_pipeline = db.SelfReferenceProperty(
                      collection_name='child_pipelines_set')
  fanned_out = db.ListProperty(db.Key, indexed=False)
  start_time = db.DateTimeProperty(indexed=False)
  finalized_time = db.DateTimeProperty(indexed=False)
  params = ParamsProperty()
  status = db.StringProperty(choices=(WAITING, RUN, DONE, ABORTED),
                             default=WAITING)

  # Retry behavior
  current_attempt = db.IntegerProperty(default=0, indexed=False)
  max_attempts = db.IntegerProperty(default=1, indexed=False)
  next_retry_time = db.DateTimeProperty(indexed=False)
  retry_message = db.TextProperty()

  # Root pipeline properties
  is_root_pipeline = db.BooleanProperty()
  abort_message = db.TextProperty()
  abort_requested = db.BooleanProperty(indexed=False)

  @classmethod
  def kind(cls):
    return '_AE_Pipeline_Record'


class _SlotRecord(db.Model):
  """Represents an output slot.

  Properties:
    root_pipeline: The root of the workflow.
    filler: The pipeline that filled this slot.
    value: Serialized value for this slot.
    status: The current status of the slot.
    fill_time: When the slot was filled by the filler.
  """

  FILLED = 'filled'
  WAITING = 'waiting'

  root_pipeline = db.ReferenceProperty(_PipelineRecord)
  filler = db.ReferenceProperty(_PipelineRecord,
                                collection_name='filled_slots_set')
  value = JsonProperty()
  status = db.StringProperty(choices=(FILLED, WAITING), default=WAITING,
                             indexed=False)
  fill_time = db.DateTimeProperty(indexed=False)

  @classmethod
  def kind(cls):
    return '_AE_Pipeline_Slot'


class _BarrierRecord(db.Model):
  """Represents a barrier.

  Properties:
    root_pipeline: The root of the workflow.
    target: The pipeline to run when the barrier fires.
    blocking_slots: The slots that must be filled before this barrier fires.
    trigger_time: When this barrier fired.
    status: The current status of the barrier.
  """

  # Barrier statuses
  FIRED = 'fired'
  WAITING = 'waiting'

  # Barrier trigger reasons (used as key names)
  START = 'start'
  FINALIZE = 'finalize'
  ABORT = 'abort'

  root_pipeline = db.ReferenceProperty(_PipelineRecord)
  target = db.ReferenceProperty(_PipelineRecord,
                                collection_name='called_barrier_set')
  blocking_slots = db.ListProperty(db.Key)
  trigger_time = db.DateTimeProperty(indexed=False)
  status = db.StringProperty(choices=(FIRED, WAITING), default=WAITING,
                             indexed=False)

  @classmethod
  def kind(cls):
    return '_AE_Pipeline_Barrier'


class _StatusRecord(db.Model):
  """Represents the current status of a pipeline.

  Properties:
    message: The textual message to show.
    console_url: URL to iframe as the primary console for this pipeline.
    link_names: Human display names for status links.
    link_urls: URLs corresponding to human names for status links.
    status_time: When the status was written.
  """

  root_pipeline = db.ReferenceProperty(_PipelineRecord)
  message = db.TextProperty()
  console_url = db.TextProperty()
  link_names = db.ListProperty(db.Text, indexed=False)
  link_urls = db.ListProperty(db.Text, indexed=False)
  status_time = db.DateTimeProperty(indexed=False)

  @classmethod
  def kind(cls):
    return '_AE_Pipeline_Status'
