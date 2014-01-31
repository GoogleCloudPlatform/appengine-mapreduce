#!/usr/bin/env python
"""Per job config for map jobs."""
from mapreduce import hooks
from mapreduce import input_readers
from mapreduce import output_writers
from mapreduce import parameters

# pylint: disable=protected-access
# pylint: disable=invalid-name

_Option = parameters._Option


class MapJobConfig(parameters._Config):
  """Configurations for a map job.

  Names started with _ are reserved for internal use.

  To create an instance:
  all option names can be used as keys to __init__.
  If an option is required, the key must be provided.
  If an option isn't required and no value is given, the default value
  will be used.
  """
  # Job name in str. UI purpose only.
  job_name = _Option(str, required=True)

  # Reference to your mapper.
  # TODO(user): Create a super class for mapper.
  mapper = _Option(object, required=True)

  # The class of input reader to use.
  input_reader_cls = _Option(input_readers.InputReader, required=True)

  # Parameters for input reader. Varies by input reader class.
  input_reader_params = _Option(dict, default={})

  # The class of output writer to use.
  output_writer_cls = _Option(output_writers.OutputWriter,
                              can_be_none=True)

  # Parameters for output writers. Varies by input reader class.
  output_writer_params = _Option(dict, default={})

  # Number of map shards.
  shard_count = _Option(int, default=parameters.config.SHARD_COUNT)

  # Additional parameters that can be access via context.Context.
  context = _Option(dict, default={})

  # The queue where all map tasks should run on.
  queue_name = _Option(str, default=parameters.config.QUEUE_NAME)

  # max attempts to run and retry a shard.
  shard_max_attempts = _Option(int,
                               default=parameters.config.SHARD_MAX_ATTEMPTS)

  # Force datastore writes.
  _force_write = _Option(bool, default=False)

  _base_path = _Option(str, default=parameters.config.BASE_PATH)

  _task_max_attempts = _Option(int, default=parameters.config.TASK_MAX_ATTEMPTS)

  _task_max_data_processing_attempts = _Option(
      int, default=parameters.config.TASK_MAX_DATA_PROCESSING_ATTEMPTS)

  _hooks_cls = _Option(hooks.Hooks, can_be_none=True)

  _app = _Option(str, can_be_none=True)

