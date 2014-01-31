#!/usr/bin/env python
"""Per job config for map jobs."""
from mapreduce import hooks
from mapreduce import input_readers
from mapreduce import model
from mapreduce import output_writers
from mapreduce import parameters
from mapreduce import util
from mapreduce.map_job import mapper as mapper_module


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
  mapper = _Option(mapper_module.Mapper, required=True)

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

  # TODO(user): Implement context to allow user to supply arbitrary k,v.

  # The queue where all map tasks should run on.
  queue_name = _Option(str, default=parameters.config.QUEUE_NAME)

  # max attempts to run and retry a shard.
  shard_max_attempts = _Option(int,
                               default=parameters.config.SHARD_MAX_ATTEMPTS)

  # The URL to GET after the job finish, regardless of success.
  # The map_job_id will be provided as a query string key.
  done_callback_url = _Option(str, can_be_none=True)

  # Force datastore writes.
  _force_writes = _Option(bool, default=False)

  _base_path = _Option(str, default=parameters.config.BASE_PATH)

  _task_max_attempts = _Option(int, default=parameters.config.TASK_MAX_ATTEMPTS)

  _task_max_data_processing_attempts = _Option(
      int, default=parameters.config.TASK_MAX_DATA_PROCESSING_ATTEMPTS)

  _hooks_cls = _Option(hooks.Hooks, can_be_none=True)

  _app = _Option(str, can_be_none=True)

  # The following methods are to convert Config to supply for older APIs.

  def _get_mapper_params(self):
    """Converts self to model.MapperSpec.params."""
    return {"input_reader": self.input_reader_params,
            "output_writer": self.output_writer_params}

  def _get_mapper_spec(self):
    """Converts self to model.MapperSpec."""
    return model.MapperSpec(
        handler_spec=util._obj_to_path(self.mapper),
        input_reader_spec=util._obj_to_path(self.input_reader_cls),
        params=self._get_mapper_params(),
        shard_count=self.shard_count,
        output_writer_spec=util._obj_to_path(self.output_writer_cls))

  def _get_mr_params(self):
    """Converts self to model.MapreduceSpec.params."""
    return {"force_writes": self._force_writes,
            "done_callback": self.done_callback_url,
            "shard_max_attempts": self.shard_max_attempts,
            "task_max_attempts": self._task_max_attempts,
            "task_max_data_processing_attempts":
                self._task_max_data_processing_attempts,
            "queue_name": self.queue_name,
            "base_path": self._base_path}

  # TODO(user): Ideally we should replace all the *_spec and *_params
  # in model.py with MapJobConfig. This not only cleans up codebase, but may
  # also be necessary for launching input_reader/output_writer API. We don't
  # want to surface the numerous *_spec and *_params objects in our public API.
  # The cleanup has to be done over several releases to not to break runtime.
  @classmethod
  def _get_default_mr_params(cls):
    """Gets default values for old API."""
    cfg = cls(_lenient=True)
    return cfg._get_mr_params()
