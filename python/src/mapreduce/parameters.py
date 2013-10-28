#!/usr/bin/env python
"""Parameters to control Mapreduce."""

__all__ = ["CONFIG_NAMESPACE",
           "config"]

from google.appengine.api import lib_config

CONFIG_NAMESPACE = "mapreduce"


class _ConfigDefaults(object):
  """Default configs.

  Do not change parameters whose names begin with _.

  SHARD_MAX_ATTEMPTS: Max attempts to execute a shard before giving up.

  TASK_MAX_ATTEMPTS: Max attempts to execute a task before dropping it. Task
    is any taskqueue task created by MR framework. A task is dropped
    when its X-AppEngine-TaskExecutionCount is bigger than this number.
    Dropping a task will cause abort on the entire MR job.

  TASK_MAX_DATA_PROCESSING_ATTEMPTS:
    Max times to execute a task when previous task attempts failed during
    data processing stage. An MR work task has three major stages:
    initial setup, data processing, and final checkpoint.
    Setup stage should be allowed to be retried more times than data processing
    stage: setup failures are caused by unavailable GAE services while
    data processing failures are mostly due to user function error out on
    certain input data. Thus, set TASK_MAX_ATTEMPTS higher than this parameter.

  QUEUE_NAME: Default queue for MR.

  SHARD_COUNT: Default shard count.

  PROCESSING_RATE_PER_SEC: Default rate of processed entities per second.

  BASE_PATH : Base path of mapreduce and pipeline handlers.
  """

  SHARD_MAX_ATTEMPTS = 4

  # Arbitrary big number.
  TASK_MAX_ATTEMPTS = 31

  TASK_MAX_DATA_PROCESSING_ATTEMPTS = 11

  QUEUE_NAME = "default"

  SHARD_COUNT = 8

  # Make this high because too many people are confused when mapper is too slow.
  PROCESSING_RATE_PER_SEC = 1000000

  # This path will be changed by build process when this is a part of SDK.
  BASE_PATH = "/mapreduce"

  # TODO(user): find a proper value for this.
  # The amount of time to perform scanning in one slice. New slice will be
  # scheduled as soon as current one takes this long.
  _SLICE_DURATION_SEC = 15

  # See model.ShardState doc on slice_start_time. In second.
  _LEASE_GRACE_PERIOD = 1

  # See model.ShardState doc on slice_start_time. In second.
  _REQUEST_EVENTUAL_TIMEOUT = 10 * 60 + 30

  # Delay between consecutive controller callback invocations.
  _CONTROLLER_PERIOD_SEC = 2


config = lib_config.register(CONFIG_NAMESPACE, _ConfigDefaults.__dict__)


# The following are constants that depends on the value of _config.
# They are constants because _config is completely initialized on the first
# request of an instance and will never change until user deploy a new version.
_DEFAULT_PIPELINE_BASE_PATH = config.BASE_PATH + "/pipeline"
# See b/11341023 for context.
_GCS_URLFETCH_TIMEOUT_SEC = 30
