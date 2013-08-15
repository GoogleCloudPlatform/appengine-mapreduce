#!/usr/bin/env python
"""Parameters to control Mapreduce."""

__all__ = ["CONFIG_NAMESPACE",
           "config"]

from google.appengine.api import lib_config

CONFIG_NAMESPACE = "mapreduce"


class _ConfigDefaults(object):
  """Default configs.

  Do not change parameters starts with _.

  SHARD_RETRY_LIMIT: How many times a shard can retry.

  QUEUE_NAME: Default queue for MR.

  SHARD_COUNT: Default shard count.

  PROCESSING_RATE_PER_SEC: Default rate of processed entities per second.

  BASE_PATH : Base path of mapreduce and pipeline handlers.

  RETRY_SLICE_ERROR_MAX_RETRIES:
    How many times to cope with a RetrySliceError before totally
  giving up and aborting the whole job. RetrySliceError is raised only
  during processing user data. Errors from MR framework are not counted.

  MAX_TASK_RETRIES: How many times to retry a task before dropping it.
  """

  SHARD_RETRY_LIMIT = 3

  QUEUE_NAME = "default"

  SHARD_COUNT = 8

  # Make this high because too many people are confused when mapper is too slow.
  PROCESSING_RATE_PER_SEC = 1000000

  # This path will be changed by build process when this is a part of SDK.
  BASE_PATH = "/mapreduce"

  RETRY_SLICE_ERROR_MAX_RETRIES = 10

  # Arbitrary big number.
  MAX_TASK_RETRIES = 30

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


# pylint: disable=protected-access
# Constants for backward compatibility.
DEFAULT_SHARD_RETRY_LIMIT = config.SHARD_RETRY_LIMIT
DEFAULT_QUEUE_NAME = config.QUEUE_NAME
DEFAULT_SHARD_COUNT = config.SHARD_COUNT
_DEFAULT_PROCESSING_RATE_PER_SEC = config.PROCESSING_RATE_PER_SEC
_DEFAULT_BASE_PATH = config.BASE_PATH
_RETRY_SLICE_ERROR_MAX_RETRIES = config.RETRY_SLICE_ERROR_MAX_RETRIES
_MAX_TASK_RETRIES = config.MAX_TASK_RETRIES
_SLICE_DURATION_SEC = config._SLICE_DURATION_SEC
_LEASE_GRACE_PERIOD = config._LEASE_GRACE_PERIOD
_REQUEST_EVENTUAL_TIMEOUT = config._REQUEST_EVENTUAL_TIMEOUT
_CONTROLLER_PERIOD_SEC = config._CONTROLLER_PERIOD_SEC
