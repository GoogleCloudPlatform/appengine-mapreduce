#!/usr/bin/env python
"""Parameters to control Mapreduce."""



__all__ = []

DEFAULT_SHARD_RETRY_LIMIT = 3
DEFAULT_QUEUE_NAME = "default"
DEFAULT_SHARD_COUNT = 8

# How many times to cope with a RetrySliceError before totally
# giving up and aborting the whole job.
# RetrySliceError is raised only during processing user data. Errors from MR
# framework are not counted.
_RETRY_SLICE_ERROR_MAX_RETRIES = 10

# How many times to retry a task before dropping it. Arbitrary big number.
_MAX_TASK_RETRIES = 30

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

# Default rate of processing entities per second.
# Make this high, because too many people are confused when mapper is too slow.
_DEFAULT_PROCESSING_RATE_PER_SEC = 1000000

# This path will be changed by build process when this is a part of SDK.
_DEFAULT_BASE_PATH = "/mapreduce"
_DEFAULT_PIPELINE_BASE_PATH = _DEFAULT_BASE_PATH + "/pipeline"
