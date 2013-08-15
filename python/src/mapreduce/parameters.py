#!/usr/bin/env python
"""Parameters to control Mapreduce."""



__all__ = []

DEFAULT_SHARD_RETRY_LIMIT = 3
DEFAULT_QUEUE_NAME = "default"
DEFAULT_SHARD_COUNT = 8

# Default rate of processed entities per second.
# Make this high, because too many people are confused when mapper is too slow.
_DEFAULT_PROCESSING_RATE_PER_SEC = 1000000

# This path will be changed by build process when this is a part of SDK.
_DEFAULT_BASE_PATH = "/mapreduce"
_DEFAULT_PIPELINE_BASE_PATH = _DEFAULT_BASE_PATH + "/pipeline"
