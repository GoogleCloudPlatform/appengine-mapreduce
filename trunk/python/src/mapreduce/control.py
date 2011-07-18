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

"""API for controlling MapReduce execution outside of MapReduce framework."""



__all__ = ["start_map"]

# pylint: disable-msg=C6409


from mapreduce import base_handler
from mapreduce import handlers
from mapreduce import model


_DEFAULT_SHARD_COUNT = 8


def start_map(name,
              handler_spec,
              reader_spec,
              mapper_parameters,
              shard_count=_DEFAULT_SHARD_COUNT,
              output_writer_spec=None,
              mapreduce_parameters=None,
              base_path=None,
              queue_name="default",
              eta=None,
              countdown=None,
              hooks_class_name=None,
              _app=None,
              transactional=False):
  """Start a new, mapper-only mapreduce.

  Args:
    name: mapreduce name. Used only for display purposes.
    handler_spec: fully qualified name of mapper handler function/class to call.
    reader_spec: fully qualified name of mapper reader to use
    mapper_parameters: dictionary of parameters to pass to mapper. These are
      mapper-specific and also used for reader initialization.
    shard_count: number of shards to create.
    mapreduce_parameters: dictionary of mapreduce parameters relevant to the
      whole job.
    base_path: base path of mapreduce library handler specified in app.yaml.
      "/mapreduce" by default.
    queue_name: executor queue name to be used for mapreduce tasks.
    eta: Absolute time when the MR should execute. May not be specified
        if 'countdown' is also supplied. This may be timezone-aware or
        timezone-naive.
    countdown: Time in seconds into the future that this MR should execute.
        Defaults to zero.
    hooks_class_name: fully qualified name of a hooks.Hooks subclass.
    transactional: Specifies if job should be started as a part of already
      opened transaction.

  Returns:
    mapreduce id as string.
  """
  if not shard_count:
    shard_count = _DEFAULT_SHARD_COUNT
  if base_path is None:
    base_path = base_handler._DEFAULT_BASE_PATH
  mapper_spec = model.MapperSpec(handler_spec,
                                 reader_spec,
                                 mapper_parameters,
                                 shard_count,
                                 output_writer_spec=output_writer_spec)

  return handlers.StartJobHandler._start_map(
      name,
      mapper_spec,
      mapreduce_parameters or {},
      base_path=base_path,
      queue_name=queue_name,
      eta=eta,
      countdown=countdown,
      hooks_class_name=hooks_class_name,
      _app=_app,
      transactional=transactional)

