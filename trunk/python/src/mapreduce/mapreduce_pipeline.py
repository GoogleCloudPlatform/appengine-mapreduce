#!/usr/bin/env python
#
# Copyright 2011 Google Inc.
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

"""Pipelines for mapreduce library."""

from __future__ import with_statement



__all__ = [
    "MapPipeline",
    "MapperPipeline",
    "MapreducePipeline",
    "ReducePipeline",
    "ShufflePipeline",
    ]


from mapreduce.lib import pipeline
from mapreduce.lib.pipeline import common as pipeline_common
from mapreduce.lib import files
from mapreduce.lib.files import file_service_pb
from mapreduce import base_handler
from mapreduce import context
from mapreduce import errors
from mapreduce import input_readers
from mapreduce import mapper_pipeline
from mapreduce import operation
from mapreduce import output_writers
from mapreduce import shuffler
from mapreduce import util


# Mapper pipeline is extracted only to remove dependency cycle with shuffler.py
# Reimport it back.
MapperPipeline = mapper_pipeline.MapperPipeline

ShufflePipeline = shuffler.ShufflePipeline


class MapPipeline(base_handler.PipelineBase):
  """Runs the map stage of MapReduce.

  Iterates over input reader and outputs data into key/value format
  for shuffler consumption.

  Args:
    job_name: mapreduce job name as string.
    mapper_spec: specification of map handler function as string.
    input_reader_spec: input reader specification as string.
    params: mapper and input reader parameters as dict.
    shards: number of shards to start as int.

  Returns:
    list of filenames written to by this mapper, one for each shard.
  """

  def run(self,
          job_name,
          mapper_spec,
          input_reader_spec,
          params,
          shards=None):
    yield MapperPipeline(
        job_name + "-map",
        mapper_spec,
        input_reader_spec,
        output_writer_spec=
            output_writers.__name__ + ".KeyValueBlobstoreOutputWriter",
        params=params,
        shards=shards)


class _ReducerReader(input_readers.RecordsReader):
  """Reader to read KeyValues records files from Files API."""

  expand_parameters = True

  def __init__(self, filenames, position):
    super(_ReducerReader, self).__init__(filenames, position)
    self.current_key = None
    self.current_values = None

  def __iter__(self):
    ctx = context.get()
    combiner = None

    if ctx:
      combiner_spec = ctx.mapreduce_spec.mapper.params.get("combiner_spec")
      if combiner_spec:
        combiner = util.handler_for_name(combiner_spec)

    self.current_key = None
    self.current_values = None

    for binary_record in super(_ReducerReader, self).__iter__():
      proto = file_service_pb.KeyValues()
      proto.ParseFromString(binary_record)

      if self.current_key is None:
        self.current_key = proto.key()
        self.current_values = []
      else:
        assert proto.key() == self.current_key, (
            "inconsistent key sequence. Expected %s but got %s" %
            (self.current_key, proto.key()))

      if combiner:
        combiner_result = combiner(
            self.current_key, proto.value_list(), self.current_values)

        if not util.is_generator(combiner_result):
          raise errors.BadCombinerOutputError(
              "Combiner %s should yield values instead of returning them (%s)" %
              (combiner, combiner_result))

        self.current_values = []
        for value in combiner_result:
          if isinstance(value, operation.Operation):
            value(ctx)
          else:
            # with combiner current values always come from combiner
            self.current_values.append(value)
      else:
        # without combiner we just accumulate values.
        self.current_values.extend(proto.value_list())

      if not proto.partial():
        key = self.current_key
        values = self.current_values
        # This is final value, don't try to serialize it.
        self.current_key = None
        self.current_values = None
        yield (key, values)
      else:
        yield input_readers.ALLOW_CHECKPOINT

  def to_json(self):
    """Returns an input shard state for the remaining inputs.

    Returns:
      A json-izable version of the remaining InputReader.
    """
    result = super(_ReducerReader, self).to_json()
    result["current_key"] = self.current_key
    result["current_values"] = self.current_values
    return result

  @classmethod
  def from_json(cls, json):
    """Creates an instance of the InputReader for the given input shard state.

    Args:
      json: The InputReader state as a dict-like object.

    Returns:
      An instance of the InputReader configured using the values of json.
    """
    result = super(_ReducerReader, cls).from_json(json)
    result.current_key = json["current_key"]
    result.current_values = json["current_values"]
    return result


class ReducePipeline(base_handler.PipelineBase):
  """Runs the reduce stage of MapReduce.

  Merge-reads input files and runs reducer function on them.

  Args:
    job_name: mapreduce job name as string.
    reader_spec: specification of reduce function.
    output_writer_spec: specification of output write to use with reduce
      function.
    params: mapper parameters to use as dict.
    filenames: list of filenames to reduce.
    combiner_spec: Optional. Specification of a combine function. If not
      supplied, no combine step will take place. The combine function takes a
      key, list of values and list of previously combined results. It yields
      combined values that might be processed by another combiner call, but will
      eventually end up in reducer. The combiner output key is assumed to be the
      same as the input key.

  Returns:
    filenames from output writer.
  """

  def run(self,
          job_name,
          reducer_spec,
          output_writer_spec,
          params,
          filenames,
          combiner_spec=None):
    new_params = dict(params or {})
    new_params.update({
        "files": filenames
        })
    if combiner_spec:
      new_params.update({
          "combiner_spec": combiner_spec,
          })

    yield mapper_pipeline.MapperPipeline(
        job_name + "-reduce",
        reducer_spec,
        __name__ + "._ReducerReader",
        output_writer_spec,
        new_params)


class MapreducePipeline(base_handler.PipelineBase):
  """Pipeline to execute MapReduce jobs.

  Args:
    job_name: job name as string.
    mapper_spec: specification of mapper to use.
    reducer_spec: specification of reducer to use.
    input_reader_spec: specification of input reader to read data from.
    output_writer_spec: specification of output writer to save reduce output to.
    mapper_params: parameters to use for mapper phase.
    reducer_params: parameters to use for reduce phase.
    shards: number of shards to use as int.
    combiner_spec: Optional. Specification of a combine function. If not
      supplied, no combine step will take place. The combine function takes a
      key, list of values and list of previously combined results. It yields
      combined values that might be processed by another combiner call, but will
      eventually end up in reducer. The combiner output key is assumed to be the
      same as the input key.

  Returns:
    filenames from output writer.
  """

  def run(self,
          job_name,
          mapper_spec,
          reducer_spec,
          input_reader_spec,
          output_writer_spec=None,
          mapper_params=None,
          reducer_params=None,
          shards=None,
          combiner_spec=None):
    map_pipeline = yield MapPipeline(job_name,
                                     mapper_spec,
                                     input_reader_spec,
                                     params=mapper_params,
                                     shards=shards)
    shuffler_pipeline = yield ShufflePipeline(
        job_name, map_pipeline)
    reducer_pipeline = yield ReducePipeline(
        job_name,
        reducer_spec,
        output_writer_spec,
        reducer_params,
        shuffler_pipeline,
        combiner_spec=combiner_spec)
    with pipeline.After(reducer_pipeline):
      all_temp_files = yield pipeline_common.Extend(
          map_pipeline, shuffler_pipeline)
      yield mapper_pipeline._CleanupPipeline(all_temp_files)
    yield pipeline_common.Return(reducer_pipeline)
