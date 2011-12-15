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





from mapreduce.lib import pipeline
from mapreduce.lib.pipeline import common as pipeline_common
from mapreduce.lib import files
from mapreduce.lib.files import file_service_pb
from mapreduce import output_writers
from mapreduce import base_handler
from mapreduce import input_readers
from mapreduce import mapper_pipeline
from mapreduce import output_writers
from mapreduce import shuffler


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


class KeyValuesReader(input_readers.RecordsReader):
  """Reader to read KeyValues records files from Files API."""

  expand_parameters = True

  def __iter__(self):
    current_key = None
    current_values = None

    for binary_record in input_readers.RecordsReader.__iter__(self):
      proto = file_service_pb.KeyValues()
      proto.ParseFromString(binary_record)
      if current_key is None:
        current_key = proto.key()
        current_values = proto.value_list()
      else:
        assert proto.key() == current_key
        current_values.extend(proto.value_list())

      if not proto.partial():
        # __iter__ can be interrupted only on yield, so we don't need to
        # persist current_key and current_value.
        yield (current_key, current_values)
        current_key = None
        current_values = None


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

  Returns:
    filenames from output writer.
  """

  def run(self,
          job_name,
          reducer_spec,
          output_writer_spec,
          params,
          filenames):
    new_params = dict(params or {})
    new_params.update({
        "files": filenames
        })
    yield mapper_pipeline.MapperPipeline(
        job_name + "-reduce",
        reducer_spec,
        __name__ + ".KeyValuesReader",
        output_writer_spec,
        new_params)


class MapreducePipeline(base_handler.PipelineBase):
  """Pipeline to execute MapReduce jobs.

  Args:
    job_name: job name as string.
    mapper_spec: specification of mapper to use.
    reader_spec: specification of reducer to use.
    input_reader_spec: specification of input reader to read data from.
    output_writer_spec: specification of output writer to save reduce output to.
    mapper_params: parameters to use for mapper phase.
    reducer_params: parameters to use for reduce phase.
    shards: number of shards to use as int.

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
          shards=None):
    map_pipeline = yield MapPipeline(job_name,
                                     mapper_spec,
                                     input_reader_spec,
                                     params=mapper_params,
                                     shards=shards)
    shuffler_pipeline = yield ShufflePipeline(job_name, map_pipeline)
    reducer_pipeline = yield ReducePipeline(job_name,
                                            reducer_spec,
                                            output_writer_spec,
                                            reducer_params,
                                            shuffler_pipeline)
    with pipeline.After(reducer_pipeline):
      all_temp_files = yield pipeline_common.Extend(
          map_pipeline, shuffler_pipeline)
      yield mapper_pipeline._CleanupPipeline(all_temp_files)
    yield pipeline_common.Return(reducer_pipeline)
