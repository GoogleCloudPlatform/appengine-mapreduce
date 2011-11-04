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





from mapreduce.lib import files
from mapreduce import base_handler
from mapreduce import control
from mapreduce import model


class MapperPipeline(base_handler.PipelineBase):
  """Pipeline wrapper for mapper job.

  Args:
    job_name: mapper job name as string
    handler_spec: mapper handler specification as string.
    input_reader_spec: input reader specification as string.
    output_writer_spec: output writer specification as string.
    params: mapper parameters for input reader and output writer as dict.
    shards: number of shards in the job as int.

  Returns:
    The list of filenames mapper was outputting to.
  """
  async = True

  # TODO(user): we probably want to output counters too.
  # Might also need to double filenames as named output.
  output_names = [
      # Job ID. MapreduceState.get_by_job_id can be used to load
      # mapreduce state. Is filled immediately after job starts up.
      "job_id",
      ]

  def run(self,
          job_name,
          handler_spec,
          input_reader_spec,
          output_writer_spec=None,
          params=None,
          shards=None):
    mapreduce_id = control.start_map(
        job_name,
        handler_spec,
        input_reader_spec,
        params or {},
        mapreduce_parameters={
            "done_callback": self.get_callback_url(),
            "done_callback_method": "GET",
            "pipeline_id": self.pipeline_id,
        },
        shard_count=shards,
        output_writer_spec=output_writer_spec,
        )
    self.fill(self.outputs.job_id, mapreduce_id)
    self.set_status(console_url="%s/detail?job_id=%s" % (
        (base_handler._DEFAULT_BASE_PATH, mapreduce_id)))

  def callback(self):
    mapreduce_id = self.outputs.job_id.value
    mapreduce_state = model.MapreduceState.get_by_job_id(mapreduce_id)
    mapper_spec = mapreduce_state.mapreduce_spec.mapper
    files = None
    output_writer_class = mapper_spec.output_writer_class()
    if output_writer_class:
      files = output_writer_class.get_filenames(mapreduce_state)

    self.complete(files)


class _CleanupPipeline(base_handler.PipelineBase):
  """A pipeline to do a cleanup for mapreduce jobs.

  Args:
    filename_or_list: list of files or file lists to delete.
  """

  def delete_file_or_list(self, filename_or_list):
    if isinstance(filename_or_list, list):
      for filename in filename_or_list:
        self.delete_file_or_list(filename)
    else:
      filename = filename_or_list
      for _ in range(10):
        try:
          files.delete(filename)
          break
        except:
          pass

  def run(self, temp_files):
    self.delete_file_or_list(temp_files)

