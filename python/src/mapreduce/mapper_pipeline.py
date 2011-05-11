#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""Pipelines for mapreduce library."""





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
    self.set_status(console_url="%s/details/detail?job_id=%s" % (
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
