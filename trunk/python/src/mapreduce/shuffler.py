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

"""Mapreduce shuffler implementation."""

from __future__ import with_statement




import gc
import heapq
import logging
import time

from mapreduce.lib import pipeline
from mapreduce.lib import files
from mapreduce.lib.files import file_service_pb
from mapreduce.lib.files import records
from google.appengine.ext import db
from mapreduce import base_handler
from mapreduce import context
from mapreduce import errors
from mapreduce import input_readers
from mapreduce import mapper_pipeline
from mapreduce import operation
from mapreduce import output_writers


class _OutputFile(db.Model):
  """Entity to store output filenames of pipelines.

  These entities are always children of key returned by get_root_key().
  """

  @classmethod
  def kind(cls):
    """Returns entity kind."""
    return "_AE_MR_OutputFile"

  @classmethod
  def get_root_key(cls, job_id):
    """Get root key to store output files.

    Args:
      job_id: pipeline's job id.

    Returns:
      root key for a given job id to store output file entities.
    """
    return db.Key.from_path(cls.kind(), job_id)


def _compare_keys(proto1, proto2):
  """Compare two KeyValue protos by key."""
  return cmp(proto1.key(), proto2.key())


class _BatchRecordsReader(input_readers.RecordsReader):
  """Records reader that reads in big batches."""

  BATCH_SIZE = 1024*1024 * 3

  def __iter__(self):
    records = []
    size = 0
    for record in input_readers.RecordsReader.__iter__(self):
      records.append(record)
      size += len(record)
      if size > self.BATCH_SIZE:
        yield records
        size = 0
        records = []
        gc.collect()
    if records:
      yield records


def _sort_records(records):
  """Map function sorting records.

  Converts records to KeyValue protos, sorts them by key and writes them
  into new blobstore file. Creates _OutputFile entity to record resulting
  file name.

  Args:
    records: list of records which are serialized KeyValue protos.
  """
  ctx = context.get()
  l = len(records)
  proto_records = [None] * l

  logging.debug("Parsing")
  for i in range(l):
    proto = file_service_pb.KeyValue()
    proto.ParseFromString(records[i])
    proto_records[i] = proto

  logging.debug("Sorting")
  proto_records.sort(cmp=_compare_keys)

  logging.debug("Writing")
  blob_file_name = (ctx.mapreduce_spec.name + "-" +
                    ctx.mapreduce_id + "-output")
  output_path = files.blobstore.create(
      _blobinfo_uploaded_filename=blob_file_name)
  with output_writers.RecordsPool(output_path, ctx=ctx) as pool:
    for proto in proto_records:
      pool.append(proto.Encode())

  logging.debug("Finalizing")
  files.finalize(output_path)
  time.sleep(1)  # TODO(user): Hack for HR datastore replication delay.
  output_path = files.blobstore.get_file_name(
      files.blobstore.get_blob_key(output_path))

  entity = _OutputFile(key_name=output_path,
                       parent=_OutputFile.get_root_key(ctx.mapreduce_id))
  entity.put()


class _CollectOutputFiles(base_handler.PipelineBase):
  """Collect output file names from _OutputFile entities for a given job.

  Args:
    job_id: job id to load filenames.

  Returns:
    list of filenames produced by job id.
  """

  def run(self, job_id):
    entities = _OutputFile.all().ancestor(
        _OutputFile.get_root_key(job_id))
    return [entity.key().name() for entity in entities]


class SortPipeline(base_handler.PipelineBase):
  """A pipeline to sort multiple key-value files.

  Args:
    filenames: list of file names to sort. Files have to be of records format
    defined by Files API and contain serialized file_service_pb.KeyValue
    protocol messages.

  Returns:
    The list of filenames as string. Resulting files have the same format as
    input and are sorted by key.
  """

  def run(self, filenames):
    mapper = yield mapper_pipeline.MapperPipeline(
        "sort",
        __name__ + "._sort_records",
        __name__ + "._BatchRecordsReader",
        None,
        {
            "files": filenames,
            "processing_rate": 1000000,
        },
        shards=1)
    # TODO(user): delete _OutputFile entities after collect
    with pipeline.After(mapper):
      yield _CollectOutputFiles(mapper.job_id)


class _MergingReader(input_readers.InputReader):
  """Reader which merge-reads multiple sorted KeyValue files.

  Reads list of lists of filenames. Each filename list constitutes one shard
  and is merged together.

  Yields (key, values) tuple.
  """

  expand_parameters = True

  FILES_PARAM = "files"

  def __init__(self, offsets):
    """Constructor.

    Args:
      offsets: offsets for each input file to start from as list of ints.
    """
    self._offsets = offsets

  def __iter__(self):
    """Iterate over records in input files.

    self._offsets is always correctly updated so that stopping iterations
    doesn't skip records and doesn't read the same record twice.
    """
    ctx = context.get()
    mapper_spec = ctx.mapreduce_spec.mapper
    shard_number = ctx.shard_state.shard_number
    filenames = mapper_spec.params[self.FILES_PARAM][shard_number]

    if len(filenames) != len(self._offsets):
      raise Exception("Files list and offsets do not match.")

    # Heap with (Key, Value, Index, reader) pairs.
    readers = []

    # Initialize heap
    for (i, filename) in enumerate(filenames):
      offset = self._offsets[i]
      reader = records.RecordsReader(files.BufferedFile(filename))
      reader.seek(offset)
      readers.append((None, None, i, reader))

    # Read records from heap and merge values with the same key.
    current_result = None
    while readers:
      (key, value, index, reader) = readers[0]

      if key is not None:
        if current_result and key != current_result[0]:
          # New key encountered. Yield corrent key.
          yield current_result
        if not current_result or key != current_result[0]:
          current_result = (key, [])
        current_result[1].append(value)

      # Read next key/value from reader.
      try:
        self._offsets[index] = reader.tell()
        start_time = time.time()
        binary_record = reader.read()
        # update counters
        if context.get():
          operation.counters.Increment(
              input_readers.COUNTER_IO_READ_BYTES,
              len(binary_record))(context.get())
          operation.counters.Increment(
              input_readers.COUNTER_IO_READ_MSEC,
              int((time.time() - start_time) * 1000))(context.get())
        proto = file_service_pb.KeyValue()
        proto.ParseFromString(binary_record)
        # Put read data back into heap.
        heapq.heapreplace(readers,
                          (proto.key(), proto.value(), index, reader))
      except EOFError:
        heapq.heappop(readers)

    # Yield leftovers.
    if current_result:
      yield current_result

  @classmethod
  def from_json(cls, json):
    """Restore reader from json state."""
    return cls(json["offsets"])

  def to_json(self):
    """Serialize reader state to json."""
    return {"offsets": self._offsets}

  @classmethod
  def split_input(cls, mapper_spec):
    """Split input into multiple shards.

    Only one shard is generated at the moment.
    """
    filelists = mapper_spec.params[cls.FILES_PARAM]
    return [cls([0] * len(files)) for files in filelists]

  @classmethod
  def validate(cls, mapper_spec):
    """Validate reader parameters in mapper_spec."""
    if mapper_spec.input_reader_class() != cls:
      raise errors.BadReaderParamsError("Input reader class mismatch")
    params = mapper_spec.params
    if not cls.FILES_PARAM in params:
      raise errors.BadReaderParamsError("Missing files parameter.")


class _KeyValueBlobstoreOutputWriter(output_writers.BlobstoreOutputWriterBase):
  """An OutputWriter which outputs data into blobstore in key-value format.

  The output is tailored towards shuffler needs. Each mapper shard creates
  number of outpupt files equal to the number of shards itself. On each
  output it hashes the key and picks a file corresponding to a key. This way,
  file 0 from all shards will have all key/values with the same hash key modulo.
  """

  def __init__(self, filenames):
    """Constructor.

    Args:
      filenames: list of filenames that this writer outputs to.
    """
    self._filenames = filenames

  @classmethod
  def validate(cls, mapper_spec):
    """Validates mapper specification.

    Args:
      mapper_spec: an instance of model.MapperSpec to validate.
    """
    if mapper_spec.output_writer_class() != cls:
      raise errors.BadWriterParamsError("Output writer class mismatch")

  @classmethod
  def init_job(cls, mapreduce_state):
    """Initialize job-level writer state.

    Args:
      mapreduce_state: an instance of model.MapreduceState describing current
      job. State can be modified during initialization.
    """
    shards = mapreduce_state.mapreduce_spec.mapper.shard_count
    subshards = shards

    filenames = []
    for i in range(shards):
      subshard_filenames = []
      for j in range(subshards):
        blob_file_name = (mapreduce_state.mapreduce_spec.name +
                          "-" + mapreduce_state.mapreduce_spec.mapreduce_id +
                          "-output-" + str(i) + "-" + str(j))
        subshard_filenames.append(
            files.blobstore.create(
                _blobinfo_uploaded_filename=blob_file_name))
      filenames.append(subshard_filenames)
    mapreduce_state.writer_state = {"filenames": filenames}

  @classmethod
  def finalize_job(cls, mapreduce_state):
    """Finalize job-level writer state.

    Args:
      mapreduce_state: an instance of model.MapreduceState describing current
        job. State can be modified during finalization.
    """
    finalized_filenames = []
    for subshard_filenames in mapreduce_state.writer_state["filenames"]:
      finalized_subshard_filenames = []
      for filename in subshard_filenames:
        finalized_subshard_filenames.append(
            files.blobstore.get_file_name(
                files.blobstore.get_blob_key(filename)))
      finalized_filenames.append(finalized_subshard_filenames)
    mapreduce_state.writer_state = {"filenames": finalized_filenames}

  @classmethod
  def from_json(cls, json):
    """Creates an instance of the OutputWriter for the given json state.

    Args:
      json: The OutputWriter state as a dict-like object.

    Returns:
      An instance of the OutputWriter configured using the values of json.
    """
    return cls(json["filenames"])

  def to_json(self):
    """Returns writer state to serialize in json.

    Returns:
      A json-izable version of the OutputWriter state.
    """
    return {"filenames": self._filenames}

  @classmethod
  def create(cls, mapreduce_state, shard_number):
    """Create new writer for a shard.

    Args:
      mapreduce_state: an instance of model.MapreduceState describing current
      job. State can be modified.
      shard_number: shard number as integer.
    """
    filenames = mapreduce_state.writer_state["filenames"][shard_number]
    return cls(filenames)

  def finalize(self, ctx, shard_number):
    """Finalize writer shard-level state.

    Args:
      ctx: an instance of context.Context.
      shard_number: shard number as integer.
    """
    finalized_filenames = []
    for filename in self._filenames:
      files.finalize(filename)

  @classmethod
  def get_filenames(cls, mapreduce_state):
    """Obtain output filenames from mapreduce state.

    Args:
      mapreduce_state: an instance of model.MapreduceState

    Returns:
        list of filenames this writer writes to or None if writer
        doesn't write to a file.
    """
    return mapreduce_state.writer_state["filenames"]

  def write(self, data, ctx):
    """Write data.

    Args:
      data: actual data yielded from handler. Type is writer-specific.
      ctx: an instance of context.Context.
    """
    if len(data) != 2:
      logging.error("Got bad tuple of length %d (2-tuple expected): %s",
                    len(data), data)

    try:
      key = str(data[0])
      value = str(data[1])
    except TypeError:
      logging.error("Expecting a tuple, but got %s: %s",
                    data.__class__.__name__, data)

    file_index = key.__hash__() % len(self._filenames)
    pool_name = "kv_pool%d" % file_index
    filename = self._filenames[file_index]

    if ctx.get_pool(pool_name) is None:
      ctx.register_pool(pool_name,
                        output_writers.RecordsPool(filename=filename, ctx=ctx))
    proto = file_service_pb.KeyValue()
    proto.set_key(key)
    proto.set_value(value)
    ctx.get_pool(pool_name).append(proto.Encode())
