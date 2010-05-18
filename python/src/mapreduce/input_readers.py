#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
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
#

"""Defines input readers for MapReduce."""


import google



import datetime
import logging
import math
import time
import zipfile

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore
from google.appengine.api import memcache
from google.appengine.api.labs import taskqueue

from google.appengine.datastore import datastore_pb

from mapreduce.lib import blobstore
from google.appengine.ext import db
from mapreduce.lib import key_range
from mapreduce import base_handler
from mapreduce import context
from mapreduce import model
from mapreduce import quota
from mapreduce import util
from mapreduce.model import JsonMixin


class Error(Exception):
  """Base-class for exceptions in this module."""


class BadReaderParamsError(Error):
  """The input parameters to a reader were invalid."""


class InputReader(JsonMixin):
  """Abstract base class for input readers.

  InputReaders have the following properties:
   * They are created by using the split_input method to generate a set of
     InputReaders from a MapperSpec.
   * They generate inputs to the mapper via the iterator interface.
   * After creation, they can be serialized and resumed using the JsonMixin
     interface.
  """

  def __iter__(self):
    return self

  def next(self):
    """Returns the next input from this input reader as a key, value pair.

    Returns:
      The next input from this input reader.
    """
    pass

  @classmethod
  def from_json(cls, input_shard_state):
    """Creates an instance of the InputReader for the given input shard state.

    Args:
      input_shard_state: The InputReader state as a dict-like object.
    """
    pass

  def to_json(self):
    """Returns an input shard state for the remaining inputs.

    Returns:
      A json-izable version of the remaining InputReader.
    """
    pass

  @classmethod
  def split_input(cls, mapper_spec):
    """Returns a list of input shard states for the input spec.

    Args:
      mapper_spec: The MapperSpec for this InputReader.

    Returns:
      A json-izable list of InputReaders.

    Raises:
      BadReaderParamsError if required parameters are missing or invalid.
    """
    pass


class DatastoreInputReader(InputReader):
  """Represents a range in query results.

  DatastoreInputReader is a generator for either entities or keys in the key
  range, depending on the value of the keys_only parameter. Iterating over
  DatastoreInputReader changes its range past consumed entries.

  The class shouldn't be instantiated directly. Use split_input class method
  instead.
  """

  _BATCH_SIZE = 50

  _MAX_SHARD_COUNT = 256

  def __init__(self, entity_kind, key_range_param, batch_size, keys_only):
    """Create new DatastoreInputReader object.

    This is internal constructor. Use split_query instead.

    Args:
      entity_kind: entity kind as string.
      key_range_param: key range to process as key_range.KeyRange.
      batch_size: batch size of entity fetching.
      keys_only: if True, then send only keys to the mapper.
    """
    self._entity_kind = entity_kind
    self._key_range = key_range_param
    self.batch_size = batch_size
    self._keys_only = keys_only

  def __iter__(self):
    """Create a generator for entities or keys in the range.

    Iterating through entries moves query range past the consumed entries.

    Yields:
      next entry.
    """
    while True:
      entries_query = self._key_range.make_ascending_query(
          util.for_name(self._entity_kind), self._keys_only)
      entries_list = entries_query.fetch(limit=self.batch_size)

      if not entries_list:
        return

      for entry in entries_list:
        if hasattr(entry, 'key'):
          key = entry.key()
        else:
          key = entry

        self._key_range = key_range.KeyRange(key,
                                             self._key_range.key_end,
                                             self._key_range.direction,
                                             False,
                                             self._key_range.include_end)
        yield entry

  @classmethod
  def split_input(cls, mapper_spec):
    """Splits query into shards without fetching query results.

    Tries as best as it can to split the whole query result set into equal
    shards. Due to difficulty of making the perfect split, resulting shards'
    sizes might differ significantly from each other. The actual number of
    shards might also be less then requested (even 1), though it is never
    greater.

    Current implementation does key-lexicographic order splitting. It requires
    query not to specify any __key__-based ordering. If an index for
    query.order('-__key__') query is not present, an inaccurate guess at
    sharding will be made by splitting the full key range.

    Args:
      mapper_spec: MapperSpec with params containing 'entity_kind'.
        May also have 'batch_size' in the params to specify the number
        of entities to process in each batch.

    Returns:
      A list of DatastoreInputReader objects of length <= number_of_shards.

    Raises:
      BadReaderParamsError if required parameters are missing or invalid.
    """
    if mapper_spec.input_reader_class() != cls:
      raise BadReaderParamsError("Input reader class mismatch")
    params = mapper_spec.params
    if "entity_kind" not in params:
      raise BadReaderParamsError("Missing mapper parameter 'entity_kind'")

    entity_kind_name = params["entity_kind"]
    entity_kind = util.for_name(entity_kind_name)
    shard_count = mapper_spec.shard_count
    batch_size = int(params.get("batch_size", cls._BATCH_SIZE))
    keys_only = int(params.get("keys_only", False))

    ds_query = entity_kind.all()._get_query()
    ds_query.Order("__key__")
    first_entity = ds_query.Get(1)
    if not first_entity:
      return []
    else:
      first_entity_key = first_entity[0].key()

    ds_query.Order(("__key__", datastore.Query.DESCENDING))
    try:
      last_entity = ds_query.Get(1)
      last_entity_key = last_entity[0].key()
    except db.NeedIndexError, e:
      logging.warning("Cannot create accurate approximation of keyspace, "
                      "guessing instead. Please address this problem: %s", e)
      last_entity_key = key_range.KeyRange.guess_end_key(
          entity_kind.kind(), first_entity_key)

    full_keyrange = key_range.KeyRange(
        first_entity_key, last_entity_key, None, True, True)
    key_ranges = [full_keyrange]

    number_of_half_splits = int(math.floor(math.log(shard_count, 2)))
    for _ in range(0, number_of_half_splits):
      new_ranges = []
      for r in key_ranges:
        new_ranges += r.split_range(1)
      key_ranges = new_ranges

    return [DatastoreInputReader(entity_kind_name, r, batch_size, keys_only)
            for r in key_ranges]

  def to_json(self):
    """Serializes all the data in this query range into json form.

    Returns:
      all the data in json-compatible map.
    """
    json_dict = {"key_range": self._key_range.to_json(),
                 "entity_kind": self._entity_kind,
                 "batch_size": self.batch_size}
    if self._keys_only:
      json_dict["keys_only"] = True

    return json_dict

  def __str__(self):
    """Returns the string representation of this DatastoreInputReader."""
    return repr(self._key_range)

  @classmethod
  def from_json(cls, json):
    """Create new DatastoreInputReader from the json, encoded by to_json.

    Args:
      json: json map representation of DatastoreInputReader.

    Returns:
      an instance of DatastoreInputReader with all data deserialized from json.
    """
    query_range = cls(json["entity_kind"],
                      key_range.KeyRange.from_json(json["key_range"]),
                      json["batch_size"],
                      json.get("keys_only", False))
    return query_range


class BlobstoreLineInputReader(InputReader):
  """Input reader for a newline delimited blob in Blobstore."""

  _BLOB_BUFFER_SIZE = 64000

  _MAX_SHARD_COUNT = 256

  _MAX_BLOB_KEYS_COUNT = 246

  def __init__(self, blob_key, start_position, end_position):
    """Initializes this instance with the given blob key and character range.

    This BlobstoreInputReader will read from the first record starting after
    strictly after start_position until the first record ending at or after
    end_position (exclusive). As an exception, if start_position is 0, then
    this InputReader starts reading at the first record.

    Args:
      blob_key: the BlobKey that this input reader is processing.
      start_position: the position to start reading at.
      end_position: a position in the last record to read.
    """
    self._blob_key = blob_key
    self._current_request_position = start_position
    self._array_start_position = start_position
    self._current_record_start_0_offset = 0
    self._end_position = end_position
    self._data = []
    self._has_iterated = False

    self._read_before_start = (start_position != 0)

  def _get_next_block(self):
    """Adds the next _BLOB_BUFFER_SIZE block of data to the _data array.

    Increments _current_request_offset by the amount of data returned.

    Returns:
      False if no data was read, true otherwise.
    """
    blob_data = blobstore.fetch_data(
        self._blob_key,
        self._current_request_position,
        self._current_request_position + self._BLOB_BUFFER_SIZE)
    if not blob_data:
      return False
    self._current_request_position += len(blob_data)
    self._data.append(blob_data)
    return True

  def _current_record_position(self):
    """Returns the character position of the current record in the blob."""
    return self._current_record_start_0_offset + self._array_start_position

  def _find_end_of_current_record(self):
    """Finds the index of the newline terminating the current record.

    Doesn't read any additional data.

    Returns:
      -1 if the newline hasn't been read. An index in self._data[-1] otherwise.
    """
    start_of_neg1_search = 0
    if len(self._data) == 1:
      start_of_neg1_search = self._current_record_start_0_offset
    return self._data[-1].find("\n", start_of_neg1_search)

  def _has_complete_record(self):
    """True iff there is a complete and unprocessed record in self._data."""
    return self._data and self._find_end_of_current_record() != -1

  def _extract_record(self, newline_neg1_offset):
    """Returns the string containing the current record.

    The current record's boundaries are defined by
    _current_record_start_0_offset inclusive and newline_neg1_offset exclusive.
    """
    if len(self._data) == 1:
      record = self._data[0][
          self._current_record_start_0_offset:newline_neg1_offset]
    elif len(self._data) > 1:
      remaining_blocks = self._data[1:-1]
      remaining_blocks.append(self._data[-1][:newline_neg1_offset])
      record = "".join([self._data[0][self._current_record_start_0_offset:]] +
                       remaining_blocks)
    return record

  def _read(self):
    if self._read_before_start:
      self._read_before_start = False
      self._read()


    if self._current_record_position() > self._end_position:
      return None

    while not self._has_complete_record():
      if not self._get_next_block():
        if not self._data:
          return None
        break

    newline_neg1_offset = self._find_end_of_current_record()
    if newline_neg1_offset == -1:
      newline_neg1_offset = len(self._data[-1])

    record = self._extract_record(newline_neg1_offset)
    record_start_position = self._current_record_position()

    self._array_start_position += sum([len(d) for d in self._data[:-1]])
    self._data = self._data[-1:]
    self._current_record_start_0_offset = newline_neg1_offset + 1

    return record_start_position, record

  def next(self):
    """Returns the next input from this input reader."""
    self._has_iterated = True
    resp = self._read()
    if not resp:
      raise StopIteration()
    return resp

  def to_json(self):
    """Returns an json-compatible input shard spec for remaining inputs."""
    new_pos = self._current_record_position()
    if self._has_iterated:
      new_pos -= 1
    return {"blob_key": self._blob_key,
            "initial_position": new_pos,
            "end_position": self._end_position}

  def __str__(self):
    """Returns the string representation of this BlobstoreLineInputReader."""
    return "blobstore.BlobKey(%r):[%d, %d]" % (
        self._blob_key, self._current_request_position, self._end_position)

  @classmethod
  def from_json(cls, json):
    """Instantiates an instance of this InputReader for the given shard spec.
    """
    return cls(json["blob_key"],
               json["initial_position"],
               json["end_position"])

  @classmethod
  def split_input(cls, mapper_spec):
    """Returns a list of shard_count input_spec_shards for input_spec.

    Args:
      mapper_spec: The mapper specification to split from.

    Returns:
      A list of BlobstoreInputReaders corresponding to the specified shards.

    Raises:
      BadReaderParamsError if required parameters are missing or invalid.
    """
    if mapper_spec.input_reader_class() != cls:
      raise BadReaderParamsError("Mapper input reader class mismatch")
    params = mapper_spec.params
    if "blob_keys" not in params:
      raise BadReaderParamsError("Must specify 'blob_keys' for mapper input")

    blob_keys = params["blob_keys"]
    if isinstance(blob_keys, basestring):
      blob_keys = [blob_keys]
    if len(blob_keys) > cls._MAX_BLOB_KEYS_COUNT:
      raise BadReaderParamsError("Too many 'blob_keys' for mapper input")
    if not blob_keys:
      raise BadReaderParamsError("No 'blob_keys' specified for mapper input")

    blob_sizes = {}
    for blob_key in blob_keys:
      blob_info = blobstore.BlobInfo.get(blobstore.BlobKey(blob_key))
      blob_sizes[blob_key] = blob_info.size

    shard_count = min(cls._MAX_SHARD_COUNT, mapper_spec.shard_count)
    shards_per_blob = shard_count // len(blob_keys)
    if shards_per_blob == 0:
      shards_per_blob = 1

    chunks = []
    for blob_key, blob_size in blob_sizes.items():
      blob_chunk_size = blob_size // shards_per_blob
      for i in xrange(shards_per_blob - 1):
        chunks.append(BlobstoreLineInputReader.from_json(
            {"blob_key": blob_key,
             "initial_position": blob_chunk_size * i,
             "end_position": blob_chunk_size * (i + 1)}))
      chunks.append(BlobstoreLineInputReader.from_json(
          {"blob_key": blob_key,
           "initial_position": blob_chunk_size * (shard_count - 1),
           "end_position": blob_size}))
    return chunks


class BlobstoreZipInputReader(InputReader):
  """Input reader for files from a zip archive stored in the Blobstore.

  Each instance of the reader will read the TOC, from the end of the zip file,
  and then only the contained files which it is responsible for.
  """

  _MAX_SHARD_COUNT = 256

  def __init__(self, blob_key, start_index, end_index,
               _reader=blobstore.BlobReader):
    """Initializes this instance with the given blob key and file range.

    This BlobstoreZipInputReader will read from the file with index start_index
    up to but not including the file with index end_index.

    Args:
      blob_key: the BlobKey that this input reader is processing.
      start_index: the index of the first file to read.
      end_index: the index of the first file that will not be read.
      _reader: a callable that returns a file-like object for reading blobs.
          Used for dependency injection.
    """
    self._blob_key = blob_key
    self._start_index = start_index
    self._end_index = end_index
    self._reader = _reader
    self._zip = None
    self._entries = None

  def next(self):
    """Returns the next input from this input reader as (ZipInfo, opener) tuple.

    Returns:
      The next input from this input reader, in the form of a 2-tuple.
      The first element of the tuple is a zipfile.ZipInfo object.
      The second element of the tuple is a zero-argument function that, when
      called, returns the complete body of the file.
    """
    if not self._zip:
      self._zip = zipfile.ZipFile(self._reader(self._blob_key))
      self._entries = self._zip.infolist()[self._start_index:self._end_index]
      self._entries.reverse()
    if not self._entries:
      raise StopIteration()
    entry = self._entries.pop()
    self._start_index += 1
    return (entry, lambda: self._zip.read(entry.filename))

  @classmethod
  def from_json(cls, json):
    """Creates an instance of the InputReader for the given input shard state.

    Args:
      input_shard_state: The InputReader state as a dict-like object.
    """
    return cls(json["blob_key"], json["start_index"], json["end_index"])

  def to_json(self):
    """Returns an input shard state for the remaining inputs.

    Returns:
      A json-izable version of the remaining InputReader.
    """
    return {"blob_key": self._blob_key,
            "start_index": self._start_index,
            "end_index": self._end_index}

  def __str__(self):
    """Returns the string representation of this BlobstoreZipInputReader."""
    return "blobstore.BlobKey(%r):[%d, %d]" % (
        self._blob_key, self._start_index, self._end_index)

  @classmethod
  def split_input(cls, mapper_spec, _reader=blobstore.BlobReader):
    """Returns a list of input shard states for the input spec.

    Args:
      mapper_spec: The MapperSpec for this InputReader.
      _reader: a callable that returns a file-like object for reading blobs.
          Used for dependency injection.

    Returns:
      A json-izable list of InputReaders.

    Raises:
      BadReaderParamsError if required parameters are missing or invalid.
    """
    if mapper_spec.input_reader_class() != cls:
      raise BadReaderParamsError("Mapper input reader class mismatch")
    params = mapper_spec.params
    if "blob_key" not in params:
      raise BadReaderParamsError("Must specify 'blob_key' for mapper input")

    blob_key = params["blob_key"]
    zip_input = zipfile.ZipFile(_reader(blob_key))
    files = zip_input.infolist()
    total_size = sum(x.file_size for x in files)
    num_shards = min(mapper_spec.shard_count, cls._MAX_SHARD_COUNT)
    size_per_shard = total_size // num_shards

    shard_start_indexes = [0]
    current_shard_size = 0
    for i, file in enumerate(files):
      current_shard_size += file.file_size
      if current_shard_size >= size_per_shard:
        shard_start_indexes.append(i + 1)
        current_shard_size = 0

    if shard_start_indexes[-1] != len(files):
      shard_start_indexes.append(len(files))

    return [cls(blob_key, start_index, end_index, _reader)
            for start_index, end_index
            in zip(shard_start_indexes, shard_start_indexes[1:])]
