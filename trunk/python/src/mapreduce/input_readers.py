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

"""Defines input readers for MapReduce."""



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

  # Mapreduce parameters.
  _APP_PARAM = "_app"
  MAPPER_PARAMS = "mapper_params"

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


# TODO(user): Use cursor API as soon as we have it available.
class DatastoreInputReader(InputReader):
  """Represents a range in query results.

  DatastoreInputReader is a generator for either entities or keys in the key
  range, depending on the value of the keys_only parameter. Iterating over
  DatastoreInputReader changes its range past consumed entries.

  The class shouldn't be instantiated directly. Use split_input class method
  instead.
  """

  # Number of entities to fetch at once while doing scanning.
  _BATCH_SIZE = 50

  # Maximum number of shards we'll create.
  _MAX_SHARD_COUNT = 256

  # Mapreduce parameters.
  ENTITY_KIND_PARAM = "entity_kind"
  KEYS_ONLY_PARAM = "keys_only"
  BATCH_SIZE_PARAM = "batch_size"
  KEY_RANGE_PARAM = "key_range"

  # TODO(user): Add support for arbitrary queries. It's not possible to
  # support them without cursors since right now you can't even serialize query
  # definition.
  def __init__(self, entity_kind, key_range_param, mapper_params):
    """Create new DatastoreInputReader object.

    This is internal constructor. Use split_query instead.

    Args:
      entity_kind: entity kind as string.
      key_range_param: key range to process as key_range.KeyRange.
      mapper_params: mapper parameters as defined by user.
    """
    self._entity_kind = entity_kind
    self._key_range = key_range_param
    self._mapper_params = mapper_params
    self._batch_size = self._mapper_params.get(
        self.BATCH_SIZE_PARAM, self._BATCH_SIZE)
    self._keys_only = self._mapper_params.get(self.KEYS_ONLY_PARAM, False)

  def __iter__(self):
    """Create a generator for entities or keys in the range.

    Iterating through entries moves query range past the consumed entries.

    Yields:
      next entry.
    """
    while True:
      if self._keys_only:
        raw_entity_kind = util.get_short_name(self._entity_kind)
        entries_query = self._key_range.make_ascending_datastore_query(
            raw_entity_kind, keys_only=self._keys_only)
        entries_list = entries_query.Get(limit=self._batch_size)
      else:
        entries_query = self._key_range.make_ascending_query(
            util.for_name(self._entity_kind), self._keys_only)
        entries_list = entries_query.fetch(limit=self._batch_size)

      if not entries_list:
        return

      for entry in entries_list:
        if hasattr(entry, "key"):
          key = entry.key()
        else:
          key = entry

        self._key_range.advance(key)
        yield entry

  # TODO(user): use query splitting functionality when it becomes available
  # instead.
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
    if cls.ENTITY_KIND_PARAM not in params:
      raise BadReaderParamsError("Missing mapper parameter 'entity_kind'")

    entity_kind_name = params[cls.ENTITY_KIND_PARAM]
    shard_count = mapper_spec.shard_count
    app = params.get(cls._APP_PARAM)
    keys_only = int(params.get(cls.KEYS_ONLY_PARAM, False))

    # Fail fast if Model cannot be located.
    if not keys_only:
      util.for_name(entity_kind_name)

    # we use datastore.Query instead of ext.db.Query here, because we can't
    # erase ordering on db.Query once we set it.
    raw_entity_kind = util.get_short_name(entity_kind_name)
    ds_query = datastore.Query(kind=raw_entity_kind, _app=app, keys_only=True)
    ds_query.Order("__key__")
    first_entity_key_list = ds_query.Get(1)
    if not first_entity_key_list:
      return []
    first_entity_key = first_entity_key_list[0]

    ds_query.Order(("__key__", datastore.Query.DESCENDING))
    try:
      last_entity_key, = ds_query.Get(1)
    except db.NeedIndexError, e:
      # TODO(user): Show this error in the worker log, not the app logs.
      logging.warning("Cannot create accurate approximation of keyspace, "
                      "guessing instead. Please address this problem: %s", e)
      # TODO(user): Use a key-end hint from the user input parameters
      # in this case, in the event the user has a good way of figuring out
      # the range of the keyspace.
      last_entity_key = key_range.KeyRange.guess_end_key(
          raw_entity_kind, first_entity_key)

    full_keyrange = key_range.KeyRange(
        first_entity_key, last_entity_key, None, True, True, _app=app)
    key_ranges = [full_keyrange]

    number_of_half_splits = int(math.floor(math.log(shard_count, 2)))
    for _ in range(0, number_of_half_splits):
      new_ranges = []
      for r in key_ranges:
        new_ranges += r.split_range(1)
      key_ranges = new_ranges

    return [cls(entity_kind_name, r, params) for r in key_ranges]

  def to_json(self):
    """Serializes all the data in this query range into json form.

    Returns:
      all the data in json-compatible map.
    """
    json_dict = {self.KEY_RANGE_PARAM: self._key_range.to_json(),
                 self.ENTITY_KIND_PARAM: self._entity_kind,
                 self.MAPPER_PARAMS: self._mapper_params}
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
    query_range = cls(json[cls.ENTITY_KIND_PARAM],
                      key_range.KeyRange.from_json(json[cls.KEY_RANGE_PARAM]),
                      json[cls.MAPPER_PARAMS])
    return query_range


class BlobstoreLineInputReader(InputReader):
  """Input reader for a newline delimited blob in Blobstore."""

  # TODO(user): Should we set this based on MAX_BLOB_FETCH_SIZE?
  _BLOB_BUFFER_SIZE = 64000

  # Maximum number of shards to allow.
  _MAX_SHARD_COUNT = 256

  # Maximum number of blobs to allow.
  _MAX_BLOB_KEYS_COUNT = 246

  # Mapreduce parameters.
  INITIAL_POSITION_PARAM = "initial_position"
  END_POSITION_PARAM = "end_position"
  BLOB_KEY_PARAM = "blob_key"

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
    self._blob_reader = blobstore.BlobReader(blob_key,
                                             self._BLOB_BUFFER_SIZE,
                                             start_position)
    self._end_position = end_position
    self._has_iterated = False
    self._read_before_start = bool(start_position)

  def next(self):
    """Returns the next input from this input reader."""
    self._has_iterated = True

    if self._read_before_start:
      self._blob_reader.readline()
      self._read_before_start = False
    start_position = self._blob_reader.tell()

    if start_position >= self._end_position:
      raise StopIteration()

    line = self._blob_reader.readline()

    if not line:
      raise StopIteration()

    return start_position, line.rstrip("\n")

  def to_json(self):
    """Returns an json-compatible input shard spec for remaining inputs."""
    new_pos = self._blob_reader.tell()
    if self._has_iterated:
      new_pos -= 1
    return {self.BLOB_KEY_PARAM: self._blob_key,
            self.INITIAL_POSITION_PARAM: new_pos,
            self.END_POSITION_PARAM: self._end_position}

  def __str__(self):
    """Returns the string representation of this BlobstoreLineInputReader."""
    return "blobstore.BlobKey(%r):[%d, %d]" % (
        self._blob_key, self._blob_reader.tell(), self._end_position)

  @classmethod
  def from_json(cls, json):
    """Instantiates an instance of this InputReader for the given shard spec.
    """
    return cls(json[cls.BLOB_KEY_PARAM],
               json[cls.INITIAL_POSITION_PARAM],
               json[cls.END_POSITION_PARAM])

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
      # This is a mechanism to allow multiple blob keys (which do not contain
      # commas) in a single string. It may go away.
      blob_keys = blob_keys.split(',')
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
            {cls.BLOB_KEY_PARAM: blob_key,
             cls.INITIAL_POSITION_PARAM: blob_chunk_size * i,
             cls.END_POSITION_PARAM: blob_chunk_size * (i + 1)}))
      chunks.append(BlobstoreLineInputReader.from_json(
          {cls.BLOB_KEY_PARAM: blob_key,
           cls.INITIAL_POSITION_PARAM: blob_chunk_size * (shards_per_blob - 1),
           cls.END_POSITION_PARAM: blob_size}))
    return chunks


class BlobstoreZipInputReader(InputReader):
  """Input reader for files from a zip archive stored in the Blobstore.

  Each instance of the reader will read the TOC, from the end of the zip file,
  and then only the contained files which it is responsible for.
  """

  # Maximum number of shards to allow.
  _MAX_SHARD_COUNT = 256

  # Mapreduce parameters.
  BLOB_KEY_PARAM = "blob_key"
  START_INDEX_PARAM = "start_index"
  END_INDEX_PARAM = "end_index"

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
      # Get a list of entries, reversed so we can pop entries off in order
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
    return cls(json[cls.BLOB_KEY_PARAM],
               json[cls.START_INDEX_PARAM],
               json[cls.END_INDEX_PARAM])

  def to_json(self):
    """Returns an input shard state for the remaining inputs.

    Returns:
      A json-izable version of the remaining InputReader.
    """
    return {cls.BLOB_KEY_PARAM: self._blob_key,
            cls.START_INDEX_PARAM: self._start_index,
            cls.END_INDEX_PARAM: self._end_index}

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
    if cls.BLOB_KEY_PARAM not in params:
      raise BadReaderParamsError("Must specify 'blob_key' for mapper input")

    blob_key = params[cls.BLOB_KEY_PARAM]
    zip_input = zipfile.ZipFile(_reader(blob_key))
    files = zip_input.infolist()
    total_size = sum(x.file_size for x in files)
    num_shards = min(mapper_spec.shard_count, cls._MAX_SHARD_COUNT)
    size_per_shard = total_size // num_shards

    # Break the list of files into sublists, each of approximately
    # size_per_shard bytes.
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
