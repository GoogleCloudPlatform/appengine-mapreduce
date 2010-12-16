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



# pylint: disable-msg=C6409

import logging
import math
import StringIO
import time
import zipfile

from google.appengine.api import datastore
from google.appengine.api import namespace_manager
# TODO(user): Remove this hack once 1.4.0 is live in production.
try:
  from google.appengine.datastore import datastore_rpc
except ImportError:
  datastore_rpc = None
from mapreduce.lib import blobstore
from google.appengine.ext import db
from mapreduce.lib import key_range
from google.appengine.ext.db import metadata
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
   * They are cast to string for a user-readable description; it may be
     valuable to implement __str__.
  """

  # Mapreduce parameters.
  _APP_PARAM = "_app"
  NAMESPACES_PARAM = "namespaces"
  MAPPER_PARAMS = "mapper_params"

  def __iter__(self):
    return self

  def next(self):
    """Returns the next input from this input reader as a key, value pair.

    Returns:
      The next input from this input reader.
    """
    raise NotImplementedError("next() not implemented in %s" % cls)

  @classmethod
  def from_json(cls, input_shard_state):
    """Creates an instance of the InputReader for the given input shard state.

    Args:
      input_shard_state: The InputReader state as a dict-like object.

    Returns:
      An instance of the InputReader configured using the values of json.
    """
    raise NotImplementedError("from_json() not implemented in %s" % cls)

  def to_json(self):
    """Returns an input shard state for the remaining inputs.

    Returns:
      A json-izable version of the remaining InputReader.
    """
    raise NotImplementedError("to_json() not implemented in %s" % cls)

  @classmethod
  def split_input(cls, mapper_spec):
    """Returns a list of input readers for the input spec.

    Args:
      mapper_spec: The MapperSpec for this InputReader.

    Returns:
      A list of InputReaders.
    """
    raise NotImplementedError("split_input() not implemented in %s" % cls)

  @classmethod
  def validate(cls, mapper_spec):
    """Validates mapper spec and all mapper parameters.

    Args:
      mapper_spec: The MapperSpec for this InputReader.

    Raises:
      BadReaderParamsError: required parameters are missing or invalid.
    """
    raise NotImplementedError("validate() not implemented in %s" % cls)


# TODO(user): Use cursor API as soon as we have it available.
class DatastoreInputReader(InputReader):
  """Represents a range in query results.

  DatastoreInputReader yields model instances from the entities in a given key
  range. Iterating over DatastoreInputReader changes its range past consumed
  entries.

  The class shouldn't be instantiated directly. Use the split_input class method
  instead.
  """

  # Number of entities to fetch at once while doing scanning.
  _BATCH_SIZE = 50

  # Maximum number of shards we'll create.
  _MAX_SHARD_COUNT = 256

  # __scatter__ oversampling factor
  _OVERSAMPLING_FACTOR = 32

  # Mapreduce parameters.
  ENTITY_KIND_PARAM = "entity_kind"
  KEYS_ONLY_PARAM = "keys_only"
  BATCH_SIZE_PARAM = "batch_size"
  KEY_RANGE_PARAM = "key_range"

  # TODO(user): Add support for arbitrary queries. It's not possible to
  # support them without cursors since right now you can't even serialize query
  # definition.
  def __init__(self, entity_kind, key_ranges, batch_size = _BATCH_SIZE):
    """Create new DatastoreInputReader object.

    This is internal constructor. Use split_query instead.

    Args:
      entity_kind: entity kind as string.
      key_ranges: a sequence of key_range.KeyRange instances to process.
      batch_size: size of read batch as int.
    """
    self._entity_kind = entity_kind
    # Reverse the KeyRanges so they can be processed in order as a stack of
    # work items.
    self._key_ranges = list(reversed(key_ranges))
    self._batch_size = int(batch_size)

  def __iter__(self):
    """Create a generator for model instances for entities.

    Iterating through entities moves query range past the consumed entities.

    Yields:
      next model instance.
    """
    while True:
      if self._current_key_range is None:
        break

      while True:
        query = self._current_key_range.make_ascending_query(
            util.for_name(self._entity_kind))
        results = query.fetch(limit=self._batch_size)

        if not results:
          self._advance_key_range()
          break

        for model_instance in results:
          key = model_instance.key()

          self._current_key_range.advance(key)
          yield model_instance

  @property
  def _current_key_range(self):
    if self._key_ranges:
      return self._key_ranges[-1]
    else:
      return None

  def _advance_key_range(self):
    if self._key_ranges:
      self._key_ranges.pop()

  # TODO(user): use query splitting functionality when it becomes available
  # instead.
  @classmethod
  def _split_input_from_namespace(cls, app, namespace, entity_kind_name,
                                  shard_count):
    """Return KeyRange objects. Helper for _split_input_from_params."""

    raw_entity_kind = util.get_short_name(entity_kind_name)

    if shard_count == 1:
      # With one shard we don't need to calculate any splitpoints at all.
      return [key_range.KeyRange(namespace=namespace, _app=app)]

    # we use datastore.Query instead of ext.db.Query here, because we can't
    # erase ordering on db.Query once we set it.
    ds_query = datastore.Query(kind=raw_entity_kind,
                               namespace=namespace,
                               _app=app,
                               keys_only=True)
    ds_query.Order("__scatter__")
    random_keys = ds_query.Get(shard_count * cls._OVERSAMPLING_FACTOR)
    if not random_keys:
      # This might mean that there are no entities with scatter property
      # or there are no entities at all.
      return [key_range.KeyRange(namespace=namespace, _app=app)]
    random_keys.sort()
    # pick shard_count - 1 points to generate shard_count splits
    split_points_count = shard_count - 1
    if len(random_keys) > split_points_count:
      # downsample
      random_keys = [random_keys[len(random_keys)*i/split_points_count]
                     for i in range(split_points_count)]

    key_ranges = []

    key_ranges.append(key_range.KeyRange(
        key_start=None,
        key_end=random_keys[0],
        direction=key_range.KeyRange.ASC,
        include_start=False,
        include_end=False,
        namespace=namespace))

    for i in range(0, len(random_keys) - 1):
      key_ranges.append(key_range.KeyRange(
          key_start=random_keys[i],
          key_end=random_keys[i+1],
          direction=key_range.KeyRange.ASC,
          include_start=True,
          include_end=False,
          namespace=namespace))

    key_ranges.append(key_range.KeyRange(
        key_start=random_keys[-1],
        key_end=None,
        direction=key_range.KeyRange.ASC,
        include_start=True,
        include_end=False,
        namespace=namespace))

    return key_ranges

  @classmethod
  def _split_input_from_params(cls, app, namespaces, entity_kind_name,
                               params, shard_count):
    """Return input reader objects. Helper for split_input."""
    key_ranges = []  # KeyRanges for all namespaces
    for namespace in namespaces:
      key_ranges.extend(
          cls._split_input_from_namespace(app,
                                          namespace,
                                          entity_kind_name,
                                          shard_count))

    # Divide the KeyRanges into shard_count shards. The KeyRanges for different
    # namespaces might be very different in size so the assignment of KeyRanges
    # to shards is done round-robin.
    shared_ranges = [[] for _ in range(shard_count)]
    for i, k_range in enumerate(key_ranges):
      shared_ranges[i % shard_count].append(k_range)
    batch_size = int(params.get(cls.BATCH_SIZE_PARAM, cls._BATCH_SIZE))
    return [cls(entity_kind_name, ranges, batch_size)
            for ranges in shared_ranges if ranges]

  @classmethod
  def validate(cls, mapper_spec):
    """Validates mapper spec and all mapper parameters.

    Args:
      mapper_spec: The MapperSpec for this InputReader.

    Raises:
      BadReaderParamsError: required parameters are missing or invalid.
    """
    cls._common_validate(mapper_spec)
    params = mapper_spec.params
    keys_only = util.parse_bool(params.get(cls.KEYS_ONLY_PARAM, False))
    if keys_only:
      raise BadReaderParamsError("The keys_only parameter is obsolete. "
                                 "Use DatastoreKeyInputReader instead.")

    entity_kind_name = params[cls.ENTITY_KIND_PARAM]
    # Fail fast if Model cannot be located.
    try:
      util.for_name(entity_kind_name)
    except ImportError, e:
      raise BadReaderParamsError("Bad entity kind: %s" % e)

  @classmethod
  def _common_validate(cls, mapper_spec):
    """Validates mapper spec and all mapper parameters.

    Common portion of validate method shared between DatastoreInputReader,
    DatastoreKeyInputReader, and DatastoreEntityInputReader.

    Args:
      cls: The class argument from the calling class method.
      mapper_spec: The MapperSpec for this InputReader.

    Raises:
      BadReaderParamsError: required parameters are missing or invalid.
    """
    if mapper_spec.input_reader_class() != cls:
      raise BadReaderParamsError("Input reader class mismatch")
    params = mapper_spec.params
    if cls.ENTITY_KIND_PARAM not in params:
      raise BadReaderParamsError("Missing mapper parameter 'entity_kind'")
    if cls.BATCH_SIZE_PARAM in params:
      try:
        batch_size = int(params[cls.BATCH_SIZE_PARAM])
        if batch_size < 1:
          raise BadReaderParamsError("Bad batch size: %s" % batch_size)
      except ValueError, e:
        raise BadReaderParamsError("Bad batch size: %s" % e)
    if cls.NAMESPACES_PARAM in params:
      if isinstance(params[cls.NAMESPACES_PARAM], (str, unicode)):
        pass
      elif isinstance(params[cls.NAMESPACES_PARAM], list):
        for namespace in params[cls.NAMESPACES_PARAM]:
          if not isinstance(namespace, (str, unicode)):
            raise BadReaderParamsError(
                "Bad namespace list: expected a list of strings")
      else:
        raise BadReaderParamsError(
            "Bad namespace list: expected a list of strings")

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
        May have 'namespaces' in the params as either a list of namespace
        strings or a comma-seperated list of namespaces. If specified then the
        input reader will only yield entities in the given namespaces. If
        'namespaces' is not given then the current namespace will be used. May
        also have 'batch_size' in the params to specify the number of entities
        to process in each batch.

    Returns:
      A list of InputReader objects of length <= number_of_shards. These
      may be DatastoreInputReader or DatastoreKeyInputReader objects.
    """
    params = mapper_spec.params
    entity_kind_name = params[cls.ENTITY_KIND_PARAM]
    shard_count = mapper_spec.shard_count
    namespaces = params.get(cls.NAMESPACES_PARAM,
                            [namespace_manager.get_namespace()])
    if isinstance(namespaces, (str, unicode)):
      namespaces = namespaces.split(",")
    app = params.get(cls._APP_PARAM)

    return cls._split_input_from_params(
        app, namespaces, entity_kind_name, params, shard_count)

  def to_json(self):
    """Serializes all the data in this query range into json form.

    Returns:
      all the data in json-compatible map.
    """
    json_dict = {self.KEY_RANGE_PARAM: [k.to_json() for k in self._key_ranges],
                 self.ENTITY_KIND_PARAM: self._entity_kind,
                 self.BATCH_SIZE_PARAM: self._batch_size}
    return json_dict

  def __str__(self):
    """Returns the string representation of this DatastoreInputReader."""
    return repr(self._key_ranges)

  @classmethod
  def from_json(cls, json):
    """Create new DatastoreInputReader from the json, encoded by to_json.

    Args:
      json: json map representation of DatastoreInputReader.

    Returns:
      an instance of DatastoreInputReader with all data deserialized from json.
    """
    query_range = cls(
        json[cls.ENTITY_KIND_PARAM],
        [key_range.KeyRange.from_json(k) for k in json[cls.KEY_RANGE_PARAM]],
        json[cls.BATCH_SIZE_PARAM])
    return query_range


class DatastoreKeyInputReader(DatastoreInputReader):
  """An input reader which takes a Kind and yields Keys for that kind."""

  def __iter__(self):
    """Create a generator for keys in the range.

    Iterating through entries moves query range past the consumed entries.

    Yields:
      next entry.
    """
    raw_entity_kind = util.get_short_name(self._entity_kind)
    while True:
      if self._current_key_range is None:
        break

      while True:
        query = self._current_key_range.make_ascending_datastore_query(
            raw_entity_kind, keys_only=True)
        results = query.Get(limit=self._batch_size)

        if not results:
          self._advance_key_range()
          break

        for key in results:
          self._current_key_range.advance(key)
          yield key

  @classmethod
  def validate(cls, mapper_spec):
    """Validates mapper spec and all mapper parameters.

    Args:
      mapper_spec: The MapperSpec for this InputReader.

    Raises:
      BadReaderParamsError: required parameters are missing or invalid.
    """
    cls._common_validate(mapper_spec)


class DatastoreEntityInputReader(DatastoreInputReader):
  """An input reader which yields low level datastore entities for a kind."""

  def __iter__(self):
    """Create a generator for low level entities in the range.

    Iterating through entries moves query range past the consumed entries.

    Yields:
      next entry.
    """
    raw_entity_kind = util.get_short_name(self._entity_kind)
    while True:
      if self._current_key_range is None:
        break

      while True:
        query = self._current_key_range.make_ascending_datastore_query(
            raw_entity_kind)
        results = query.Get(limit=self._batch_size)

        if not results:
          self._advance_key_range()
          break

        for entity in results:
          self._current_key_range.advance(entity.key())
          yield entity

  @classmethod
  def validate(cls, mapper_spec):
    """Validates mapper spec and all mapper parameters.

    Args:
      mapper_spec: The MapperSpec for this InputReader.

    Raises:
      BadReaderParamsError: required parameters are missing or invalid.
    """
    cls._common_validate(mapper_spec)


class BlobstoreLineInputReader(InputReader):
  """Input reader for a newline delimited blob in Blobstore."""

  # TODO(user): Should we set this based on MAX_BLOB_FETCH_SIZE?
  _BLOB_BUFFER_SIZE = 64000

  # Maximum number of shards to allow.
  _MAX_SHARD_COUNT = 256

  # Maximum number of blobs to allow.
  _MAX_BLOB_KEYS_COUNT = 246

  # Mapreduce parameters.
  BLOB_KEYS_PARAM = "blob_keys"

  # Serialization parmaeters.
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
    """Returns the next input from as an (offset, line) tuple."""
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
    """Instantiates an instance of this InputReader for the given shard spec."""
    return cls(json[cls.BLOB_KEY_PARAM],
               json[cls.INITIAL_POSITION_PARAM],
               json[cls.END_POSITION_PARAM])

  @classmethod
  def validate(cls, mapper_spec):
    """Validates mapper spec and all mapper parameters.

    Args:
      mapper_spec: The MapperSpec for this InputReader.

    Raises:
      BadReaderParamsError: required parameters are missing or invalid.
    """
    if mapper_spec.input_reader_class() != cls:
      raise BadReaderParamsError("Mapper input reader class mismatch")
    params = mapper_spec.params
    if cls.BLOB_KEYS_PARAM not in params:
      raise BadReaderParamsError("Must specify 'blob_keys' for mapper input")
    blob_keys = params[cls.BLOB_KEYS_PARAM]
    if isinstance(blob_keys, basestring):
      # This is a mechanism to allow multiple blob keys (which do not contain
      # commas) in a single string. It may go away.
      blob_keys = blob_keys.split(",")
    if len(blob_keys) > cls._MAX_BLOB_KEYS_COUNT:
      raise BadReaderParamsError("Too many 'blob_keys' for mapper input")
    if not blob_keys:
      raise BadReaderParamsError("No 'blob_keys' specified for mapper input")
    for blob_key in blob_keys:
      blob_info = blobstore.BlobInfo.get(blobstore.BlobKey(blob_key))
      if not blob_info:
        raise BadReaderParamsError("Could not find blobinfo for key %s" %
                                   blob_key)

  @classmethod
  def split_input(cls, mapper_spec):
    """Returns a list of shard_count input_spec_shards for input_spec.

    Args:
      mapper_spec: The mapper specification to split from. Must contain
          'blob_keys' parameter with one or more blob keys.

    Returns:
      A list of BlobstoreInputReaders corresponding to the specified shards.
    """
    params = mapper_spec.params
    blob_keys = params[cls.BLOB_KEYS_PARAM]
    if isinstance(blob_keys, basestring):
      # This is a mechanism to allow multiple blob keys (which do not contain
      # commas) in a single string. It may go away.
      blob_keys = blob_keys.split(",")

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
      json: The InputReader state as a dict-like object.

    Returns:
      An instance of the InputReader configured using the values of json.
    """
    return cls(json[cls.BLOB_KEY_PARAM],
               json[cls.START_INDEX_PARAM],
               json[cls.END_INDEX_PARAM])

  def to_json(self):
    """Returns an input shard state for the remaining inputs.

    Returns:
      A json-izable version of the remaining InputReader.
    """
    return {self.BLOB_KEY_PARAM: self._blob_key,
            self.START_INDEX_PARAM: self._start_index,
            self.END_INDEX_PARAM: self._end_index}

  def __str__(self):
    """Returns the string representation of this BlobstoreZipInputReader."""
    return "blobstore.BlobKey(%r):[%d, %d]" % (
        self._blob_key, self._start_index, self._end_index)

  @classmethod
  def validate(cls, mapper_spec):
    """Validates mapper spec and all mapper parameters.

    Args:
      mapper_spec: The MapperSpec for this InputReader.

    Raises:
      BadReaderParamsError: required parameters are missing or invalid.
    """
    if mapper_spec.input_reader_class() != cls:
      raise BadReaderParamsError("Mapper input reader class mismatch")
    params = mapper_spec.params
    if cls.BLOB_KEY_PARAM not in params:
      raise BadReaderParamsError("Must specify 'blob_key' for mapper input")
    blob_key = params[cls.BLOB_KEY_PARAM]
    blob_info = blobstore.BlobInfo.get(blobstore.BlobKey(blob_key))
    if not blob_info:
      raise BadReaderParamsError("Could not find blobinfo for key %s" %
                                 blob_key)


  @classmethod
  def split_input(cls, mapper_spec, _reader=blobstore.BlobReader):
    """Returns a list of input shard states for the input spec.

    Args:
      mapper_spec: The MapperSpec for this InputReader. Must contain
          'blob_key' parameter with one blob key.
      _reader: a callable that returns a file-like object for reading blobs.
          Used for dependency injection.

    Returns:
      A list of InputReaders spanning files within the zip.
    """
    params = mapper_spec.params
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
    for i, fileinfo in enumerate(files):
      current_shard_size += fileinfo.file_size
      if current_shard_size >= size_per_shard:
        shard_start_indexes.append(i + 1)
        current_shard_size = 0

    if shard_start_indexes[-1] != len(files):
      shard_start_indexes.append(len(files))

    return [cls(blob_key, start_index, end_index, _reader)
            for start_index, end_index
            in zip(shard_start_indexes, shard_start_indexes[1:])]


class BlobstoreZipLineInputReader(InputReader):
  """Input reader for newline delimited files in zip archives from  Blobstore.

  This has the same external interface as the BlobstoreLineInputReader, in that
  it takes a list of blobs as its input and yields lines to the reader.
  However the blobs themselves are expected to be zip archives of line delimited
  files instead of the files themselves.

  This is useful as many line delimited files gain greatly from compression.
  """

  # Maximum number of shards to allow.
  _MAX_SHARD_COUNT = 256

  # Maximum number of blobs to allow.
  _MAX_BLOB_KEYS_COUNT = 246

  # Mapreduce parameters.
  BLOB_KEYS_PARAM = "blob_keys"

  # Serialization parameters.
  BLOB_KEY_PARAM = "blob_key"
  START_FILE_INDEX_PARAM = "start_file_index"
  END_FILE_INDEX_PARAM = "end_file_index"
  OFFSET_PARAM = "offset"

  def __init__(self, blob_key, start_file_index, end_file_index, offset,
               _reader=blobstore.BlobReader):
    """Initializes this instance with the given blob key and file range.

    This BlobstoreZipLineInputReader will read from the file with index
    start_file_index up to but not including the file with index end_file_index.
    It will return lines starting at offset within file[start_file_index]

    Args:
      blob_key: the BlobKey that this input reader is processing.
      start_file_index: the index of the first file to read within the zip.
      end_file_index: the index of the first file that will not be read.
      offset: the byte offset within blob_key.zip[start_file_index] to start
        reading. The reader will continue to the end of the file.
      _reader: a callable that returns a file-like object for reading blobs.
          Used for dependency injection.
    """
    self._blob_key = blob_key
    self._start_file_index = start_file_index
    self._end_file_index = end_file_index
    self._initial_offset = offset
    self._reader = _reader
    self._zip = None
    self._entries = None
    self._filestream = None

  @classmethod
  def validate(cls, mapper_spec):
    """Validates mapper spec and all mapper parameters.

    Args:
      mapper_spec: The MapperSpec for this InputReader.

    Raises:
      BadReaderParamsError: required parameters are missing or invalid.
    """
    if mapper_spec.input_reader_class() != cls:
      raise BadReaderParamsError("Mapper input reader class mismatch")
    params = mapper_spec.params
    if cls.BLOB_KEYS_PARAM not in params:
      raise BadReaderParamsError("Must specify 'blob_key' for mapper input")

    blob_keys = params[cls.BLOB_KEYS_PARAM]
    if isinstance(blob_keys, basestring):
      # This is a mechanism to allow multiple blob keys (which do not contain
      # commas) in a single string. It may go away.
      blob_keys = blob_keys.split(",")
    if len(blob_keys) > cls._MAX_BLOB_KEYS_COUNT:
      raise BadReaderParamsError("Too many 'blob_keys' for mapper input")
    if not blob_keys:
      raise BadReaderParamsError("No 'blob_keys' specified for mapper input")
    for blob_key in blob_keys:
      blob_info = blobstore.BlobInfo.get(blobstore.BlobKey(blob_key))
      if not blob_info:
        raise BadReaderParamsError("Could not find blobinfo for key %s" %
                                   blob_key)

  @classmethod
  def split_input(cls, mapper_spec, _reader=blobstore.BlobReader):
    """Returns a list of input readers for the input spec.

    Args:
      mapper_spec: The MapperSpec for this InputReader. Must contain
          'blob_keys' parameter with one or more blob keys.
      _reader: a callable that returns a file-like object for reading blobs.
          Used for dependency injection.

    Returns:
      A list of InputReaders spanning the subfiles within the blobs.
      There will be at least one reader per blob, but it will otherwise
      attempt to keep the expanded size even.
    """
    params = mapper_spec.params
    blob_keys = params[cls.BLOB_KEYS_PARAM]
    if isinstance(blob_keys, basestring):
      # This is a mechanism to allow multiple blob keys (which do not contain
      # commas) in a single string. It may go away.
      blob_keys = blob_keys.split(",")

    blob_files = {}
    total_size = 0
    for blob_key in blob_keys:
      zip_input = zipfile.ZipFile(_reader(blob_key))
      blob_files[blob_key] = zip_input.infolist()
      total_size += sum(x.file_size for x in blob_files[blob_key])

    shard_count = min(cls._MAX_SHARD_COUNT, mapper_spec.shard_count)

    # We can break on both blob key and file-within-zip boundaries.
    # A shard will span at minimum a single blob key, but may only
    # handle a few files within a blob.

    size_per_shard = total_size // shard_count

    readers = []
    for blob_key in blob_keys:
      files = blob_files[blob_key]
      current_shard_size = 0
      start_file_index = 0
      next_file_index = 0
      for fileinfo in files:
        next_file_index += 1
        current_shard_size += fileinfo.file_size
        if current_shard_size >= size_per_shard:
          readers.append(cls(blob_key, start_file_index, next_file_index, 0,
                             _reader))
          current_shard_size = 0
          start_file_index = next_file_index
      if current_shard_size != 0:
        readers.append(cls(blob_key, start_file_index, next_file_index, 0,
                           _reader))

    return readers

  def next(self):
    """Returns the next line from this input reader as (lineinfo, line) tuple.

    Returns:
      The next input from this input reader, in the form of a 2-tuple.
      The first element of the tuple describes the source, it is itself
        a tuple (blobkey, filenumber, byteoffset).
      The second element of the tuple is the line found at that offset.
    """
    if not self._filestream:
      if not self._zip:
        self._zip = zipfile.ZipFile(self._reader(self._blob_key))
        # Get a list of entries, reversed so we can pop entries off in order
        self._entries = self._zip.infolist()[self._start_file_index:
                                             self._end_file_index]
        self._entries.reverse()
      if not self._entries:
        raise StopIteration()
      entry = self._entries.pop()
      value = self._zip.read(entry.filename)
      self._filestream = StringIO.StringIO(value)
      if self._initial_offset:
        self._filestream.seek(self._initial_offset)
        self._filestream.readline()

    start_position = self._filestream.tell()
    line = self._filestream.readline()

    if not line:
      # Done with this file in the zip. Move on to the next file.
      self._filestream.close()
      self._filestream = None
      self._start_file_index += 1
      self._initial_offset = 0
      return self.next()

    return ((self._blob_key, self._start_file_index, start_position),
            line.rstrip("\n"))

  def _next_offset(self):
    """Return the offset of the next line to read."""
    if self._filestream:
      offset = self._filestream.tell()
      if offset:
        offset -= 1
    else:
      offset = self._initial_offset

    return offset

  def to_json(self):
    """Returns an input shard state for the remaining inputs.

    Returns:
      A json-izable version of the remaining InputReader.
    """

    return {self.BLOB_KEY_PARAM: self._blob_key,
            self.START_FILE_INDEX_PARAM: self._start_file_index,
            self.END_FILE_INDEX_PARAM: self._end_file_index,
            self.OFFSET_PARAM: self._next_offset()}

  @classmethod
  def from_json(cls, json, _reader=blobstore.BlobReader):
    """Creates an instance of the InputReader for the given input shard state.

    Args:
      json: The InputReader state as a dict-like object.
      _reader: For dependency injection.

    Returns:
      An instance of the InputReader configured using the values of json.
    """
    return cls(json[cls.BLOB_KEY_PARAM],
               json[cls.START_FILE_INDEX_PARAM],
               json[cls.END_FILE_INDEX_PARAM],
               json[cls.OFFSET_PARAM],
               _reader)

  def __str__(self):
    """Returns the string representation of this reader.

    Returns:
      string blobkey:[start file num, end file num]:current offset.
    """
    return "blobstore.BlobKey(%r):[%d, %d]:%d" % (
        self._blob_key, self._start_file_index, self._end_file_index,
        self._next_offset())


class ConsistentKeyReader(DatastoreKeyInputReader):
  """A key reader which reads consistent data from datastore.

  Datastore might have entities which were written, but not visible through
  queries for some time. Typically these entities can be only read inside
  transaction until they are 'applied'.

  This reader reads all keys even if they are not visible. It might take
  significant time to start yielding some data because it has to apply all
  modifications created before its start.
  """
  START_TIME_US_PARAM = 'start_time_us'
  UNAPPLIED_LOG_FILTER = '__unapplied_log_timestamp_us__ <'
  DUMMY_KIND = 'DUMMY_KIND'
  DUMMY_ID = 106275677020293L

  def __init__(self,
               entity_kind,
               key_range_param,
               batch_size=DatastoreKeyInputReader._BATCH_SIZE,
               start_time_us=None):
    """Constructor.

    Args:
      entity_kind: Kind of entity to read as string.
      key_range_param: Key range to scan through as key_range.KeyRange.
      batch_size: Size of single batch read (number of entities).
      start_time_us: Start time of the reader (as given by time.time()
        function). It will apply all unapplied jobs created before it was
        started.
    """
    DatastoreInputReader.__init__(
        self, entity_kind, key_range_param, batch_size)
    self.start_time_us = start_time_us

  def __iter__(self):
    """Iterates over the keys in the given KeyRanges.

    Yields:
      A db.Key instance for each key in the given key range, starting with
      keys for unapplied jobs.
    """
    while True:  # Iterates over each key range.
      if self._current_key_range is None:
        break

      # TODO(user): Remove this hack once 1.4.0 is live in production.
      if datastore_rpc:
        self._apply_jobs()

      while True:  # Iterates over each key in the current key range.
        # Fetches the next batch of the result keys.
        query = self._current_key_range.make_ascending_datastore_query(
            kind=self._entity_kind, keys_only=True)
        keys = query.Get(limit=self._batch_size)

        # No results, this shard is complete.
        if not keys:
          self._advance_key_range()
          break

        # All good, now we can feed the mapper.
        for key in keys:
          self._current_key_range.advance(key)
          yield key

  def _apply_jobs(self):
    """Apply all jobs in current key range."""
    while True:
      # Creates an unapplied query and fetches unapplied jobs in the result
      # range.
      unapplied_query = self._current_key_range.make_ascending_datastore_query(
          kind=None, keys_only=True)
      unapplied_query[
          ConsistentKeyReader.UNAPPLIED_LOG_FILTER] = self.start_time_us
      unapplied_jobs = unapplied_query.Get(limit=self._batch_size)

      if not unapplied_jobs:
        return

      # There were some unapplied jobs. Roll them forward.
      keys_to_apply = []
      for key in unapplied_jobs:
        # To apply the entity group we need to read something from it.
        # We use dummy kind and id because we don't actually need any data.
        path = key.to_path() + [ConsistentKeyReader.DUMMY_KIND,
                                ConsistentKeyReader.DUMMY_ID]
        keys_to_apply.append(
            db.Key.from_path(_app=key.app(), namespace=key.namespace(), *path))
      db.get(keys_to_apply, config=datastore_rpc.Configuration(
          deadline=10,
          read_policy=datastore_rpc.Configuration.APPLY_ALL_JOBS_CONSISTENCY))


  @classmethod
  def _split_input_from_namespace(cls,
                                  app,
                                  namespace,
                                  entity_kind_name,
                                  shard_count):
    key_ranges = super(ConsistentKeyReader, cls)._split_input_from_namespace(
        app, namespace, entity_kind_name, shard_count)

    # The KeyRanges calculated by the base class may not include keys for
    # entities that have unapplied jobs. So use an open key range for the first
    # and last KeyRanges to ensure that they will be processed.
    if key_ranges:
      key_ranges[0].key_start = None
      key_ranges[0].include_start = False
      key_ranges[-1].key_end = None
      key_ranges[-1].include_end = False
    return key_ranges

  @classmethod
  def _split_input_from_params(cls, app, namespaces, entity_kind_name,
                               params, shard_count):
    readers = super(ConsistentKeyReader, cls)._split_input_from_params(app,
                                                          namespaces,
                                                          entity_kind_name,
                                                          params,
                                                          shard_count)

    # We always produce at least one key range because:
    # a) there might be unapplied entities
    # b) it simplifies mapper code
    if not readers:
      key_ranges = [key_range.KeyRange(namespace=namespace, _app=app)
                    for namespace in namespaces]
      readers = [cls(entity_kind_name, key_ranges)]

    return readers

  @classmethod
  def split_input(cls, mapper_spec):
    """Splits input into key ranges."""
    readers = super(ConsistentKeyReader, cls).split_input(mapper_spec)

    start_time_us = mapper_spec.params.get(
        cls.START_TIME_US_PARAM, long(time.time() * 1e6))
    for reader in readers:
      reader.start_time_us = start_time_us
    return readers

  def to_json(self):
    """Serializes all the data in this reader into json form.

    Returns:
      all the data in json-compatible map.
    """
    json_dict = {self.KEY_RANGE_PARAM: [k.to_json() for k in self._key_ranges],
                 self.ENTITY_KIND_PARAM: self._entity_kind,
                 self.BATCH_SIZE_PARAM: self._batch_size,
                 self.START_TIME_US_PARAM: self.start_time_us}
    return json_dict

  @classmethod
  def from_json(cls, json):
    """Create new ConsistentKeyReader from the json, encoded by to_json.

    Args:
      json: json map representation of ConsistentKeyReader.

    Returns:
      an instance of ConsistentKeyReader with all data deserialized from json.
    """
    query_range = cls(
        json[cls.ENTITY_KIND_PARAM],
        [key_range.KeyRange.from_json(k) for k in json[cls.KEY_RANGE_PARAM]],
        json[cls.BATCH_SIZE_PARAM],
        json[cls.START_TIME_US_PARAM])
    return query_range


# TODO(user): This reader always produces only one shard, because
# namespace entities use the mix of ids/names, and KeyRange-based splitting
# doesn't work satisfactory in this case.
# It's possible to implement specific splitting functionality for the reader
# instead of reusing generic one. Meanwhile 1 shard is enough for our
# applications.
class NamespaceInputReader(DatastoreKeyInputReader):
  """An input reader to iterate over namespaces.

  This reader yields namespace names as string.
  It will always produce only one shard.
  """

  @classmethod
  def validate(cls, mapper_spec):
    """Validates mapper spec.

    Args:
      mapper_spec: The MapperSpec for this InputReader.

    Raises:
      BadReaderParamsError: required parameters are missing or invalid.
    """
    mapper_spec.params[cls.ENTITY_KIND_PARAM] = metadata.Namespace.kind()
    mapper_spec.shard_count = 1
    cls._common_validate(mapper_spec)

  @classmethod
  def split_input(cls, mapper_spec):
    """Returns a list of input readers for the input spec.

    Args:
      mapper_spec: The MapperSpec for this InputReader.

    Returns:
      A list of InputReaders.
    """
    mapper_spec.params[cls.ENTITY_KIND_PARAM] = metadata.Namespace.kind()
    mapper_spec.shard_count = 1
    return super(DatastoreKeyInputReader, cls).split_input(mapper_spec)

  def __iter__(self):
    for key in DatastoreKeyInputReader.__iter__(self):
      yield metadata.Namespace.key_to_namespace(key)
