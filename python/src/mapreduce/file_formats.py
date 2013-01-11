#!/usr/bin/env python
# Copyright 2012 Google Inc. All Rights Reserved.
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

"""Define file formats."""



__all__ = ['FileFormat',
           'FORMATS']

import StringIO
import zipfile


class FileFormat(object):
  """FileFormat.

  FileFormat knows how to operate on file of a specific format.
  It should never been instantiated directly.

  Life cycle of FileFormat:
    1. Two ways that FileFormat is created: file_format_root.split creates
       FileFormat from scratch. FileFormatRoot.from_json creates FileFormat
       from serialized json str. Either way, it is associated with a
       FileFormatRoot.
    2. Root acts as a coordinator among FileFormats. Root initializes
       its many fields so that FileFormat knows how to iterate over its inputs.
    3. Its next() method is used to iterate. next() gets input from a
       _FileStream object associated to this FileFormat by root.
    4. It keeps iterating until either root calls its to_json() or root
       sends it a StopIteration.

  How to define a new format:
    1. Subclass this.
    2. Override NAME and ARGUMENTS. They are used during parsing.
       See file_format_parser._FileFormatParser.
    3. Optionally override preprocess_file(), which operates on
       _input_files_stream.current before any next() is called. See method.
    4. Override get_next(). Used by next() to fetch the next content to
       return. See method.
    5. Optionally override can_split() and split() if this format
       supports them. See method.
    6. Write unit tests. Tricky logics (to/from_json, advance
       _input_files_stream) are shared. Thus as long as you respected
       get_next()'s pre/post conditions, tests are very simple.
    7. Register your format at FORMATS.

  Attributes:
    ARGUMENTS: a list of acceptable arguments to this format. Used for parsing
        this format.
    NAME: the name of this format. Used for parsing this format.
  """

  ARGUMENTS = []
  NAME = '_file'
  # Default value for self._index.
  DEFAULT_INDEX_VALUE = None

  # Json Properties.
  _KWARGS = 'kwargs'
  _INDEX = 'index'
  _RANGE = 'input_range'
  _FORMAT = 'name'
  _PREVIOUS_INDEX = 'previous_index'

  def __init__(self, **kwargs):
    for k in kwargs:
      if k not in self.ARGUMENTS:
        raise ValueError('Illegal argument %s' % k)
    self._kwargs = kwargs

    # A dict to save all the transient objects needed during iteration.
    # If an object is expensive to initiate, put it here.
    self._cache = {}

    # These fields are directly set by FileFormatRoot.

    # index of where to read _input_files_stream.current.
    # This is NOT the actually offset into the file.
    # It's interpreted by different _FileFormats differently.
    # A format should override DEFAULT_INDEX_VALUE if it needs _index.
    # See each FileFormat.DEFAULT_INDEX_VALUE for its semantic.
    self._index = self.DEFAULT_INDEX_VALUE
    self._previous_index = self.DEFAULT_INDEX_VALUE
    # A _FilesStream object where _FileFormat should read inputs from.
    self._input_files_stream = None
    # A tuple [start_index, end_index) that if defined, should bound
    # self._index.
    self._range = None

  def __repr__(self):
    return str(self.to_json())

  def __str__(self):
    result = self.NAME
    sorted_keys = self._kwargs.keys()
    sorted_keys.sort()

    if self._kwargs:
      result += (
          '(' +
          ','.join([key + '=' + self._kwargs[key] for key in sorted_keys]) +
          ')')
    return result

  def checkpoint(self):
    self._previous_index = self._index

  def to_json(self):
    return {self._KWARGS: self._kwargs,
            self._INDEX: self._index,
            self._RANGE: self._range,
            self._FORMAT: self.NAME,
            self._PREVIOUS_INDEX: self._previous_index}

  @classmethod
  def from_json(cls, json):
    file_format = cls(**json[cls._KWARGS])
    file_format._index = json[cls._PREVIOUS_INDEX]
    file_format._previous_index = json[cls._PREVIOUS_INDEX]
    file_format._range = json[cls._RANGE]
    return file_format

  @classmethod
  def can_split(cls):
    """Does this format support split.

    Return True if a FileFormat allows its inputs to be splitted into
    different shards. Must implement split method.
    """
    return False

  @classmethod
  # pylint: disable-msg=W0613
  def split(cls, desired_size, start_index, input_file, cache):
    """Split a single chunk of desired_size from file.

    FileFormatRoot uses this method to ask FileFormat how to split
    one file of its format.

    This method takes an opened file and a start_index. If file
    size is bigger than desired_size, the method determines a chunk of the
    file whose size is close to desired_size. The chuck is indicated by
    [start_index, end_index). If the file is smaller than desired_size,
    the chunk will include the rest of the input_file.

    This method also indicates how many bytes are consumed by this chunk
    by returning size_left to the caller.

    Args:
      desired_size: desired number of bytes for this split. Positive int.
      start_index: the index to start this split. The index is not necessarily
        an offset. In zipfile, for example, it's the index of the member file
        in the archive. Non negative int.
      input_file: opened Files API file to split. Do not close this file.
      cache: a dict to cache any object over multiple calls if needed.

    Returns:
      Return a tuple of (size_left, end_index). If end_index equals start_index,
      the file is fully split.
    """
    raise NotImplementedError('split is not implemented for %s.' %
                              cls.__name__)

  def __iter__(self):
    return self

  def preprocess(self, file_object):
    """Does preprocessing on the file-like object and return another one.

    Normally a FileFormat directly reads from the original
    _input_files_stream.current, which is a File-like object containing str.
    But some FileFormat need to decode the entire str before any iteration
    is possible (e.g. lines). This method takes the original
    _input_files_stream.current and returns another File-like object
    that replaces the original one.

    Args:
      file_object: read from this object and process its content.

    Returns:
      a file-like object containing processed contents. If the returned object
      is newly created, close the old one.
    """
    return file_object

  def next(self):
    """Return a file-like object containing next content."""
    try:
      # Limit _index by _range.
      if self.DEFAULT_INDEX_VALUE is not None and self._range:
        if self._index < self._range[0]:
          self._index = self._range[0]
        elif self._index >= self._range[1]:
          raise EOFError()

      self._input_files_stream.checkpoint()
      self.checkpoint()
      result = self.get_next()
      if isinstance(result, str):
        result = StringIO.StringIO(result)
      if isinstance(result, unicode):
        raise ValueError('%s can not return unicode object.' %
                         self.__class__.__name__)
      return result
    except EOFError:
      self._input_files_stream.advance()
      self._index = self.DEFAULT_INDEX_VALUE
      self._cache = {}
      return self.next()

  def get_next(self):
    """Find the next content to return.

    Expected steps of any implementation:
      1. Read input from _input_files_stream.current. It is guaranteed
         to be a file-like object ready to be read from. It returns
         Python str.
      2. If nothing is read, raise EOFError. Otherwise, process the
         contents read in anyway. _kwargs is guaranteed to be a dict
         containing all arguments and values to this format.
      3. If this format needs _index to keep track of where in
         _input_files_stream.current to read next, the format is
         responsible for updating _index correctly in each iteration.
         _index and _input_files_stream.current is guaranteed to be
         correctly (de)serialized. Thus the implementation don't need
         to worry/know about (de)serialization at all.
      4. Return the processed contents either as a file-like object or
         Python str. NO UNICODE.

    Returns:
      The str or file like object if got anything to return.

    Raises:
      EOFError if no content is found to return.
    """
    raise NotImplementedError('%s not implemented.' % self.__class__.__name__)


# Binary formats.
class _BinaryFormat(FileFormat):
  """Base class for any binary formats.

  This class just reads the entire file as raw str. All subclasses
  should simply override NAME. That NAME will be passed to Python
  to decode the bytes.
  """

  NAME = 'bytes'

  def get_next(self):
    result = self._input_files_stream.current.read()
    if not result:
      raise EOFError()
    if self.NAME != _BinaryFormat.NAME:
      return result.decode(self.NAME)
    return result


class _Base64Format(_BinaryFormat):
  """Read entire file as base64 str."""

  NAME = 'base64'


# Archive formats.
class _ZipFormat(FileFormat):
  """Read member files of zipfile."""

  NAME = 'zip'
  # _index specifies the next member file to read.
  DEFAULT_INDEX_VALUE = 0

  def get_next(self):
    if self._cache:
      zip_file = self._cache['zip_file']
      infolist = self._cache['infolist']
    else:
      zip_file = zipfile.ZipFile(self._input_files_stream.current)
      infolist = zip_file.infolist()
      self._cache['zip_file'] = zip_file
      self._cache['infolist'] = infolist

    if self._index == len(infolist):
      raise EOFError()

    result = zip_file.read(infolist[self._index])
    self._index += 1
    return result

  @classmethod
  def can_split(cls):
    return True

  @classmethod
  def split(self, desired_size, start_index, opened_file, cache):
    if 'infolist' in cache:
      infolist = cache['infolist']
    else:
      zip_file = zipfile.ZipFile(opened_file)
      infolist = zip_file.infolist()
      cache['infolist'] = infolist

    index = start_index
    while desired_size > 0 and index < len(infolist):
      desired_size -= infolist[index].file_size
      index += 1
    return desired_size, index


# Text formats.
class _TextFormat(FileFormat):
  """Base class for any text format.

  Text formats are those that require decoding before iteration.
  This class takes care of the preprocessing logic of decoding.
  """

  ARGUMENTS = ['encoding']
  NAME = '_text'

  def preprocess(self, file_object):
    """Decode the entire file to read text."""
    if 'encoding' in self._kwargs:
      content = file_object.read()
      content = content.decode(self._kwargs['encoding'])
      file_object.close()
      return StringIO.StringIO(content)
    return file_object


class _LinesFormat(_TextFormat):
  """Read file line by line."""

  NAME = 'lines'

  def get_next(self):
    result = self._input_files_stream.current.readline()
    if not result:
      raise EOFError()
    if result and 'encoding' in self._kwargs:
      result = result.encode(self._kwargs['encoding'])
    return result


class _CSVFormat(_TextFormat):
  ARGUMENTS = _TextFormat.ARGUMENTS + ['delimiter']
  NAME = 'csv'
  # TODO(user) implement this. csv exists now only to test parser.


FORMATS = {
    # Binary formats.
    'base64': _Base64Format,
    'bytes': _BinaryFormat,
    # Text format.
    'csv': _CSVFormat,
    'lines': _LinesFormat,
    # Archive formats.
    'zip': _ZipFormat}
