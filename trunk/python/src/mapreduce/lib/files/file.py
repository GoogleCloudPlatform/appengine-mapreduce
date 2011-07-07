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




"""App Engine Files API."""

from __future__ import with_statement


__all__ = [
           'ApiTemporaryUnavailableError',
           'Error',
           'ExclusiveLockFailedError',
           'ExistenceError',
           'FileNotOpenedError',
           'FileTemporaryUnavailableError',
           'FinalizationError',
           'InvalidArgumentError',
           'InvalidFileNameError',
           'InvalidParameterError',
           'OperationNotSupportedError',
           'PermissionDeniedError',
           'ReadOnlyError',
           'SequenceKeyOutOfOrderError',
           'UnknownError',
           'UnsupportedContentTypeError',
           'UnsupportedOpenModeError',
           'WrongContentTypeError' ,
           'WrongOpenModeError',

           'RAW',

           'delete',
           'finalize',
           'open',

           'BufferedFile',
           ]

import logging
import gc
import os

from google.appengine.api import apiproxy_stub_map
from mapreduce.lib.files import file_service_pb
from google.appengine.runtime import apiproxy_errors


class Error(Exception):
  """Base error class for this module."""


class UnsupportedOpenModeError(Error):
  """Unsupported file open mode was specified."""


class UnsupportedContentTypeError(Error):
  """Specified file content type is not supported by this api."""


class InvalidArgumentError(Error):
  """Function argument has invalid value."""


class FinalizationError(Error):
  """File is in wrong finalization state."""


class ExistenceError(Error):
  """File is in wrong existence state."""


class UnknownError(Error):
  """Unknown unexpected io error occured."""


class SequenceKeyOutOfOrderError(Error):
  """Sequence key specified is out of order.

  Attributes:
    last_sequence_key: last sequence key which was written to the file.
  """

  def __init__(self, last_sequence_key):
    Error.__init__(self)
    self.last_sequence_key = last_sequence_key


class InvalidFileNameError(Error):
  """File name is invalid."""


class FileNotOpenedError(Error):
  """File was not opened."""


class ReadOnlyError(Error):
  """File is read-only mode."""


class WrongContentTypeError(Error):
  """File has a different content type."""


class WrongOpenModeError(Error):
  """Incorrect file open mode."""


class OperationNotSupportedError(Error):
  """Incorrect file open mode."""


class PermissionDeniedError(Error):
  """Application doesn't have permissions to perform the operation."""


class ApiTemporaryUnavailableError(Error):
  """Files API is temporary unavailable. Request should be retried soon."""


class FileTemporaryUnavailableError(Error):
  """File is temporary unavailable. Request should be retried soon."""


class InvalidParameterError(Error):
  """Parameter specified in Create() call is invalid."""


class ExclusiveLockFailedError(Error):
  """Exclusive lock can't be obtained."""



RAW = file_service_pb.FileContentType.RAW


def _raise_app_error(e):
  """Convert RPC error into api-specific exception."""
  if (e.application_error ==
      file_service_pb.FileServiceErrors.EXISTENCE_ERROR):
    raise ExistenceError()
  elif (e.application_error ==
        file_service_pb.FileServiceErrors.API_TEMPORARILY_UNAVAILABLE):
    raise ApiTemporaryUnavailableError()
  elif (e.application_error ==
        file_service_pb.FileServiceErrors.FINALIZATION_ERROR):
    raise FinalizationError()
  elif (e.application_error ==
        file_service_pb.FileServiceErrors.IO_ERROR):
    raise UnknownError()
  elif (e.application_error ==
        file_service_pb.FileServiceErrors.SEQUENCE_KEY_OUT_OF_ORDER):
    raise SequenceKeyOutOfOrderError(e.error_detail)
  elif (e.application_error ==
        file_service_pb.FileServiceErrors.INVALID_FILE_NAME):
    raise InvalidFileNameError()
  elif (e.application_error ==
        file_service_pb.FileServiceErrors.FILE_NOT_OPENED):
    raise FileNotOpenedError()
  elif (e.application_error ==
        file_service_pb.FileServiceErrors.READ_ONLY):
    raise ReadOnlyError()
  elif (e.application_error ==
        file_service_pb.FileServiceErrors.WRONG_CONTENT_TYPE):
    raise WrongContentTypeError()
  elif (e.application_error ==
        file_service_pb.FileServiceErrors.WRONG_OPEN_MODE):
    raise WrongOpenModeError()
  elif (e.application_error ==
        file_service_pb.FileServiceErrors.OPERATION_NOT_SUPPORTED):
    raise OperationNotSupportedError()
  elif (e.application_error ==
        file_service_pb.FileServiceErrors.PERMISSION_DENIED):
    raise PermissionDeniedError()
  elif (e.application_error ==
        file_service_pb.FileServiceErrors.FILE_TEMPORARILY_UNAVAILABLE):
    raise FileTemporaryUnavailableError()
  elif (e.application_error ==
        file_service_pb.FileServiceErrors.INVALID_PARAMETER):
    raise InvalidParameterError()
  elif (e.application_error ==
        file_service_pb.FileServiceErrors.EXCLUSIVE_LOCK_FAILED):
    raise ExclusiveLockFailedError()
  raise Error(str(e))


def _create_rpc(deadline):
  """Create RPC object for file service.

  Args:
    deadling: Request deadline in seconds.
  """
  return apiproxy_stub_map.UserRPC('file', deadline)


def _make_call(method, request, response,
               deadline=30):
  """Perform File RPC call.

  Args:
    method: Service method name as string.
    request: Request protocol buffer.
    response: Response protocol buffer.
    deadline: Request deadline in seconds.

  Raises:
    Error or it's descendant if any File API specific error has happened.
  """

  rpc = _create_rpc(deadline=deadline)
  rpc.make_call(method, request, response)
  rpc.wait()
  try:
    rpc.check_success()
  except apiproxy_errors.ApplicationError, e:
    _raise_app_error(e)


class _File(object):
  """File object.

  File object must be obtained by open() function and closed by its close()
  method. It supports scoped closing by with operator.
  """

  def __init__(self, filename, mode, content_type, exclusive_lock):
    """Constructor.

    Args:
      filename: File's name as string.
      content_type: File's content type. Value from FileContentType.ContentType
        enum.
    """
    self._filename = filename
    self._closed = False
    self._content_type = content_type
    self._mode = mode
    self._exclusive_lock = exclusive_lock
    self._offset = 0
    self._open()

  def close(self, finalize=False):
    """Close file.

    Args:
      finalize: Specifies if file should be finalized upon closing.
    """
    if self._closed:
      return
    self._closed = True
    request = file_service_pb.CloseRequest()
    response = file_service_pb.CloseResponse()
    request.set_filename(self._filename)
    request.set_finalize(finalize)
    self._make_rpc_call_with_retry('Close', request, response)

  def __enter__(self):
    return self

  def __exit__(self, atype, value, traceback):
    self.close()

  def write(self, data, sequence_key=None):
    """Write data to file.

    Args:
      data: Data to be written to the file. For RAW files it should be a string
        or byte sequence.
      sequence_key: Sequence key to use for write. Is used for RAW files only.
        File API infrastructure ensures that sequence_key are monotonically
        increasing. If sequence key less than previous one is used, a
        SequenceKeyOutOfOrderError exception with last recorded sequence key
        will be raised. If part of already written content is lost due to
        infrastructure failure, last_sequence_key will point to last
        successfully written key.

    Raises:
      SequenceKeyOutOfOrderError: Raised when passed sequence keys are not
        monotonically increasing.
      InvalidArgumentError: Raised when wrong object type is apssed in as data.
      Error: Error or its descendants are raised when other error has happened.
    """
    if self._content_type == RAW:
      request = file_service_pb.AppendRequest()
      response = file_service_pb.AppendResponse()
      request.set_filename(self._filename)
      request.set_data(data)
      if sequence_key:
        request.set_sequence_key(sequence_key)
      self._make_rpc_call_with_retry('Append', request, response)
    else:
      raise UnsupportedContentTypeError(
          'Unsupported content type: %s' % self._content_type)

  def tell(self):
    """Return file's current position.

    Is valid only when file is opened for read.
    """
    self._verify_read_mode()
    return self._offset

  def seek(self, offset, whence=os.SEEK_SET):
    """Set the file's current position.

    Args:
      offset: seek offset as number.
      whence: seek mode. Supported modes are os.SEEK_SET (absolute seek),
        and os.SEEK_CUR (seek relative to the current position).
    """
    self._verify_read_mode()
    if whence == os.SEEK_SET:
      self._offset = offset
    elif whence == os.SEEK_CUR:
      self._offset += offset
    else:
      raise InvalidArgumentError('Whence mode %d is not supported', whence)

  def read(self, size):
    """Read data from RAW file.

    Args:
      size: Number of bytes to read as integer. Actual number of bytes
        read might be less than specified, but it's never 0 unless current
        offset is at the end of the file.

    Returns:
      A string with data read.
    """
    self._verify_read_mode()
    if self._content_type != RAW:
      raise UnsupportedContentTypeError(
          'Unsupported content type: %s' % self._content_type)

    request = file_service_pb.ReadRequest()
    response = file_service_pb.ReadResponse()
    request.set_filename(self._filename)
    request.set_pos(self._offset)
    request.set_max_bytes(size)
    self._make_rpc_call_with_retry('Read', request, response)
    result = response.data()
    self._offset += len(result)
    return result

  def _verify_read_mode(self):
    if self._mode != 'r':
      raise WrongOpenModeError('File is opened for write.')

  def _open(self):
    request = file_service_pb.OpenRequest()
    response = file_service_pb.OpenResponse()

    request.set_filename(self._filename)
    request.set_exclusive_lock(self._exclusive_lock)
    request.set_content_type(self._content_type)

    if self._mode == 'a':
      request.set_open_mode(file_service_pb.OpenRequest.APPEND)
    elif self._mode == 'r':
      request.set_open_mode(file_service_pb.OpenRequest.READ)
    else:
      raise UnsupportedOpenModeError('Unsupported open mode: %s', self._mode)

    self._make_rpc_call_with_retry('Open', request, response)

  def _make_rpc_call_with_retry(self, method, request, response):
    try:
      _make_call(method, request, response)
    except (ApiTemporaryUnavailableError, FileTemporaryUnavailableError):

      if method == 'Open':
        _make_call(method, request, response)
        return
      if self._exclusive_lock:

        raise

      self._open()
      _make_call(method, request, response)


def open(filename, mode='r', content_type=RAW, exclusive_lock=False):
  """Open a file.

  Args:
    filename: A name of the file as string.
    mode: File open mode. Either 'a' or 'r'.
    content_type: File's content type. Value from FileContentType.ContentType
      enum.
    exclusive_lock: If file should be exclusively locked. All other exclusive
      lock attempts will file untile file is correctly closed.

  Returns:
    File object.
  """
  if not filename:
    raise InvalidArgumentError('Filename is empty')
  if not isinstance(filename, basestring):
    raise InvalidArgumentError('Filename should be a string but is %s (%s)' %
                               (filename.__class__, filename))
  if content_type != RAW:
    raise InvalidArgumentError('Invalid content type')

  f = _File(filename,
            mode=mode,
            content_type=content_type,
            exclusive_lock=exclusive_lock)
  return f


def finalize(filename, content_type=RAW):
  """Finalize a file.

  Args:
    filename: File name as string.
    content_type: File's content type. Value from FileContentType.ContentType
      enum.
  """
  if not filename:
    raise InvalidArgumentError('Filename is empty')
  if not isinstance(filename, basestring):
    raise InvalidArgumentError('Filename should be a string')
  if content_type != RAW:
    raise InvalidArgumentError('Invalid content type')

  try:
    f = open(filename, 'a', exclusive_lock=True, content_type=content_type)
    f.close(finalize=True)
  except FinalizationError:

    pass


def _create(filesystem, content_type=RAW, filename=None, params=None):
  """Create a file.

  Args:
    filesystem: File system to create a file at as string.
    content_type: File content type.
    filename: Requested file name as string. Some file system require this
      to be filled in, some require it to be None.
    params: {string: string} dict of file parameters. Each filesystem
      interprets them differently.
  """
  if not filesystem:
    raise InvalidArgumentError('Filesystem is empty')
  if not isinstance(filesystem, basestring):
    raise InvalidArgumentError('Filesystem should be a string')
  if content_type != RAW:
    raise InvalidArgumentError('Invalid content type')

  request = file_service_pb.CreateRequest()
  response = file_service_pb.CreateResponse()

  request.set_filesystem(filesystem)
  request.set_content_type(content_type)

  if filename:
    if not isinstance(filename, basestring):
      raise InvalidArgumentError('Filename should be a string')
    request.set_filename(filename)

  if params:
    if not isinstance(params, dict):
      raise InvalidArgumentError('Parameters should be a dictionary')
    for k,v in params.items():
      param = request.add_parameters()
      param.set_name(k)
      param.set_value(v)

  _make_call('Create', request, response)
  return response.filename()


def delete(filename):
  """Permanently delete a file.

  Args:
    filename: finalized file name as string.
  """
  from mapreduce.lib.files import blobstore as files_blobstore

  if not isinstance(filename, basestring):
    raise InvalidArgumentError('Filename should be a string, but is %s(%r)' %
                               (filename.__class__.__name__, filename))
  if filename.startswith(files_blobstore._BLOBSTORE_DIRECTORY):
    files_blobstore._delete(filename)
  else:
    raise InvalidFileNameError( 'Unsupported file name: %s' % filename)


class BufferedFile(object):
  """BufferedFile is a file-like object reading underlying file in chunks."""

  _BUFFER_SIZE = 512 * 1024

  def __init__(self, filename, buffer_size=_BUFFER_SIZE):
    """Constructor.

    Args:
      filename: the name of the file to read as string.
      buffer_size: buffer read size to use as int.
    """
    self._filename = filename
    self._position = 0
    self._buffer = ''
    self._buffer_pos = 0
    self._buffer_size = buffer_size

  def tell(self):
    """Return file's current position."""
    return self._position

  def read(self, size):
    """Read data from RAW file.

    Args:
      size: Number of bytes to read as integer. Actual number of bytes
        read is always equal to size unless end if file was reached.

    Returns:
      A string with data read.
    """
    while len(self._buffer) - self._buffer_pos < size:
      self._buffer = self._buffer[self._buffer_pos:]
      self._buffer_pos = 0
      with open(self._filename, 'r') as f:
        f.seek(self._position + len(self._buffer))
        data = f.read(self._buffer_size)
        if not data:
          break
        self._buffer += data
      gc.collect()

    if len(self._buffer) - self._buffer_pos < size:
      result = self._buffer[self._buffer_pos:]
      self._buffer = ''
      self._buffer_pos = 0
      self._position += len(result)
      return result
    else:
      result = self._buffer[self._buffer_pos:self._buffer_pos + size]
      self._buffer_pos += size
      self._position += size
      return result

  def seek(self, offset, whence=os.SEEK_SET):
    """Set the file's current position.

    Args:
      offset: seek offset as number.
      whence: seek mode. Supported modes are os.SEEK_SET (absolute seek),
        and os.SEEK_CUR (seek relative to the current position).
    """
    if whence == os.SEEK_SET:
      self._position = offset
      self._buffer = ''
      self._buffer_pos = 0
    elif whence == os.SEEK_CUR:
      self._position += offset
      self._buffer = ''
      self._buffer_pos = 0
    else:
      raise InvalidArgumentError('Whence mode %d is not supported', whence)
