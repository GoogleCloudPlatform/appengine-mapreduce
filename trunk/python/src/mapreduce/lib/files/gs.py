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




"""Bigstore-specific Files API calls."""

from __future__ import with_statement


__all__ = ['create']

from mapreduce.lib.files import file as files



_GS_FILESYSTEM = 'gs'
_GS_PREFIX = '/gs/'
_MIME_TYPE_PARAMETER = 'content_type'
_CANNED_ACL_PARAMETER = 'acl'
_FILENAME_PARAMETER = 'filename'


def create(filename, mime_type='application/octet-stream'):
  """Create a writable blobstore file.

  Args:
    filename: Bigstore object name (/gs/bucket/object)
    mime_type: Blob content MIME type as string.

  Returns:
    A writable file name for bigstore file. This file can be opened for write
    by File API open function. To read the file call file::open with the plain
    Bigstore filename (/gs/bucket/object).
  """
  if not filename:
    raise files.InvalidArgumentError('Empty filename')
  elif not isinstance(filename, basestring):
    raise files.InvalidArgumentError('Expected string for filename', filename)
  elif not filename.startswith(_GS_PREFIX) or filename == _GS_PREFIX:
    raise files.InvalidArgumentError(
        'Google storage files must be of the form /gs/bucket/object', filename)
  elif not mime_type:
    raise files.InvalidArgumentError('Empty mime_type')
  elif not isinstance(mime_type, basestring):
    raise files.InvalidArgumentError('Expected string for mime_type', mime_type)

  params = {_MIME_TYPE_PARAMETER: mime_type,
            _FILENAME_PARAMETER: filename[len(_GS_PREFIX) - 1:]}
  return files._create(_GS_FILESYSTEM, params=params)
