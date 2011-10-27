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




"""Blobstore-specific Files API calls."""

from __future__ import with_statement


__all__ = ['create', 'get_blob_key', 'get_file_name']

import urllib

from mapreduce.lib.files import file as files
from google.appengine.api import datastore
from google.appengine.ext import blobstore



_BLOBSTORE_FILESYSTEM = 'blobstore'
_BLOBSTORE_DIRECTORY = '/' + _BLOBSTORE_FILESYSTEM + '/'
_BLOBSTORE_NEW_FILE_NAME = 'new'
_CREATION_HANDLE_PREFIX = 'writable:'
_MIME_TYPE_PARAMETER = 'content_type'
_BLOBINFO_UPLOADED_FILENAME_PARAMETER = 'file_name'


def create(mime_type='application/octet-stream',
           _blobinfo_uploaded_filename=None):
  """Create a writable blobstore file.

  Args:
    mime_type: Resulting blob content MIME type as string.
    _blobinfo_uploaded_filename: Resulting blob's BlobInfo file name as string.

  Returns:
    A file name for blobstore file. This file can be opened for write
    by File API open function. To read the file or obtain its blob key, finalize
    it and call get_blob_key function.
  """
  if not mime_type:
    raise files.InvalidArgumentError('Empty mime_type')
  if not isinstance(mime_type, basestring):
    raise files.InvalidArgumentError('Expected string for mime_type')

  params = {_MIME_TYPE_PARAMETER: mime_type}
  if _blobinfo_uploaded_filename:
    if not isinstance(_blobinfo_uploaded_filename, basestring):
      raise files.InvalidArgumentError(
          'Expected string for _blobinfo_uploaded_filename')
    params[_BLOBINFO_UPLOADED_FILENAME_PARAMETER] = _blobinfo_uploaded_filename
  return files._create(_BLOBSTORE_FILESYSTEM, params=params)



_BLOB_FILE_INDEX_KIND = '__BlobFileIndex__'



_BLOB_KEY_PROPERTY_NAME = 'blob_key'


def get_blob_key(create_file_name):
  """Get a blob key for finalized blobstore file.

  Args:
    create_file_name: Writable blobstore filename as obtained from create()
    function. The file should be finalized.

  Returns:
    An instance of apphosting.ext.blobstore.BlobKey for corresponding blob
    or None if the blob referred to by the file name is not finalized.

  Raises:
    mapreduce.lib.files.InvalidFileNameError if the file name is not
    a valid nonfinalized blob file name.
  """
  if not create_file_name:
    raise files.InvalidArgumentError('Empty file name')
  if not isinstance(create_file_name, basestring):
    raise files.InvalidArgumentError('Expected string for file name')
  if not create_file_name.startswith(_BLOBSTORE_DIRECTORY):
    raise files.InvalidFileNameError(
        'Filename %s passed to get_blob_key doesn\'t have prefix %s' %
        (create_file_name, _BLOBSTORE_DIRECTORY))
  ticket = create_file_name[len(_BLOBSTORE_DIRECTORY):]

  if not ticket.startswith(_CREATION_HANDLE_PREFIX):

    return blobstore.BlobKey(ticket)


  blob_file_index = datastore.Get([datastore.Key.from_path(
      _BLOB_FILE_INDEX_KIND,
      ticket)])[0]
  if blob_file_index:
    blob_key_str = blob_file_index[_BLOB_KEY_PROPERTY_NAME]







    results = datastore.Get([datastore.Key.from_path(
        blobstore.BLOB_INFO_KIND, blob_key_str)])
    if results[0] is None:
      return None
  else:




    query = datastore.Query(blobstore.BLOB_INFO_KIND,
                            {'creation_handle =': ticket},
                            keys_only=True,
                            namespace='')
    results = query.Get(1)
    if not results:
      return None
    blob_key_str = results[0].name()
  return blobstore.BlobKey(blob_key_str)


def get_file_name(blob_key):
  """Get a filename to read from the blob.

  Args:
    blob_key: An instance of BlobKey.

  Returns:
    File name as string which can be used with File API to read the file.
  """
  if not blob_key:
    raise files.InvalidArgumentError('Empty blob key')
  if not isinstance(blob_key, (blobstore.BlobKey, basestring)):
    raise files.InvalidArgumentError('Expected string or blobstore.BlobKey')
  return '%s%s' % (_BLOBSTORE_DIRECTORY, blob_key)


def _delete(filename):
  """Permanently delete a file.

  Args:
    filename: finalized file name as string.
  """

  blob_key = get_blob_key(filename)
  if blob_key is None:
    return
  blob_info = blobstore.BlobInfo.get(blob_key)
  if blob_info is None:
    return
  blob_info.delete()
