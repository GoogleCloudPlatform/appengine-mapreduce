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




"""Google Storage specific Files API calls."""






from __future__ import with_statement


__all__ = ['create']

import re
from urllib import urlencode
from xml.dom import minidom

from google.appengine.api import app_identity
from google.appengine.api import urlfetch
from mapreduce.lib.files import file as files




_GS_FILESYSTEM = files.GS_FILESYSTEM
_GS_PREFIX = '/' + _GS_FILESYSTEM + '/'
_MIME_TYPE_PARAMETER = 'content_type'
_CANNED_ACL_PARAMETER = 'acl'
_CONTENT_ENCODING_PARAMETER = 'content_encoding'
_CONTENT_DISPOSITION_PARAMETER = 'content_disposition'
_CACHE_CONTROL_PARAMETER = 'cache_control'
_USER_METADATA_PREFIX = 'x-goog-meta-'



_GS_RESTFUL_URL = 'commondatastorage.googleapis.com'
_GS_RESTFUL_SCOPE_READ_ONLY = (
    'https://www.googleapis.com/auth/devstorage.read_only')
_GS_RESTFUL_API_VERSION = '2'
_GS_BUCKETPATH_REGEX = re.compile(r'/gs/[a-z0-9\.\-]{3,}$')


def listdir(path, kwargs=None):
  """Return a sorted list of filenames (matching a pattern) in the given path.

  Sorting (decrease by string) is done automatically by Google Cloud Storage.

  Args:
    path: a Google Cloud Storage path of "/gs/bucketname" form.
    kwargs: other keyword arguments to be relayed to Google Cloud Storage.
      This can be used to select certain files with names matching a pattern.

      Supported keywords:
      marker: a string after which (exclusive) to start listing.
      max-keys: the maximum number of filenames to return.
      prefix: limits the returned filenames to those with this prefix. no regex.

      See Google Cloud Storage documentation for more details and examples.
      https://developers.google.com/storage/docs/reference-methods#getbucket

  Returns:
    a sorted list containing filenames (matching a pattern) from
    the given path. The last filename can be used as a marker for another
    request for more files.
  """
  if not path:
    raise files.InvalidFileNameError('Empty path')
  elif not isinstance(path, basestring):
    raise files.InvalidFileNameError('Expected string for path %s' % path)
  elif not _GS_BUCKETPATH_REGEX.match(path):
    raise files.InvalidFileNameError(
        'Google storage path must have the form /gs/bucketname')

  bucketname = path[len(_GS_PREFIX):]

  request_headers = {
      'Authorization': 'OAuth %s' % app_identity.get_access_token(
                       _GS_RESTFUL_SCOPE_READ_ONLY)[0],
      'x-goog-api-version': _GS_RESTFUL_API_VERSION
      }

  url = 'https://%s/%s' % (_GS_RESTFUL_URL, bucketname)

  if kwargs:
    url += '/?' + urlencode(kwargs)

  response = urlfetch.fetch(url=url,
                            headers=request_headers,
                            deadline=60)

  if response.status_code == 404:
    raise files.InvalidFileNameError('Bucket %s does not exist.' % bucketname)
  elif response.status_code == 401:
    raise files.PermissionDeniedError('Permission denied to read bucket %s.' %
                                      bucketname)

  dom = minidom.parseString(response.content)

  def __textValue(node):
    return node.firstChild.nodeValue


  error = dom.getElementsByTagName('Error')
  if len(error) == 1:
    details = error[0].getElementsByTagName('Details')
    if len(details) == 1:
      raise files.InvalidParameterError(__textValue(details[0]))
    else:
      code = __textValue(error[0].getElementsByTagName('Code')[0])
      msg = __textValue(error[0].getElementsByTagName('Message')[0])
      raise files.InvalidParameterError('%s: %s' % (code, msg))

  return [__textValue(key) for key in dom.getElementsByTagName('Key')]


def create(filename,
           mime_type='application/octet-stream',
           acl=None,
           cache_control=None,
           content_encoding=None,
           content_disposition=None,
           user_metadata=None):
  """Create a writable googlestore file.

  Args:
    filename: Google Storage object name (/gs/bucket/object)
    mime_type: Blob content MIME type as string.
    acl: Canned acl to apply to the object as per:
      http://code.google.com/apis/storage/docs/reference-headers.html#xgoogacl
      If not specified (or set to None), default object acl is used.
    cache_control: Cache control header to set when serving through Google
      storage. If not specified, default of 3600 seconds is used.
    content_encoding: If object is compressed, specify the compression method
      here to set the header correctly when served through Google Storage.
    content_disposition: Header to use when serving through Google Storage.
    user_metadata: Dictionary specifying key value pairs to apply to the
      object. Each key is prefixed with x-goog-meta- when served through
      Google Storage.

  Returns:
    A writable file name for a Google Storage file. This file can be opened for
    write by File API open function. To read the file call file::open with the
    plain Google Storage filename (/gs/bucket/object).
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

  params = {_MIME_TYPE_PARAMETER: mime_type}

  if acl:
    if not isinstance(acl, basestring):
      raise files.InvalidArgumentError('Expected string for acl', acl)
    params[_CANNED_ACL_PARAMETER] = acl

  if content_encoding:
    if not isinstance(content_encoding, basestring):
      raise files.InvalidArgumentError('Expected string for content_encoding')
    else:
      params[_CONTENT_ENCODING_PARAMETER] = content_encoding
  if content_disposition:
    if not isinstance(content_disposition, basestring):
      raise files.InvalidArgumentError(
          'Expected string for content_disposition')
    else:
      params[_CONTENT_DISPOSITION_PARAMETER] = content_disposition
  if cache_control:
    if not isinstance(cache_control, basestring):
      raise files.InvalidArgumentError('Expected string for cache_control')
    else:
      params[_CACHE_CONTROL_PARAMETER] = cache_control
  if user_metadata:
    if not isinstance(user_metadata, dict):
      raise files.InvalidArgumentError('Expected dict for user_metadata')
    for key, value in user_metadata.items():
      if not isinstance(key, basestring):
        raise files.InvalidArgumentError(
            'Expected string for key in user_metadata')
      if not isinstance(value, basestring):
        raise files.InvalidArgumentError(
            'Expected string for value in user_metadata for key: ', key)
      params[_USER_METADATA_PREFIX + key] = value
  return files._create(_GS_FILESYSTEM, filename=filename, params=params)


def default_bucket_name():
  """Obtain the default Google Storage bucket name for this application.

    Returns:
      A string that is the name of the default bucket.
  """
  return files._default_gs_bucket_name()
