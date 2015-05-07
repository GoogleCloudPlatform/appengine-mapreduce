#!/usr/bin/env python
# Copyright 2010 Google Inc. All Rights Reserved.
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

"""Cloudstorage stub."""
import os
import re
import logging

# pylint: disable=g-import-not-at-top
# TODO(user): Cleanup imports if/when cloudstorage becomes part of runtime.
try:
  # Check if the full cloudstorage package exists. The stub part is in runtime.
  cloudstorage = None
  import cloudstorage
  if hasattr(cloudstorage, "_STUB"):
    cloudstorage = None
  # "if" is needed because apphosting/ext/datastore_admin:main_test fails.
  if cloudstorage:
    from cloudstorage import errors as cloud_errors
    from cloudstorage import common
    from cloudstorage.cloudstorage_api import _copy2
except ImportError:
  pass  # CloudStorage library not available
# pylint: disable=too-many-branches, too-many-locals, too-many-statements
def compose(list_of_files, destination_file, preserve_order=True,
            content_type=None, retry_params=None, _account_id=None):
  """
    Internal only!
    Should only be used when the included cloudstorage lib
      does not contain the compose functionality
    Runs the GCS Compose on the inputed files.
    Merges between 2 and 1024 files into one file.
    Automatically breaks down the files into batches of 32.
    There is an option to sort naturally.
  Args:
    list_of_files: list of dictionaries with the following format:
      {"file_name" : REQUIRED name of the file to be merged. Do not include the bucket name,
       "Generation" : OPTIONAL Used to specify what version of a file to use,
       "IfGenerationMatch" : OPTIONAL Used to fail requests if versions don't match}
    destination_file: Path to the desired output file. Must have the bucket in the path.
    preserve_order: If true will not sort the files into natural order.
    content_type: Used to specify the content-header of the output.
                  If None will try to guess based off the first file.
    retry_params: An api_utils.RetryParams for this call to GCS. If None,
    the default one is used.
    _account_id: Internal-use only.

  Raises:
    TypeError: If the dictionary for the file list is malformed
    ValueError: If the number of files is outside the range of 2-1024
    errors.NotFoundError: If any element in the file list is missing the "file_name" key
  """
  def _alphanum_key(input_string):
    """
      Internal use only.
      Splits the file names up to allow natural sorting
    """
    return [ int(char) if char.isdigit() else char for char in re.split('([0-9]+)', input_string) ]
  # pylint: disable=too-many-locals
  def _make_api_call(bucket, file_list, destination_file, content_type, retry_params, _account_id):
    """
        Internal Only
        Makes the actual calls.
        Currently stubbed because the dev server cloudstorage_stub.py
          does not handle compose requests.
        TODO: When the dev server gets patch please remove the stub
    Args:
      bucket: Bucket where the files are kept
      file_list: list of dicts with the file name (see compose argument "list_of_files" for format).
      destination_file: Path to the destination file.
      content_type: Content type for the destination file.
      retry_params: An api_utils.RetryParams for this call to GCS. If None,
      the default one is used.
    _account_id: Internal-use only.
    """
    if len(file_list) == 0:
      raise ValueError("Unable to merge 0 files")
    if len(file_list) == 1:
      _copy2(bucket + file_list[0]["file_name"], destination_file)
      return
    '''
    Needed until cloudstorage_stub.py is updated to accept compose requests
    TODO: When patched remove the True flow from this if.
    '''
    if 'development' in os.environ.get('SERVER_SOFTWARE', '').lower():
      '''
      Below is making the call to the Development server
      '''
      with open(destination_file, "w", content_type=content_type) as gcs_merge:
        for source_file in file_list:
          try:
            with open(bucket + source_file['file_name'], "r") as gcs_source:
              gcs_merge.write(gcs_source.read())
          except cloud_errors.NotFoundError:
            logging.warn("File not found %s, skipping", source_file['file_name'])
    else:
      '''
      Below is making the call to the Production server
      '''
      xml = ""
      for item in file_list:
        generation = item.get("Generation", "")
        generation_match = item.get("IfGenerationMatch", "")
        if generation != "":
          generation = "<Generation>%s</Generation>" % generation
        if generation_match != "":
          generation_match = "<IfGenerationMatch>%s</IfGenerationMatch>" % generation_match
        xml += "<Component><Name>%s</Name>%s%s</Component>" % \
                  (item["file_name"], generation, generation_match)
      xml = "<ComposeRequest>%s</ComposeRequest>" % xml
      logging.info(xml)
      # pylint: disable=protected-access
      api = cloudstorage.storage_api._get_storage_api(retry_params=retry_params,
                                   account_id=_account_id)
      headers = {"Content-Type" : content_type}
      # pylint: disable=no-member
      status, resp_headers, content = api.put_object(
                cloudstorage.api_utils._quote_filename(destination_file) + "?compose",
                                          payload=xml,
                                          headers=headers)
      # TODO: confirm whether [200] is sufficient, or if 204 etc. might be returned?
      cloud_errors.check_status(status, [200], destination_file, resp_headers, body=content)
  '''
    Actual start of the compose call. The above is inside to prevent calls to it directly
  '''
  temp_file_suffix = "____MergeTempFile"
  # Make a copy of the list as they are passed ref
  file_list = list_of_files[:]
  if not isinstance(file_list, list):
    raise TypeError("file_list must be a list of dictionaries")
  list_len = len(file_list)
  if list_len > 1024:
    raise ValueError(
          "Compose attempted to create composite with too many (%i) components; limit is (1024)." \
                      % list_len)
  if list_len <= 1:
    raise ValueError("Compose operation requires at least two components; %i provided." % list_len)

  common.validate_file_path(destination_file)
  bucket = "/" + destination_file.split("/")[1] + "/"
  for source_file in file_list:
    if not isinstance(source_file, dict):
      raise TypeError("Each item of file_list must be dictionary")
    file_name = source_file.get("file_name", None)
    if file_name is None:
      raise cloud_errors.NotFoundError("Each item in file_list must specify a file_name")
    if file_name.startswith(bucket):
      logging.warn("Detected bucket name at the start of the file, " + \
                   "must not specify the bucket when listing file_names." + \
                   " May cause files to be miss read")
    common.validate_file_path(bucket + source_file['file_name'])
  if content_type is None:
    if file_exists(bucket + list_of_files[0]["file_name"]):
      content_type = cloudstorage.stat(bucket + list_of_files[0]["file_name"]).content_type
    else:
      logging.warn("Unable to read first file to divine content type, using text/plain")
      content_type = "text/plain"
  # Sort naturally if the flag is false
  if not preserve_order:
    file_list.sort(key=lambda x: _alphanum_key(x['file_name']))
  '''
  Compose can only handle 32 files at a time. Breaks down the list into batches of 32
  (this will only need to happen once, since the file_list size restriction is 1024 = 32 * 32)
  '''
  temp_list = []  # temporary storage for the filenames that store the merged segments of 32
  if len(file_list) > 32:
    temp_file_counter = 0
    segments_list = [file_list[i:i + 32] for i in range(0, len(file_list), 32)]
    file_list = []
    for segment in segments_list:
      temp_file_name = destination_file + temp_file_suffix + str(temp_file_counter)
      _make_api_call(bucket, segment, temp_file_name, content_type, retry_params, _account_id)
      file_list.append({"file_name" : temp_file_name.replace(bucket, "", 1)})
      temp_file_counter += 1
      temp_list.append(temp_file_name)
  # There will always be 32 or less files to merge at this point
  _make_api_call(bucket, file_list, destination_file, content_type, retry_params, _account_id)
  # grab all temp files that were created during the merging of segments of 32
  temp_list = cloudstorage.listbucket(destination_file + temp_file_suffix)
  # delete all the now-unneeded temporary merge-files for the segments of 32 (if any)
  for item in temp_list:
    try:
      cloudstorage.delete(item.filename)
    except cloud_errors.NotFoundError:
      pass

def _file_exists(destination):
  """Checks if a file exists.
    Tries to do a stat check on the file.
    If it succeeds returns True otherwise false

    Args:
      destination: Full path to the file (ie. /bucket/object) w/ leading slash
    Returns:
      True if the file is accessible otherwsie False
    """
  try:
    cloudstorage.stat(destination)
    return True
  except cloud_errors.NotFoundError:
    return False

