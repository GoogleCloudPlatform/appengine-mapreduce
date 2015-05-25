#!/usr/bin/env python
# Copyright 2010 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test for GCS MapReduce."""

import webapp2
import logging
import urllib2
import random
import string
import zipfile
import jinja2
import re
import cloudstorage

from mapreduce import base_handler
from mapreduce import mapreduce_pipeline
from mapreduce import mapper_pipeline

from google.appengine.api import app_identity
from google.appengine.api import mail
from google.appengine.ext import blobstore
from google.appengine.ext.webapp import blobstore_handlers

BUCKET_NAME = app_identity.get_default_gcs_bucket_name()
WORK_FOLDER = '/MR-GCS-Tests'
OUTPUT_FOLDER = 'MR-GCS-Tests/Results/'
FILE_PATH = '/' + BUCKET_NAME + WORK_FOLDER + '/Test.txt'
ZIP_PATH = '/' + BUCKET_NAME + WORK_FOLDER + '/Test.zip'
STRESS_TEST_PATH = '/' + BUCKET_NAME + WORK_FOLDER + '/Stressc.txt'

STRESS_TEST_WORD_SIZE = 1024
STRESS_TEST_CHAR_BASE = list(string.ascii_letters)

def fill_file(list_of_chars):
  """
  Shuffles a list of chars, adding a space between.
  With a 1 in 25 chance of adding a new line

  Args:
    list_of_chars: list of chars to shuffle
  """
  random.shuffle(list_of_chars)
  ret_string = ''
  for item in list_of_chars:
    ret_string += item + ' '
    if random.randrange(0, 25) == 0:
      ret_string += '\n'
  return ret_string


class MainPage(webapp2.RequestHandler):
  """
  Main handler for the Mapreduce test
  """
  def get(self):
    """Get handler, simply loads the page"""
    # pylint: disable=no-member
    self.response.out.write(jinja2.Environment(
                              loader=jinja2.FileSystemLoader('templates'),
                              autoescape=True)
                            .get_template('index.html').render())
  # pylint: disable=too-many-locals, too-many-statements
  def post(self):
    """
    Based of user input create random file then start the appropriate job
    """
    base_list = list(string.ascii_uppercase)
    number_of_shards = int(self.request.get('num_shards', 5))
    number_of_chars_per_file = int(self.request.get('repeat_factor', 5))
    number_of_chars = int(self.request.get('num_letters', 5))
    if number_of_chars > len(base_list):
      number_of_chars = len(base_list)
    number_of_files_per_zip = int(self.request.get('files_per_zip', 5))
    number_of_zip_files = int(self.request.get('number_zips', 5))
    sort_output = self.request.get('sort', 'True') == 'True'
    number_of_stress_files = int(self.request.get('stress_size', 1))
    job = self.request.get('job')
    email = self.request.get('email', None)
    pipe_output = []
    blob_output = []
    list_of_chars = []
    for i in range(number_of_chars):
      list_of_chars.extend([base_list[i]
                            for _ in range(number_of_chars_per_file)])

    if 'FileTest' == job:
      logging.info('Creating test file')
      with cloudstorage.open(FILE_PATH, 'w', content_type='text/plain') as gcs:
        gcs.write(fill_file(list_of_chars))

      destination = 'File.txt'
      pipeline = Mapreduce('FileTest', FILE_PATH, destination,
                  'mapreduce.input_readers.GoogleCloudStorageLineInputReader',
                  number_of_shards, email=email, base_url=self.request.url,
                  sort_output=sort_output)
      pipeline.start()
      blob_output = ('<p><A HREF="/blobstore/' +
                        blobstore.create_gs_key('/gs' + FILE_PATH) +
                        '/File.txt">File Check</A></p>')
      pipe_output = ('<p><A HREF="' + pipeline.base_path + '/status?root='
                    + pipeline.pipeline_id +
                    "' target='_blank'>MR Watch File</A></p>")
    if 'ZipTestWhole' == job or 'ZipTestLine' == job:
      logging.info('Creating test zip')
      for _ in range(number_of_zip_files):
        with cloudstorage.open(ZIP_PATH, 'w',
                               content_type='text/plain') as gcs_file:
          with zipfile.ZipFile(gcs_file, 'w') as zipit:
            for i in range(number_of_files_per_zip):
              file_name = 'test_file_%i.txt' % i
              logging.info('Creating File: %s ', file_name)
              zipit.writestr(file_name, fill_file(list_of_chars),
                             compress_type=zipfile.ZIP_STORED)
      blob_output = ('<p><A HREF="/blobstore/' +
                      blobstore.create_gs_key('/gs' + ZIP_PATH) +
                      '/Zip.zip">Zip Check</A></p>')
      if 'ZipTestWhole' == job:
        destination = 'ZipFile.txt'
        pipeline = Mapreduce('Zip Input', ZIP_PATH, destination,
                             'mapreduce.input_readers'
                             '.GoogleCloudStorageZipInputReader',
                             number_of_shards, email=email,
                             base_url=self.request.url,
                             sort_output=sort_output)
        pipeline.start()

        pipe_output = ('<p><A HREF="' + pipeline.base_path
                      + '/status?root=' + pipeline.pipeline_id
                      + '" target="_blank">MR Watch Zip Input</A></p>')

      if 'ZipTestLine' == job:
        destination = 'ZipLine.txt'
        pipeline = Mapreduce('Zip Line', ZIP_PATH, destination,
                             'mapreduce.input_readers.'
                             'GoogleCloudStorageZipLineInputReader',
                             number_of_shards, email=email,
                             base_url=self.request.url,
                             sort_output=sort_output)
        pipeline.start()
        pipe_output = ('<p><A HREF="' + pipeline.base_path
                       + '/status?root=' + pipeline.pipeline_id
                        + '" target="_blank">MR Watch Zip Line</A></p>')

    if 'StressTest' == job:
      logging.info('Creating stress test file')
      with cloudstorage.open(STRESS_TEST_PATH, 'w',
                             content_type='text/plain') as gcs:
        for _ in range(1024):
          gcs.write('\r\n' * 1024)
      blob_output = ('<p><A HREF="/blobstore/' +
                    blobstore.create_gs_key('/gs' + STRESS_TEST_PATH)
                     + '/Stress_File.txt">Stress File Check</A></p>')
      pipeline = MapperOutputWriterStressTest(number_of_stress_files,
                                              email=email,
                                              base_url=self.request.url,
                                              sort_output=sort_output)
      pipeline.start()
      pipe_output = ('</p><A HREF="' + pipeline.base_path + '/status?root='
                    + pipeline.pipeline_id +
                    '" target="_blank">MR Stress Test</A>')
    # pylint: disable=no-member
    self.response.out.write(jinja2.Environment(loader=jinja2.
                                               FileSystemLoader('templates'),
                                               autoescape=True)
                            .get_template('index.html')
                            .render({'pipe_output':pipe_output,
                                     'blob_output':blob_output}))


# pylint: disable=too-few-public-methods
class GCSServingHandler(blobstore_handlers.BlobstoreDownloadHandler):
  """
  Download handler for files
  When the link is created the name of the file is added at the end
    so the file has a human readable name.
  """
  # pylint: disable=arguments-differ
  def get(self, key):
    key = str(urllib2.unquote(key)).strip()
    logging.debug('key is %s', key)
    # This is needed to get the file name for files created in GCS
    file_info = key.split('/')
    # If there is more than one entry then the first one is the Key,
    # the second is a human readable name
    if len(file_info) > 1:
      self.send_blob(file_info[0], save_as=file_info[1])
    else:
      # Otherwise the file was uploaded with a blob_info, that has the name
      blob_info = blobstore.BlobInfo.get(key)
      self.send_blob(blob_info, save_as=True)


class Mapreduce(base_handler.PipelineBase):
  """
  General Mapreduce call
  """
  # pylint: disable=arguments-differ, unused-argument, too-many-arguments
  def run(self, title, source_file, dest_file, op_type,
          number_of_shards, email=None, base_url=None, sort_output=True):
    """
    Run for the general Mapreduce
    Args:
      title: Title of the job
      source_file: source file to run through the mapper
      dest_file: Destination file to save the output
      op_type: What mapper to run Line, Zip Zip Line
      number_of_shards: Number of shards to use
      email=None: If both email and base url are
                  specified it will email when the work is done
      base_url=None: Root URL, used to build the link to the Mapreduce page

    """
    output = yield mapreduce_pipeline.MapreducePipeline(
      title,
      'main.standard_mapper',
      'main.standard_reducer',
      op_type,
      'mapreduce.output_writers.GoogleCloudStorageMergedOutputWriter',
      mapper_params={'input_reader' : {
                           'bucket_name' : BUCKET_NAME,
                           'objects' : [source_file[len(BUCKET_NAME) + 2:]]
                           },
                       },
      reducer_params={'output_writer' : {'bucket_name' : BUCKET_NAME,
                                           'naming_format' : OUTPUT_FOLDER
                                                            + dest_file,
                                           'sort_output' : sort_output,
                                           'content_type' : 'text/plain'}
                        },
      shards=number_of_shards)
    yield DisplayOutputRaw(output)

  def finalized(self):
    """ Email's results to the specified email """
    email_results(self)


class MapperOutputWriterStressTest(base_handler.PipelineBase):
  """ Stress test for output writer """
  # pylint: disable=arguments-differ, unused-argument
  def run(self, number_of_stress_files=1, email=None,
          base_url=None, sort_output=True):
    """
    Based of selection will generate a number of gigs of output to make
      sure the output writer can handle large outputs

    number_of_stress_files=1: Each file will generate 1g of output
    email=None: If both email and base url are
                specified it will email when the work is done
    base_url=None: Root URL, used to build the link to the Mapreduce page
    """
    output = yield mapper_pipeline.MapperPipeline(
      'Output Writer Stress Test',
      'main.size_stress_test',
      'mapreduce.input_readers.GoogleCloudStorageLineInputReader',
      'mapreduce.output_writers.GoogleCloudStorageMergedOutputWriter',
      params={'input_reader' : {
                           'bucket_name' : BUCKET_NAME,
                           'objects' : [STRESS_TEST_PATH[len(BUCKET_NAME) + 2:]]
                                         * number_of_stress_files
                           },
                'output_writer' : {'bucket_name' : BUCKET_NAME,
                                    'naming_format' : OUTPUT_FOLDER
                                    + 'Stressc.txt',
                                    'sort_output' : sort_output,
                                    'content_type' : 'text/plain'}
                       },
      shards=200)

    yield DisplayDetails(output)
  def finalized(self):
    """ Email's results to the specified email """
    email_results(self)


class DisplayOutputRaw(base_handler.PipelineBase):
  """
    Outputs the file details and contents after a standard test
  """
  # pylint: disable=arguments-differ
  def run(self, output):
    logging.info('Standard test complete, output: %s', output)
    logging.info('File Stats for output: ')
    logging.info(cloudstorage.stat(str(output)))
    logging.info('File contents:')
    with cloudstorage.open(output, 'r') as gcs_file:
      logging.info(gcs_file.read())


class DisplayDetails(base_handler.PipelineBase):
  """
    Outputs the file details and file size in easy read
      format after a stress test
  """
  # pylint: disable=arguments-differ
  def run(self, output):
    logging.info('Stress test complete, output: %s', str(output))
    file_stats = cloudstorage.stat(str(output))
    logging.info('Details: %s', str(file_stats))
    logging.info('Output file size %s', human_readable_size(file_stats.st_size))
    logging.info('Cleaning up all files')
    # Clean up files to reduce charges
    cloudstorage.delete(output)
    cloudstorage.delete(STRESS_TEST_PATH)

def email_results(self):
  """
    If both email and base are set emails result page and details.
  """
  if self.pipeline_id == self.root_pipeline_id:
    email = self.kwargs.get('email', None)
    base_url = self.kwargs.get('base_url', 'http://%s.appspot.com%s' %
                               (str(app_identity.get_application_id()),
                                self.base_path))
    if email is not None and email != '':
      body = '%smapreduce/pipeline/status?root=%s' % (base_url,
                                                      self.root_pipeline_id)

      mail.send_mail(sender=('noreply@' + str(app_identity.get_application_id())
                     + '.appspotmail.com'),
                     to=email,
                     subject='Work Finished C',
                     body=body)
    else:
      logging.info('Email empty, no email sent')

def human_readable_size(size):
  """Converts bytes into largest grouping"""
  for unit in ['b', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB']:
    if abs(size) < 1024.0:
      return '%3.1f%s' % (size, unit)
    size /= 1024.0
  return '%.1f%s' % (size, 'YB')

def split_into_sentences(input_string):
  """Split text into list of sentences."""
  input_string = re.sub(r'\s+', ' ', input_string)
  input_string = re.sub(r'[\\.\\?\\!]', '\n', input_string)
  return input_string.split('\n')

def split_into_words(input_string):
  """Split a sentence into list of words."""
  input_string = re.sub(r'\W+', ' ', input_string)
  input_string = re.sub(r'[_0-9]+', ' ', input_string)
  return input_string.split()

def standard_mapper(data):
  """Mapper, splits in bound data by white space"""
  _, data = data
  for sub in split_into_sentences(data):
    for word in split_into_words(sub.lower()):
      yield (word, '')

def standard_reducer(key, values):
  """Yields the count of each grouping"""
  yield '%s: %d\n' % (key, len(values))

# pylint: disable=unused-argument
def size_stress_test(data):
  """Generates random letters for stress test"""
  yield ''.join([random.choice(STRESS_TEST_CHAR_BASE)
                 for _ in range(STRESS_TEST_WORD_SIZE)]) + '\n'

APP = webapp2.WSGIApplication([
  ('/', MainPage),
  ('/blobstore/(.*)', GCSServingHandler),
], debug=True)
