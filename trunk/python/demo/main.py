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

"""."""



import logging
import re

from google.appengine.ext import blobstore
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.ext.webapp import util
from google.appengine.api import taskqueue
from mapreduce import base_handler
from mapreduce import mapreduce_pipeline
from mapreduce import operation as op
from mapreduce import shuffler


BLOB_KEY="AMIfv97lCf3MiRAwdYtZbIzd4EKGfzP8ns63qHcvnSZg9fSxFNoiEPpufZESxHllm3lxx0-DQHEVA5qKEw5bKt6J9IPngigyhQepUuMli8-zWADWhsCRjzA-dIenu7p31B6gGupP7OpDqLqFSUUZDr07B5gJdRfPWw"


class IndexHandler(webapp.RequestHandler):
  def get(self):
    self.response.out.write(
        """<form action="/" method="post">
           Blob Key: <input type="text" name="blobkey"></br>
           <input type="submit" name="word_count" value="Word Count">
           <input type="submit" name="index" value="Index">
           <input type="submit" name="phrases" value="Phrases">
           </form>
           <br/>
           <a href="/upload">Upload Blob</a><br/>
           <form action="/download" method="get">
           <input type="text" name="blobkey">
           <input type="submit" value="Download Blob">
           </form>
        """)

  def post(self):
    blob_key = self.request.get("blobkey").strip()

    if self.request.get("word_count"):
      pipeline = WordCountPipeline(blob_key)
    elif self.request.get("index"):
      pipeline = IndexPipeline(blob_key)
    else:
      pipeline = PhrasesPipeline(blob_key)
    pipeline.start()
    self.redirect(pipeline.base_path + '/status?root=' + pipeline.pipeline_id)


def split_into_sentences(s):
  """Split text into list of sentences."""
  s = re.sub(r"\s+", " ", s)
  s = re.sub(r"[\\.\\?\\!]", "\n", s)
  return s.split("\n")


def split_into_words(s):
  """Split a sentence into list of words."""
  s = re.sub(r"\W+", " ", s)
  s = re.sub(r"[_0-9]+", " ", s)
  return s.split()


def word_count_map(data):
  """Word count map function."""
  (entry, text_fn) = data
  text = text_fn()

  logging.debug("Got %s", entry.filename)
  for s in split_into_sentences(text):
    for w in split_into_words(s.lower()):
      yield (w, "")


def word_count_reduce(key, values):
  """Word count reduce function."""
  yield "%s: %d\n" % (key, len(values))


def index_map(data):
  """Index demo map function."""
  (entry, text_fn) = data
  text = text_fn()

  logging.debug("Got %s", entry.filename)
  for s in split_into_sentences(text):
    for w in split_into_words(s.lower()):
      yield (w, entry.filename)


def index_reduce(key, values):
  """Index demo reduce function."""
  yield "%s: %s\n" % (key, list(set(values)))


PHRASE_LENGTH = 4


def phrases_map(data):
  """Phrases demo map function."""
  (entry, text_fn) = data
  text = text_fn()
  filename = entry.filename

  logging.debug("Got %s", filename)
  for s in split_into_sentences(text):
    words = split_into_words(s.lower())
    if len(words) < PHRASE_LENGTH:
      yield (":".join(words), filename)
      continue
    for i in range(0, len(words) - PHRASE_LENGTH):
      yield (":".join(words[i:i+PHRASE_LENGTH]), filename)


def phrases_reduce(key, values):
  """Phrases demo reduce function."""
  if len(values) < 10:
    return
  counts = {}
  for filename in values:
    counts[filename] = counts.get(filename, 0) + 1

  words = re.sub(r":", " ", key)
  threshold = len(values) / 2
  for filename, count in counts.items():
    if count > threshold:
      yield "%s:%s\n" % (words, filename)


class WordCountPipeline(base_handler.PipelineBase):
  """A pipeline to run Word count demo.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """

  def run(self, blobkey):
    yield mapreduce_pipeline.MapreducePipeline(
        "word_count",
        "main.word_count_map",
        "main.word_count_reduce",
        "mapreduce.input_readers.BlobstoreZipInputReader",
        "mapreduce.output_writers.BlobstoreOutputWriter",
        mapper_params={
            'blob_key': blobkey,
        },
        reducer_params={
            'mime_type': 'text/plain',
        },
        shards=16)


class IndexPipeline(base_handler.PipelineBase):
  """A pipeline to run Index demo.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """


  def run(self, blobkey):
    yield mapreduce_pipeline.MapreducePipeline(
        "index",
        "main.index_map",
        "main.index_reduce",
        "mapreduce.input_readers.BlobstoreZipInputReader",
        "mapreduce.output_writers.BlobstoreOutputWriter",
        mapper_params={
            'blob_key': blobkey,
        },
        reducer_params={
            'mime_type': 'text/plain',
        },
        shards=16)



class PhrasesPipeline(base_handler.PipelineBase):
  """A pipeline to run Phrases demo.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """

  def run(self, blobkey):
    yield mapreduce_pipeline.MapreducePipeline(
        "phrases",
        "main.phrases_map",
        "main.phrases_reduce",
        "mapreduce.input_readers.BlobstoreZipInputReader",
        "mapreduce.output_writers.BlobstoreOutputWriter",
        mapper_params={
            'blob_key': blobkey,
        },
        reducer_params={
            'mime_type': 'text/plain',
        },
        shards=16)


class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
  """Handler to upload data to blobstore."""

  def get(self):
    upload_url = blobstore.create_upload_url('/upload')
    self.response.out.write('<html><body>')
    self.response.out.write(
        '<form action="%s" method="POST" enctype="multipart/form-data">' % 
        upload_url)
    self.response.out.write(
        "Upload File: <input type='file' name='file'><br>"
        "<input type='submit' name='submit' value='Upload'>"
        "</form></body></html>")

  def post(self):
    upload_files = self.get_uploads('file')
    blob_info = upload_files[0]
    self.redirect('/')


class DownloadHandler(blobstore_handlers.BlobstoreDownloadHandler):
  """Handler to download blob by blobkey."""

  def get(self):
    blob_info = blobstore.BlobInfo.get(self.request.get("blobkey").strip())
    self.send_blob(blob_info)


APP = webapp.WSGIApplication(
    [
        ('/', IndexHandler),
        ('/upload', UploadHandler),
        ('/download', DownloadHandler),
    ],
    debug=True)

def main():
  util.run_wsgi_app(APP)




if __name__ == '__main__':
  main()
