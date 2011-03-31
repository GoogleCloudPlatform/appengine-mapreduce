#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.





import os
from testlib import mox
import unittest
import shutil
import tempfile

from google.appengine.api import apiproxy_stub_map
from google.appengine.api.blobstore import blobstore_stub
from google.appengine.api.blobstore import file_blob_storage
from mapreduce.lib import files
from google.appengine.api.files import file_service_stub
from mapreduce.lib.files import testutil as files_testutil
from google.appengine.ext import db
from mapreduce import control
from mapreduce import errors
from mapreduce import model
from mapreduce import output_writers
from testlib import testutil


class TestEntity(db.Model):
  """Test entity class."""


def test_handler_yield_key_str(entity):
  """Test handler which yields entity key."""
  yield str(entity.key()) + "\n"


class FilePoolTest(unittest.TestCase):
  """Tests for _FilePool class."""

  def setUp(self):
    self.file_service = files_testutil.TestFileServiceStub()
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub(
        "file", self.file_service)

    self.pool = output_writers._FilePool(max_size_chars=10)

  def testAppendAndFlush(self):
    self.pool.append("foo", "a")
    self.assertEquals("", self.file_service.get_content("foo"))
    self.pool.append("foo", "b")
    self.assertEquals("", self.file_service.get_content("foo"))
    self.pool.flush()
    self.assertEquals("ab", self.file_service.get_content("foo"))

  def testAutoFlush(self):
    self.pool.append("foo", "a"*10)
    self.pool.append("foo", "b")
    self.assertEquals("a"*10, self.file_service.get_content("foo"))
    self.pool.flush()
    self.assertEquals("a"*10 + "b", self.file_service.get_content("foo"))

  def testAppendTooMuchData(self):
    """Test appending too much data."""
    self.assertRaises(errors.Error, self.pool.append, "foo", "a"*25)

  def testAppendMultipleFiles(self):
    self.pool.append("foo", "a")
    self.pool.append("bar", "b")
    self.pool.append("foo", "a")
    self.pool.append("bar", "b")

    self.assertEquals("", self.file_service.get_content("foo"))
    self.assertEquals("", self.file_service.get_content("bar"))
    self.pool.flush()
    self.assertEquals("aa", self.file_service.get_content("foo"))
    self.assertEquals("bb", self.file_service.get_content("bar"))


class BlobstoreOutputWriterEndToEndTest(testutil.HandlerTestBase):
  """End-to-end tests for BlobstoreOutputWriter.

  BlobstoreOutputWriter isn't complex enough yet to do extensive
  unit tests. Do end-to-end tests just to check that it works.
  """

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)

    self.blob_storage_directory = tempfile.mkdtemp()
    blob_storage = file_blob_storage.FileBlobStorage(
        self.blob_storage_directory, self.appid)
    self.blobstore_stub = blobstore_stub.BlobstoreServiceStub(blob_storage)
    apiproxy_stub_map.apiproxy.RegisterStub("blobstore", self.blobstore_stub)
    apiproxy_stub_map.apiproxy.RegisterStub(
        "file", file_service_stub.FileServiceStub(blob_storage))

  def tearDown(self):
    shutil.rmtree(self.blob_storage_directory)
    testutil.HandlerTestBase.tearDown(self);

  def testSmoke(self):
    entity_count = 1000

    for i in range(entity_count):
      TestEntity().put()

    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".test_handler_yield_key_str",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
        },
        shard_count=4,
        base_path="/mapreduce_base_path",
        output_writer_spec=
            output_writers.__name__ + "." +
            output_writers.BlobstoreOutputWriter.__name__)

    testutil.execute_until_empty(self.taskqueue)

    mapreduce_state = model.MapreduceState.get_by_job_id(mapreduce_id)
    blob_name = mapreduce_state.writer_state["filename"]
    self.assertTrue(blob_name.startswith("/blobstore/"))
    self.assertFalse(blob_name.startswith("/blobstore/c_"))

    with files.open(blob_name, "r") as f:
      data = f.read(10000000)
      self.assertEquals(1000, len(data.strip().split("\n")))


if __name__ == "__main__":
  unittest.main()
