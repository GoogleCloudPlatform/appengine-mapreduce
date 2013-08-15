#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.




import unittest
from testlib import mox

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import files
from google.appengine.api.files import file_service_stub
from google.appengine.ext import testbed
from mapreduce import shuffler
from mapreduce import test_support
from testlib import testutil


class MockFileServiceStub(file_service_stub.FileServiceStub):
  """A FileServiceStub to be used with mox."""

  def __init__(self, blob_storage):
      file_service_stub.FileServiceStub.__init__(self, blob_storage)
      self.shuffle_request = None

  def _Dynamic_GetCapabilities(self, request, response):
    """Handler for GetCapabilities RPC call."""
    file_service_stub.FileServiceStub._Dynamic_GetCapabilities(
        self, request, response)
    response.set_shuffle_available(True)

  def _Dynamic_Shuffle(self, request, response):
    self.shuffle_request = request


class ShuffleServicePipelineTest(testutil.HandlerTestBase):
  """Tests for _ShuffleServicePipeline."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    # Use a special file service stub that enables the shuffle service
    self.file_service = MockFileServiceStub(self.testbed.get_stub(
        testbed.BLOBSTORE_SERVICE_NAME).storage)
    apiproxy_stub_map.apiproxy.ReplaceStub(testbed.FILES_SERVICE_NAME,
                                           self.file_service)

  def _CreateInputFile(self):
    input_file = files.blobstore.create()
    with files.open(input_file, "a") as f:
      f.write("foo")
    files.finalize(input_file)
    input_file = files.blobstore.get_file_name(
        files.blobstore.get_blob_key(input_file))
    return input_file

  def testSuccessfulRun(self):
    input_file1 = self._CreateInputFile()
    input_file2 = self._CreateInputFile()
    p = shuffler._ShuffleServicePipeline("testjob", [input_file1, input_file2])
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    request = self.file_service.shuffle_request
    self.assertTrue(request)
    self.assertTrue(request.shuffle_name().startswith("testjob-"))
    self.assertEquals(2, len(request.input_list()))
    self.assertEquals(1, request.input(0).format())
    self.assertEquals(input_file1, request.input(0).path())
    self.assertEquals(1, request.input(1).format())
    self.assertEquals(input_file2, request.input(1).path())
    self.assertEquals(2, len(request.output().path_list()))

    callback = request.callback()
    self.assertTrue(callback.url().startswith(
        "/mapreduce/pipeline/callback?pipeline_id="))
    self.assertEquals(self.version_id, callback.app_version_id())
    self.assertEquals("GET", callback.method())
    self.assertEquals("default", callback.queue())

    callback_task = {
        "url": callback.url(),
        "method": callback.method(),
        }
    test_support.execute_task(callback_task)
    test_support.execute_until_empty(self.taskqueue)

    p = shuffler._ShuffleServicePipeline.from_id(p.pipeline_id)
    self.assertTrue(p.has_finalized)
    output_files = p.outputs.default.value
    self.assertEquals(2, len(output_files))
    self.assertTrue(output_files[0].startswith("/blobstore/"))
    self.assertTrue(output_files[1].startswith("/blobstore/"))

  def testError(self):
    input_file1 = self._CreateInputFile()
    input_file2 = self._CreateInputFile()

    p = shuffler._ShuffleServicePipeline("testjob", [input_file1, input_file2])
    self.assertEquals(1, p.current_attempt)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    request = self.file_service.shuffle_request
    callback = request.callback()

    callback_task = {
        "url": callback.url() + "&error=1",
        "method": callback.method(),
        }
    test_support.execute_task(callback_task)
    test_support.execute_until_empty(self.taskqueue)

    p = shuffler._ShuffleServicePipeline.from_id(p.pipeline_id)
    self.assertEquals(2, p.current_attempt)

  def testNoData(self):
    input_file = files.blobstore.create()
    files.finalize(input_file)
    input_file = files.blobstore.get_file_name(
        files.blobstore.get_blob_key(input_file))
    p = shuffler._ShuffleServicePipeline("testjob", [input_file, input_file])
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    # No shuffle request.
    request = self.file_service.shuffle_request
    self.assertEqual(None, request)

    p = shuffler._ShuffleServicePipeline.from_id(p.pipeline_id)
    self.assertEqual([], p.outputs.default.value)

  def testNoInputFile(self):
    p = shuffler._ShuffleServicePipeline("testjob", [])
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    # No shuffle request.
    request = self.file_service.shuffle_request
    self.assertEqual(None, request)

    p = shuffler._ShuffleServicePipeline.from_id(p.pipeline_id)
    self.assertEqual([], p.outputs.default.value)


if __name__ == "__main__":
  unittest.main()

