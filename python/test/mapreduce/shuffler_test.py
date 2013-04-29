#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.




import unittest
from testlib import mox

from google.appengine.api import apiproxy_stub
from google.appengine.api.files import file_service_stub
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

  def createFileServiceStub(self, blob_storage):
    return MockFileServiceStub(blob_storage)

  def testSuccessfulRun(self):
    p = shuffler._ShuffleServicePipeline("testjob", ["file1", "file2"])
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    request = self.file_service.shuffle_request
    self.assertTrue(request)
    self.assertTrue(request.shuffle_name().startswith("testjob-"))
    self.assertEquals(2, len(request.input_list()))
    self.assertEquals(1, request.input(0).format())
    self.assertEquals("file1", request.input(0).path())
    self.assertEquals(1, request.input(1).format())
    self.assertEquals("file2", request.input(1).path())
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
    p = shuffler._ShuffleServicePipeline("testjob", ["file1", "file2"])
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


if __name__ == "__main__":
  unittest.main()

