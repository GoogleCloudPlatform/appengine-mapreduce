#!/usr/bin/env python
"""Run handlers using webapp2."""


# pylint: disable=g-statement-before-imports
def _ImportWebapp2():
  # This file hot swaps webapp to webapp2 in python27 runtime.
  # Fake python27 runtime.
  # pylint: disable=g-import-not-at-top
  import os
  os.environ["APPENGINE_RUNTIME"] = "python27"
  __import__("google.appengine.ext", fromlist=["webapp"])
  del os.environ["APPENGINE_RUNTIME"]
_ImportWebapp2()


import unittest

from mapreduce import input_readers
from mapreduce import mapreduce_pipeline
from mapreduce import model
from mapreduce import output_writers
from mapreduce import test_support
from testlib import testutil


# Map or reduce functions.
def TestMapreduceMap(string):
  """Test map handler."""
  yield (string, "")


def TestMapreduceReduce(key, values):
  """Test reduce handler."""
  yield str((key, values))


class Webapp2EndToEndTest(testutil.HandlerTestBase):
  """Test mapreduce handlers under webapp2."""

  def testUsingWebapp2(self):
    # Access a webapp2 only class.
    # pylint: disable=g-import-not-at-top
    from google.appengine.ext import webapp
    self.assertTrue(webapp.Webapp2HandlerAdapter)

  def testSmoke(self):
    """Test all handlers still works.

    This test doesn't care about the integrity of the job outputs.
    Just that things works under webapp2 framework.
    """
    # Run Mapreduce
    p = mapreduce_pipeline.MapreducePipeline(
        "test",
        __name__ + ".TestMapreduceMap",
        __name__ + ".TestMapreduceReduce",
        input_reader_spec=input_readers.__name__ + ".RandomStringInputReader",
        output_writer_spec=(
            output_writers.__name__ + "._GoogleCloudStorageRecordOutputWriter"),
        mapper_params={
            "input_reader": {
                "count": 100
            },
        },
        reducer_params={
            "output_writer": {
                "bucket_name": "test"
            },
        },
        shards=3)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    # Verify output.
    p = mapreduce_pipeline.MapreducePipeline.from_id(p.pipeline_id)
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS,
                     p.outputs.result_status.value)


if __name__ == "__main__":
  unittest.main()
