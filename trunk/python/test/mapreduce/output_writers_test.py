#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.





import os
from testlib import mox
import unittest

from google.appengine.api import apiproxy_stub_map
from mapreduce.lib import files
from mapreduce.lib.files import testutil as files_testutil
from mapreduce.lib.files import records
from mapreduce import errors
from mapreduce import model
from mapreduce import output_writers
from testlib import testutil


FILE_WRITER_NAME = (output_writers.__name__ + "." +
                    output_writers.FileOutputWriter.__name__)


class FilePoolTest(unittest.TestCase):
  """Tests for _FilePool class."""

  def setUp(self):
    self.file_service = files_testutil.TestFileServiceStub()
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub(
        "file", self.file_service)

    self.pool = output_writers._FilePool(flush_size_chars=10)

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
    self.assertRaises(errors.Error, self.pool.append, "foo", "a"*1024*1024*2)

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


class RecordsPoolTest(unittest.TestCase):
  """Tests for RecordsPool."""

  def setUp(self):
    self.file_service = files_testutil.TestFileServiceStub()
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub(
        "file", self.file_service)

    self.pool = output_writers.RecordsPool("tempfile", flush_size_chars=30)

  def testAppendAndFlush(self):
    self.pool.append("a")
    self.assertEquals("", self.file_service.get_content("tempfile"))
    self.pool.append("b")
    self.assertEquals("", self.file_service.get_content("tempfile"))
    self.pool.flush()
    self.assertEquals(
        ["a", "b"],
        list(records.RecordsReader(files.open("tempfile", "r"))))


class FileOutputWriterTest(testutil.HandlerTestBase):

  def create_mapper_spec(self,
                         output_writer_spec=FILE_WRITER_NAME,
                         params=None):
    params = params or {}
    mapper_spec = model.MapperSpec(
        "FooHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        params,
        10,
        output_writer_spec=output_writer_spec)
    return mapper_spec

  def create_mapreduce_state(self, params=None):
    mapreduce_spec = model.MapreduceSpec(
        "mapreduce0",
        "mapreduce0",
        self.create_mapper_spec(params=params).to_json())
    mapreduce_state = model.MapreduceState.create_new("mapreduce0")
    mapreduce_state.mapreduce_spec = mapreduce_spec
    return mapreduce_state

  def testValidate_Passes(self):
    output_writers.FileOutputWriter.validate(
        self.create_mapper_spec(params={"filesystem": "blobstore"}))

  def testValidate_WriterNotSet(self):
    self.assertRaises(
        errors.BadWriterParamsError,
        output_writers.FileOutputWriter.validate,
        self.create_mapper_spec(output_writer_spec=None))

  def testValidate_ShardingNone(self):
    output_writers.FileOutputWriter.validate(self.create_mapper_spec(
        params={"output_sharding": "NONE", "filesystem": "blobstore"}))

  def testValidate_ShardingInput(self):
    output_writers.FileOutputWriter.validate(self.create_mapper_spec(
        params={"output_sharding": "input", "filesystem": "blobstore"}))

  def testValidate_ShardingIncorrect(self):
    self.assertRaises(
        errors.BadWriterParamsError,
        output_writers.FileOutputWriter.validate,
        self.create_mapper_spec(
            params={"output_sharding": "foo", "filesystem": "blobstore"}))

  def testInitJob_NoSharding(self):
    mapreduce_state = self.create_mapreduce_state(
        params={"filesystem": "blobstore"})
    output_writers.FileOutputWriter.init_job(mapreduce_state)
    self.assertTrue(mapreduce_state.writer_state)
    filenames = output_writers.FileOutputWriter.get_filenames(mapreduce_state)
    self.assertEqual(1, len(filenames))
    self.assertTrue(filenames[0].startswith("/blobstore/writable:"))

  def testInitJob_ShardingNone(self):
    mapreduce_state = self.create_mapreduce_state(
        params={"output_sharding": "none", "filesystem": "blobstore"})
    output_writers.FileOutputWriter.init_job(mapreduce_state)
    self.assertTrue(mapreduce_state.writer_state)
    filenames = output_writers.FileOutputWriter.get_filenames(mapreduce_state)
    self.assertEqual(1, len(filenames))
    self.assertTrue(filenames[0].startswith("/blobstore/writable:"))

  def testInitJob_ShardingInput(self):
    mapreduce_state = self.create_mapreduce_state(
        params={"output_sharding": "input", "filesystem": "blobstore"})
    output_writers.FileOutputWriter.init_job(mapreduce_state)
    self.assertTrue(mapreduce_state.writer_state)
    filenames = output_writers.FileOutputWriter.get_filenames(mapreduce_state)
    self.assertEqual(10, len(filenames))
    for filename in filenames:
      self.assertTrue(filename.startswith("/blobstore/writable:"))


if __name__ == "__main__":
  unittest.main()
