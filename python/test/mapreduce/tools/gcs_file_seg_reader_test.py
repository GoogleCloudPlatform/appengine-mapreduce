#!/usr/bin/env python
"""Tests for gcs_file_seg_reader."""

# pylint: disable=g-bad-name

import pickle
import unittest

from google.appengine.datastore import datastore_stub_util
import cloudstorage
from google.appengine.ext import testbed
from mapreduce import output_writers
from mapreduce.tools import gcs_file_seg_reader


class GCSFileSegReaderTest(unittest.TestCase):
  """Test GCSFileSegReader."""

  def setUp(self):
    super(GCSFileSegReaderTest, self).setUp()

    self.testbed = testbed.Testbed()
    self.testbed.activate()

    self.testbed.init_app_identity_stub()
    self.testbed.init_blobstore_stub()
    # HRD with no eventual consistency.
    policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=1)
    self.testbed.init_datastore_v3_stub(consistency_policy=policy)
    self.testbed.init_memcache_stub()
    self.testbed.init_urlfetch_stub()

    self.writer_cls = output_writers._GoogleCloudStorageOutputWriter

    self.seg_prefix = "/bucket/prefix-"

  def tearDown(self):
    self.testbed.deactivate()
    super(GCSFileSegReaderTest, self).tearDown()

  def testMissingMetadata(self):
    f = cloudstorage.open(self.seg_prefix + "0", "w")
    f.write("abc")
    f.close()

    self.assertRaises(ValueError,
                      gcs_file_seg_reader._GCSFileSegReader,
                      self.seg_prefix, 0)

  def testInvalidMetadata(self):
    f = cloudstorage.open(self.seg_prefix + "0", "w",
                          options={self.writer_cls._VALID_LENGTH: "10"})
    f.write("abc")
    f.close()

    self.assertRaises(ValueError,
                      gcs_file_seg_reader._GCSFileSegReader,
                      self.seg_prefix, 0)

  def ReadOneFileTest(self, read_size):
    f = cloudstorage.open(self.seg_prefix + "0", "w",
                          options={self.writer_cls._VALID_LENGTH: "5"})
    f.write("1234567")
    f.close()

    r = gcs_file_seg_reader._GCSFileSegReader(self.seg_prefix, 0)
    result = ""
    while True:
      tmp = r.read(read_size)
      if not tmp:
        break
      result += tmp
    self.assertEqual("12345", result)
    self.assertEqual(5, r.tell())

  def testReadBig(self):
    """Test read bigger than valid offset."""
    self.ReadOneFileTest(10)

  def testReadSmall(self):
    """Test read smaller than valid offset."""
    self.ReadOneFileTest(1)

  def testReadEqual(self):
    """Test read size equals valid offset."""
    self.ReadOneFileTest(5)

  def setUpMultipleFile(self):
    f = cloudstorage.open(self.seg_prefix + "0", "w",
                          options={self.writer_cls._VALID_LENGTH: "5"})
    f.write("12345garbage")
    f.close()

    f = cloudstorage.open(self.seg_prefix + "1", "w",
                          options={self.writer_cls._VALID_LENGTH: "5"})
    f.write("67890garbage")
    f.close()

    f = cloudstorage.open(self.seg_prefix + "2", "w",
                          options={self.writer_cls._VALID_LENGTH: "6"})
    f.write("123456garbage")
    f.close()

  def testReadMultipleFiles(self):
    self.setUpMultipleFile()

    r = gcs_file_seg_reader._GCSFileSegReader(self.seg_prefix, 2)
    result = ""
    while True:
      tmp = r.read(1)
      if not tmp:
        break
      result += tmp
    self.assertEqual("1234567890123456", result)
    self.assertEqual(len(result), r.tell())

  def testPickle(self):
    self.setUpMultipleFile()

    r = gcs_file_seg_reader._GCSFileSegReader(self.seg_prefix, 2)
    result = ""
    while True:
      r = pickle.loads(pickle.dumps(r))
      tmp = r.read(1)
      if not tmp:
        break
      result += tmp
      self.assertEqual(len(result), r.tell())
    self.assertEqual("1234567890123456", result)


if __name__ == "__main__":
  unittest.main()
