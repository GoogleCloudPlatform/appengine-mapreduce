#!/usr/bin/env python
"""Tests for parameters.py."""

import os
import sys
import unittest

# Add appengine_config.py to sys.path to imitate user config.
_TEST_DATA_PATH = os.path.join(os.path.dirname(__file__), 'test_data')
sys.path.insert(0, _TEST_DATA_PATH)

# pylint: disable=g-import-not-at-top
from mapreduce import parameters


class Foo(object):
  pass


class Bar(Foo):
  pass


class TestConfig(parameters._Config):
  a = parameters._Option(str, required=True)
  b = parameters._Option(bool, default=True)
  c = parameters._Option(Foo, can_be_none=True)
  d = parameters._Option(Foo)


class TestConfig2(parameters._Config):
  b = parameters._Option(str, required=True)
  z = parameters._Option(int, required=True)


class TestConfig3(TestConfig2, TestConfig):
  pass


class JobConfigTest(unittest.TestCase):

  def testRequiredField(self):
    self.assertRaises(ValueError, TestConfig)

  def testSmoke(self):
    config = TestConfig(a='foo', d=Bar)
    self.assertEqual('foo', config.a)
    self.assertEqual(True, config.b)
    self.assertEqual(None, config.c)
    self.assertEqual(Bar, config.d)

  def testInstanceTypeCheck(self):
    self.assertRaises(TypeError, TestConfig, a='foo', d=Bar,
                      # b has wrong type.
                      b='bar')

  def testSubclassTypeCheckFails(self):
    self.assertRaises(TypeError, TestConfig, a='foo',
                      # d has wrong type.
                      d=object)

  def testSubclassTypeCheckPasses(self):
    config = TestConfig(a='foo', d=Bar)
    self.assertEqual(Bar, config.d)

  def testTestMode(self):
    TestConfig(_test=True)

  def testToFromJson(self):
    config = TestConfig(a='foo', b=True, c=Foo, d=Bar)
    config2 = TestConfig._from_json(config._to_json())
    self.assertTrue(config == config2)

  def testConfigInheritance(self):
    # Should inherit b from TestConfig2 instead of TestConfig
    config = TestConfig3(a='foo', b='bar', d=Bar, z=2)
    self.assertEqual('foo', config.a)
    self.assertEqual('bar', config.b)
    self.assertEqual(2, config.z)


class OptionTest(unittest.TestCase):

  def testDefault(self):
    self.assertRaises(ValueError,
                      parameters._Option,
                      str, required=True, default=1)


class UserParametersTest(unittest.TestCase):

  def testUserCanSetParameters(self):
    # Match test_data/appengine_config.py
    self.assertEqual(5, parameters.config.SHARD_MAX_ATTEMPTS)
    self.assertEqual('foo', parameters.config.QUEUE_NAME)
    self.assertEqual('/my-mapreduce', parameters.config.BASE_PATH)

    # No overriding. Default settings that match parameters.py
    self.assertEqual(8, parameters.config.SHARD_COUNT)
    self.assertEqual(1000000, parameters.config.PROCESSING_RATE_PER_SEC)
    self.assertEqual(11, parameters.config.TASK_MAX_DATA_PROCESSING_ATTEMPTS)
    self.assertEqual(31, parameters.config.TASK_MAX_ATTEMPTS)
    self.assertEqual(15, parameters.config._SLICE_DURATION_SEC)
    self.assertEqual(1, parameters.config._LEASE_GRACE_PERIOD)
    self.assertEqual(10 * 60 + 30, parameters.config._REQUEST_EVENTUAL_TIMEOUT)
    self.assertEqual(2, parameters.config._CONTROLLER_PERIOD_SEC)

    # Other constant that depends on _config.
    self.assertEqual('/my-mapreduce/pipeline',
                     parameters._DEFAULT_PIPELINE_BASE_PATH)


if __name__ == '__main__':
  unittest.main()
