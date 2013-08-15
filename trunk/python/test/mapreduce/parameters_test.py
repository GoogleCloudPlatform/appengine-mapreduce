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


class UserParametersTest(unittest.TestCase):

  def testUserCanSetParameters(self):
    # Match test_data/appengine_config.py
    self.assertEqual(5, parameters.config.SHARD_RETRY_LIMIT)
    self.assertEqual('foo', parameters.config.QUEUE_NAME)
    self.assertEqual('/my-mapreduce', parameters.config.BASE_PATH)

    # No overriding. Default settings that match parameters.py
    self.assertEqual(8, parameters.config.SHARD_COUNT)
    self.assertEqual(1000000, parameters.config.PROCESSING_RATE_PER_SEC)
    self.assertEqual(10, parameters.config.RETRY_SLICE_ERROR_MAX_RETRIES)
    self.assertEqual(30, parameters.config.MAX_TASK_RETRIES)
    self.assertEqual(15, parameters.config._SLICE_DURATION_SEC)
    self.assertEqual(1, parameters.config._LEASE_GRACE_PERIOD)
    self.assertEqual(10 * 60 + 30, parameters.config._REQUEST_EVENTUAL_TIMEOUT)
    self.assertEqual(2, parameters.config._CONTROLLER_PERIOD_SEC)

    # Other constant that depends on _config.
    self.assertEqual('/my-mapreduce/pipeline',
                     parameters._DEFAULT_PIPELINE_BASE_PATH)

  def testBackwardCompat(self):
    self.assertEqual('/my-mapreduce', parameters._DEFAULT_BASE_PATH)
    self.assertEqual(5, parameters.DEFAULT_SHARD_RETRY_LIMIT)
    self.assertEqual('foo', parameters.DEFAULT_QUEUE_NAME)
    self.assertEqual(8, parameters.DEFAULT_SHARD_COUNT)
    self.assertEqual(1000000, parameters._DEFAULT_PROCESSING_RATE_PER_SEC)
    self.assertEqual(10, parameters._RETRY_SLICE_ERROR_MAX_RETRIES)
    self.assertEqual(30, parameters._MAX_TASK_RETRIES)
    self.assertEqual(15, parameters._SLICE_DURATION_SEC)
    self.assertEqual(1, parameters._LEASE_GRACE_PERIOD)
    self.assertEqual(10 * 60 + 30, parameters._REQUEST_EVENTUAL_TIMEOUT)
    self.assertEqual(2, parameters._CONTROLLER_PERIOD_SEC)


if __name__ == '__main__':
  unittest.main()
