#!/usr/bin/env python


# testutil must be imported before mock.
# pylint: disable=unused-import
# pylint: disable=g-bad-import-order
from testlib import testutil

import mock
import unittest

from mapreduce.api import map_job


class InputReaderTest(unittest.TestCase):

  def testBeginEndSlice(self):
    reader = map_job.InputReader()
    slice_ctx = mock.Mock()
    reader.begin_slice(slice_ctx)
    self.assertEqual(slice_ctx, reader._slice_ctx)
    reader.end_slice(slice_ctx)
    self.assertEqual(None, reader._slice_ctx)


if __name__ == '__main__':
  unittest.main()
