#!/usr/bin/env python


# testutil must be imported before mock.
# pylint: disable=unused-import
# pylint: disable=g-bad-import-order
from testlib import testutil

import mock
import unittest

from mapreduce.api.map_job import output_writer


class OutputWriterTest(unittest.TestCase):

  def testBeginEndSlice(self):
    writer = output_writer.OutputWriter()
    slice_ctx = mock.Mock()
    writer.begin_slice(slice_ctx)
    self.assertEqual(slice_ctx, writer._slice_ctx)
    writer.end_slice(slice_ctx)
    self.assertEqual(None, writer._slice_ctx)


if __name__ == '__main__':
  unittest.main()
