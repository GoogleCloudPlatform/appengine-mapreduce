#!/usr/bin/env python
# Copyright 2012 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.




import os
import random
import string
import tempfile
import unittest
import zipfile

from google.appengine.api.files import file as files
from mapreduce import file_format_root


class SplitTest(unittest.TestCase):
  """tests for file_format_root.split.

  All other functionalities of FileFormatRoot is blackbox tested by
  file_formats_test.
  """

  def setUp(self):
    unittest.TestCase.setUp(self)
    self._files_open = files.open
    self._files_stat = files.stat
    files.open = open
    files.stat = os.stat
    self.__created_files = []

  def tearDown(self):
    files.open = self._files_open
    files.stat = self._files_stat
    for filename in self.__created_files:
      os.remove(filename)

  def createFilesForDeepSplitTest(self):
    def _random_filename():
      return ''.join(random.choice(string.ascii_letters) for _ in range(10))

    # Create 100 zip files, each of which has 10 member files.
    tmp_filenames = []
    for _ in range(100):
      _, path = tempfile.mkstemp(text=True)
      tmp_filenames.append(path)
      archive = zipfile.ZipFile(path, 'w')
      for _ in range(10):
        archive.writestr(_random_filename(),
                         (string.ascii_letters + '\n')*10000)
      archive.close()

    self.__created_files.extend(tmp_filenames)

    return tmp_filenames

  def runDeepSplitTest(self, shards, filenames):
    roots = file_format_root.split(filenames, 'zip', shards)
    self.assertTrue(len(roots) <= shards)
    # This low threshold is kind arbitrary based on empirical data.
    self.assertTrue(len(roots) >= shards/2.0)

  def testDeepSplit(self):
    filenames = self.createFilesForDeepSplitTest()
    self.runDeepSplitTest(1, filenames)
    self.runDeepSplitTest(3, filenames)
    self.runDeepSplitTest(5, filenames)
    self.runDeepSplitTest(33, filenames)
    self.runDeepSplitTest(66, filenames)
    self.runDeepSplitTest(100, filenames)
    self.runDeepSplitTest(500, filenames)
    self.runDeepSplitTest(1000, filenames)

  def createFilesForShallowSplitTest(self):
    # Create 1000 zip files
    tmp_filenames = []
    for _ in range(1000):
      _, path = tempfile.mkstemp(text=True)
      tmp_filenames.append(path)
      with open(path, 'w') as f:
        for _ in range(10):
          f.write((string.ascii_letters + '\n')*100)
    self.__created_files.extend(tmp_filenames)
    return tmp_filenames

  def runShallowSplitTest(self, shards, filenames):
    roots = file_format_root.split(filenames, 'lines', shards)
    self.assertTrue(len(roots) <= shards)
    self.assertTrue(len(roots) >= shards*0.9)

  def testShallowSplit(self):
    filenames = self.createFilesForShallowSplitTest()
    self.runShallowSplitTest(1, filenames)
    self.runShallowSplitTest(3, filenames)
    self.runShallowSplitTest(5, filenames)
    self.runShallowSplitTest(33, filenames)
    # This will only generate 63 shards. Each shard is slightly bigger.
    # But this is OK because they are still even in size.
    self.runShallowSplitTest(66, filenames)
    self.runShallowSplitTest(100, filenames)
    self.runShallowSplitTest(500, filenames)
    self.runShallowSplitTest(1000, filenames)


if __name__ == '__main__':
  unittest.main()
