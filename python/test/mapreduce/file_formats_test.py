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




import base64
import os
import tempfile
import unittest
import zipfile

from google.appengine.api.files import file as files
from mapreduce import file_format_root


class FileFormatsTest(unittest.TestCase):
  """Blackbox tests for FileFormats and FileFormatRoot.

  Each testcase tests format iteration, (de)serialization, and (if supported)
  inputs splitting.
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

  def createTmp(self):
    _, filename = tempfile.mkstemp()
    self.__created_files.append(filename)
    return filename

  def assertEqualsAfterJson(self, expected, root):
    json_str = root.to_json_str()
    root = root.from_json_str(json_str)
    self.assertEquals(expected, root.next().read())
    return root

  def assertRaisesAfterJson(self, root):
    json_str = root.to_json_str()
    root = root.from_json_str(json_str)
    self.assertRaises(StopIteration, root.next)
    return root

  def createRoots(self, inputs, format_string, shards):
    return file_format_root.split(inputs, format_string, shards)

  def createSingleRoot(self, inputs, format_string):
    return self.createRoots(inputs, format_string, 1)[0]


class SimpleFileFormatsTest(FileFormatsTest):
  """Blackbox tests for each FileFormat."""

  def testLinesFormat(self):
    contents = [u'\u5de5\u4f5c\n',
                u'\u7761\u89c9\n',
                u'\u5403\u996d\n']
    filename = self.createTmp()
    with open(filename, 'w') as f:
      f.write(u''.join(contents).encode('utf32'))

    root = self.createSingleRoot([filename], 'lines(encoding=utf32)')
    for content in contents:
      root = self.assertEqualsAfterJson(content.encode('utf32'), root)
    self.assertRaisesAfterJson(root)

  def testBinaryFormat(self):
    filename = self.createTmp()
    with open(filename, 'w') as f:
      f.write('abcde')

    root = self.createSingleRoot([filename], 'bytes')
    root = self.assertEqualsAfterJson('abcde', root)
    self.assertRaisesAfterJson(root)

  def testBase64Format(self):
    content = 'abc\nabc'
    filename = self.createTmp()
    with open(filename, 'w') as f:
      f.write(content.encode('base64'))

    root = self.createSingleRoot([filename], 'base64')
    root = self.assertEqualsAfterJson('abc\nabc', root)
    self.assertRaisesAfterJson(root)

  def testZipFormat(self):
    filename = self.createTmp()
    myzip = zipfile.ZipFile(filename, 'w')
    myzip.writestr('a.txt', 'abcde')
    myzip.writestr('b.txt', 'higkl')
    myzip.close()

    root = self.createSingleRoot([filename], 'zip')
    root = self.assertEqualsAfterJson('abcde', root)
    root = self.assertEqualsAfterJson('higkl', root)
    self.assertRaisesAfterJson(root)

  def testZipFormatOneFileSplitting(self):
    contents = [char*1024 for char in 'abcdef']
    filename = self.createTmp()
    myzip = zipfile.ZipFile(filename, 'w')
    for content in contents:
      myzip.writestr('filename', content)
    myzip.close()

    # split to 2 shards.
    roots = self.createRoots([filename], 'zip', 2)
    self.assertTrue(len(roots) <= 2)
    root = roots[0]

    # Uneven split due to zip overheads.
    root = self.assertEqualsAfterJson(contents[0], root)
    root = self.assertEqualsAfterJson(contents[1], root)
    root = self.assertEqualsAfterJson(contents[2], root)
    root = self.assertEqualsAfterJson(contents[3], root)
    self.assertRaisesAfterJson(root)

    root = roots[1]
    root = self.assertEqualsAfterJson(contents[4], root)
    root = self.assertEqualsAfterJson(contents[5], root)
    self.assertRaisesAfterJson(root)

    # split to 3 shards.
    roots = self.createRoots([filename], 'zip', 3)
    self.assertTrue(len(roots) <= 3)
    root = roots[0]
    root = self.assertEqualsAfterJson(contents[0], root)
    root = self.assertEqualsAfterJson(contents[1], root)
    root = self.assertEqualsAfterJson(contents[2], root)
    self.assertRaisesAfterJson(root)

    root = roots[1]
    root = self.assertEqualsAfterJson(contents[3], root)
    root = self.assertEqualsAfterJson(contents[4], root)
    root = self.assertEqualsAfterJson(contents[5], root)
    self.assertRaisesAfterJson(root)

  def testZipFormatTwoFilesSplitting(self):
    """Split on two files.

    This is test worthy because one root covers part of both files."""
    def createFile(*contents):
      filenames = []
      for content in contents:
        filename = self.createTmp()
        filenames.append(filename)
        myzip = zipfile.ZipFile(filename, 'w')
        for f in content:
          myzip.writestr('filename', f)
        myzip.close()
      return filenames

    contents1 = [char*1024 for char in 'abd']
    contents2 = [char*1024 for char in 'j']
    filenames = createFile(contents1, contents2)

    roots = self.createRoots(filenames, 'zip', 3)
    self.assertTrue(len(roots) <= 3)

    root = roots[0]
    root = self.assertEqualsAfterJson(contents1[0], root)
    root = self.assertEqualsAfterJson(contents1[1], root)
    self.assertRaisesAfterJson(root)

    root = roots[1]
    root = self.assertEqualsAfterJson(contents1[2], root)
    root = self.assertEqualsAfterJson(contents2[0], root)
    self.assertRaisesAfterJson(root)


class NestedFileFormatsTest(FileFormatsTest):
  """Blackbox test for useful and common nested FileFormats."""

  def testNestedFormat1(self):
    """base64[zip[lines(encoding=utf32)]]."""
    contents1 = [u'\u5de5\u4f5c\n',
                 u'\u7761\u89c9\n',
                 u'\u5403\u996d\n']

    contents2 = [u'\u7761\u89c9\n',
                 u'\u5de5\u4f5c\n',
                 u'\u5403\u996d\n']

    def createFile(encoding='utf32'):
      # create two files with utf-32
      paths = []
      for contents in (contents1, contents2):
        path = self.createTmp()
        paths.append(path)
        with open(path, 'w') as f:
          f.write(u''.join(contents).encode(encoding))

      # zip them.
      zippath = self.createTmp()
      myzip = zipfile.ZipFile(zippath, 'w')
      for path in paths:
        myzip.write(path)

      # base64 encode the zip file.
      path = self.createTmp()
      with open(path, 'w') as f:
        with open(zippath) as myzip:
          f.write(base64.b64encode(myzip.read()))

      return path

    filename = createFile('utf32')
    root = self.createSingleRoot([filename],
                                 'base64[zip[lines(encoding=utf32)]]')
    for content in contents1:
      root = self.assertEqualsAfterJson(content.encode('utf32'), root)
    for content in contents2:
      root = self.assertEqualsAfterJson(content.encode('utf32'), root)
    self.assertRaisesAfterJson(root)


if __name__ == '__main__':
  unittest.main()
