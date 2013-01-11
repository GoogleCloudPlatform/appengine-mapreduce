#!/usr/bin/env python
# coding: utf-8
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




import unittest

import mapreduce.file_format_parser as parser


class FileFormatParserTest(unittest.TestCase):
  """Test Parser properly parses various format strings."""

  def runTest(self, format_string):
    return ' '.join([unicode(f) for f in parser.parse(format_string)])

  def testFormats(self):
    self.assertEquals(
        'lines',
        self.runTest('lines'))
    self.assertEquals(
        'base64 zip lines',
        self.runTest('base64[zip[lines]]'))
    self.assertEquals(
        'csv(delimiter=.) lines(encoding=utf8)',
        self.runTest('csv(delimiter=.,)[lines( encoding= utf8)]'))

  def testUnicode(self):
    self.assertEquals(
        u'csv(delimiter=工)',
        self.runTest(u'csv(delimiter=工)'))

  def testEscape(self):
    self.assertEquals(
        'base64 zip csv(delimiter=\',encoding=utf-8)',
        self.runTest('base64[zip[csv(delimiter=\', encoding=utf-8)]]'))
    self.assertEquals(
        'csv(delimiter=,,encoding=utf-8)',
        # pylint: disable-msg=W1401
        self.runTest('csv(delimiter=\,, encoding=utf-8)'))
    self.assertEquals(
        r'csv(delimiter=\,encoding=utf-8)',
        self.runTest(r'csv(delimiter=\\, encoding=utf-8)'))

  def assertParseRaise(self, format_string):
    self.assertRaises(ValueError, parser.parse, format_string)

  def testValidation(self):
    self.assertParseRaise('base64(')
    self.assertParseRaise('csv(delimiter=1, delimieter=2)')
    self.assertParseRaise('csv(foo=1)')
    self.assertParseRaise('csv[zip]]')
    self.assertParseRaise('csv[[zip]')
    self.assertParseRaise('csv(delimiter=1 encoding=2)')
    self.assertParseRaise('f*f(delimiter=1)')


if __name__ == '__main__':
  unittest.main()
