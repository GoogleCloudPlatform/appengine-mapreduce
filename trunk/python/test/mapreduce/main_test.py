#!/usr/bin/env python
#
# Copyright 2010 Google Inc.
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





import re
import unittest

from mapreduce import main


class MainTest(unittest.TestCase):
  """Tests for main module."""

  def testCreateApplication(self):
    """Test create_application function.

    Verify that it gets all import right.
    """
    main.create_application()

  def testRedirectRegularExpression(self):
    """Verify that depth of mapped path does not impact redirect RE matching."""
    basepath = '/mapreduce'
    extensions = {'': True, '/': True, '/resource.css': False, '/status': False}
    self.redirectMatchesExtensions(basepath, extensions)
    basepath = '/_ah/mapper'
    self.redirectMatchesExtensions(basepath, extensions)
    basepath = '/contains/status/but/does/not/end/in/it'
    self.redirectMatchesExtensions(basepath, extensions)

  def redirectMatchesExtensions(self, basepath, extensions):
    """Iterates over extensions to assert whether or not regex should match."""
    for ext, should_match in extensions.iteritems():
      if should_match:
        self.assertMatches(basepath+ext, main.REDIRECT_RE)
      else:
        self.assertNotMatches(basepath+ext, main.REDIRECT_RE)

  def assertMatches(self, text, regex):
    """Corresponding re.match version of assertRegexpMatches."""
    m = re.match(regex, text)
    if not m:
      self.fail('Text %s was not matched by pattern %s.' % (text, regex))

  def assertNotMatches(self, text, regex):
    """Corresponding re.match version of assertNotRegexpMatches."""
    m = re.match(regex, text)
    if m:
      self.fail('Text %s was matched by pattern %s as %s.'
                % (text, regex, m.group(0)))

if __name__ == '__main__':
  unittest.main()
