#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
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
#

"""Tests for google.appengine.ext.mapreduce.util."""


import unittest
from mapreduce import util


class TestHandler(object):
  """Test handler class."""

  def __call__(self, entity):
    pass

  def process(self, entity):
    pass


def test_handler_function(entity):
  """Empty test handler function."""
  pass


class TestHandlerWithArgs(object):
  """Test handler with argument in constructor."""

  def __init__(self, arg_unused):
    """Constructor."""
    pass

  def process(self, entity):
    """Empty process function."""
    pass


def test_handler_yield(entity):
  """Yielding handler function."""
  yield 1
  yield 2


class ForNameTest(unittest.TestCase):
  """Test util.for_name function."""

  def testClassName(self):
    """Test passing fq class name."""
    self.assertEquals(TestHandler, util.for_name("__main__.TestHandler"))

  def testFunctionName(self):
    """Test passing function name."""
    self.assertEquals(test_handler_function,
                      util.for_name("__main__.test_handler_function"))

  def testMethodName(self):
    """Test passing method name."""
    self.assertEquals(TestHandler.process,
                      util.for_name("__main__.TestHandler.process"))

  def testClassWithArgs(self):
    """Test passing method name of class with constructor args."""
    self.assertEquals(TestHandlerWithArgs.process,
                      util.for_name("__main__.TestHandlerWithArgs.process"))

  def testBadModule(self):
    """Tests when the module name is bogus."""
    try:
      util.for_name("this_is_a_bad_module_name.stuff")
    except ImportError, e:
      self.assertEquals(
          "Could not find 'stuff' on path 'this_is_a_bad_module_name'",
          str(e))
    else:
      self.fail("Did not raise exception")

  def testBadFunction(self):
    """Tests when the module name is good but the function is missing."""
    try:
      util.for_name("__main__.does_not_exist")
    except ImportError, e:
      self.assertEquals(
          "Could not find 'does_not_exist' on path '__main__'",
          str(e))
    else:
      self.fail("Did not raise exception")

  def testBadClass(self):
    """Tests when the class is found but the function name is missing."""
    try:
      util.for_name("__main__.TestHandlerWithArgs.missing")
    except ImportError, e:
      self.assertEquals(
          "Could not find 'missing' on path '__main__.TestHandlerWithArgs'",
          str(e))
    else:
      self.fail("Did not raise exception")

  def testGlobalName(self):
    """Tests when the name has no dots in it."""
    try:
      util.for_name("this_is_a_bad_module_name")
    except ImportError, e:
      self.assertTrue(str(e).startswith(
          "Could not find 'this_is_a_bad_module_name' on path "))
    else:
      self.fail("Did not raise exception")


class IsGeneratorFunctionTest(unittest.TestCase):
  """Test util.is_generator_function function."""

  def testGenerator(self):
    self.assertTrue(util.is_generator_function(test_handler_yield))

  def testNotGenerator(self):
    self.assertFalse(util.is_generator_function(test_handler_function))


class GetShortNameTest(unittest.TestCase):
  """Test util.get_short_name function."""

  def testGetShortName(self):
    self.assertEquals("blah", util.get_short_name("blah"))
    self.assertEquals("blah", util.get_short_name(".blah"))
    self.assertEquals("blah", util.get_short_name("__mmm__.blah"))
    self.assertEquals("blah", util.get_short_name("__mmm__.Krb.blah"))


if __name__ == "__main__":
  unittest.main()
