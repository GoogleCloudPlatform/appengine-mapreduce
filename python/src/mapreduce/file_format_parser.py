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

"""Define file format string Parser."""



__all__ = ['parse']

import re
import tokenize

from mapreduce.file_formats import FORMATS


def parse(format_string):
  """Parse format string.

  Args:
    format_string: format_string from MapReduce FileInputReader.

  Returns:
    a list of _FileFormat objects.
  """
  return _FileFormatParser(_FileFormatTokenizer(format_string)).parse()


class _FileFormatParser(object):
  """Parses a format string according to the following grammar.

  In Python's modified BNF notation.
  format_string ::= parameterized_format ( “[“ parameterized_format “]” )*
  parameterized_format ::= format [ format_parameters ]
  format_parameters ::= “(“ format_paramter (“,” format_parameter )* “)”
  format_parameter ::= format_specific_parameter “=” parameter_value
  format ::= (<letter>|<number>)+
  parameter_value ::= (<letter>|<number>|<punctuation>)+
  format_specific_parameter ::= (<letter>|<number>)+
  """

  def __init__(self, tokenizer):
    self._formats = []
    self._tokenizer = tokenizer

  def _add_format(self, format_name, arguments):
    if format_name not in FORMATS:
      raise ValueError('Invalid format %s.' % format_name)
    format_cls = FORMATS[format_name]
    for k in arguments:
      if k not in format_cls.ARGUMENTS:
        raise ValueError('Invalid argument %s for format %s' %
                         (k, format_name))
    self._formats.append(format_cls(**arguments))

  def parse(self):
    self._parse_format_string()

    if self._tokenizer.remainder():
      raise ValueError('Extra chars after index -%d' %
                       self._tokenizer.remainder())
    return self._formats

  def _parse_format_string(self):
    """Parse format_string."""
    self._parse_parameterized_format()
    if self._tokenizer.consume_if('['):
      self._parse_format_string()
      self._tokenizer.consume(']')

  def _validate_string(self, text):
    """Validate a string is composed of valid characters."""
    if not re.match(tokenize.Name, text):
      raise ValueError('%s should only contain ascii letters or digits.' %
                       text)

  def _parse_parameterized_format(self):
    """Parse parameterized_format."""
    if not self._tokenizer.remainder():
      return

    format_name = self._tokenizer.next()
    self._validate_string(format_name)

    arguments = {}

    if self._tokenizer.consume_if('('):
      arguments = self._parse_format_parameters()
      self._tokenizer.consume(')')

    self._add_format(format_name, arguments)

  def _parse_format_parameters(self):
    """Parse format_parameters."""
    arguments = {}
    comma_exist = True
    while self._tokenizer.peek() not in ')]':
      if not comma_exist:
        raise ValueError('Arguments should be separated by comma at index %d.'
                         % self._tokenizer.index())
      key = self._tokenizer.next()
      self._validate_string(key)
      self._tokenizer.consume('=')
      value = self._tokenizer.next()
      comma_exist = self._tokenizer.consume_if(',')
      if key in arguments:
        raise ValueError('Argument %s defined more than once.' % key)
      arguments[key] = value
    return arguments


class _FileFormatTokenizer(object):
  """Tokenize a user supplied format string.

  A token is either a special character or a group of characters between
  two special characters or the beginning or the end of format string.
  Escape character can be used to escape special characters and itself.
  """

  SPECIAL_CHARS = '[]()=,'
  ESCAPE_CHAR = '\\'

  def __init__(self, format_string):
    self._index = 0
    self._format_string = format_string

  def next(self):
    return self._next().strip()

  def _next(self):
    escaped = False
    token = ''
    while self.remainder():
      char = self._format_string[self._index]
      if char == self.ESCAPE_CHAR:
        if escaped:
          token += char
          self._index += 1
          escaped = False
        else:
          self._index += 1
          escaped = True
      elif char in self.SPECIAL_CHARS and not escaped:
        if token:
          return token
        else:
          self._index += 1
          return char
      else:
        escaped = False
        self._index += 1
        token += char
    return token

  def consume(self, expected_token):
    token = self.next()
    if token != expected_token:
      raise ValueError('Expect "%s" but got "%s" at offset %d' %
                       (expected_token, token, self._index))

  def consume_if(self, token):
    if self.peek() == token:
      self.consume(token)
      return True
    return False

  def peek(self):
    token = self._next()
    self._index -= len(token)
    return token.strip()

  def remainder(self):
    return len(self._format_string) - self._index

  def index(self):
    return self._index
