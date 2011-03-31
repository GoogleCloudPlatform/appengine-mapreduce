#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""Testing mapreduce functionality end to end."""




import random
import string
import unittest

from google.appengine.ext import db
from mapreduce import control
from mapreduce import output_writers
from testlib import testutil


class TestEntity(db.Model):
  """Test entity class."""


class TestHandler(object):
  """Test handler which stores all processed entities keys.

  Properties:
    processed_keys: all keys of processed entities.
  """

  processed_keys = []

  def __call__(self, entity):
    """Main handler process function.

    Args:
      entity: entity to process.
    """
    TestHandler.processed_keys.append(str(entity.key()))

  @staticmethod
  def reset():
    """Clear processed_keys & reset delay to 0."""
    TestHandler.processed_keys = []


def test_handler_yield_key(entity):
  """Test handler which yields entity key."""
  yield entity.key()


class TestOutputWriter(output_writers.OutputWriter):
  """Test output writer."""

  file_contents = {}

  def __init__(self, filename):
    self.filename = filename

  @classmethod
  def reset(cls):
    cls.file_contents = {}

  @classmethod
  def validate(cls, mapper_spec):
    pass

  @classmethod
  def init_job(cls, mapreduce_state):
    random_str = ''.join(
        random.choice(string.ascii_uppercase + string.digits)
        for _ in range(64))
    mapreduce_state.mapreduce_spec.params["writer_filename"] = random_str
    cls.file_contents[random_str] = []

  @classmethod
  def finalize_job(cls, mapreduce_state):
    pass

  @classmethod
  def create(cls, mapreduce_state, shard_number):
    return cls(mapreduce_state.mapreduce_spec.params["writer_filename"])

  def to_json(self):
    return {"filename": self.filename}

  @classmethod
  def from_json(cls, json_dict):
    return cls(json_dict["filename"])

  def write(self, data, ctx):
    self.file_contents[self.filename].append(data)

  def finalize(self, ctx, shard_number):
    pass


class EndToEndTest(testutil.HandlerTestBase):
  """Test mapreduce functionality end to end."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    TestOutputWriter.reset()

  def testLotsOfEntities(self):
    entity_count = 1000

    for i in range(entity_count):
      TestEntity().put()

    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".TestHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
        },
        shard_count=4,
        base_path="/mapreduce_base_path")

    testutil.execute_until_empty(self.taskqueue)
    self.assertEquals(entity_count, len(TestHandler.processed_keys))

  def testOutputWriter(self):
    """End-to-end test with output writer."""
    entity_count = 1000

    for i in range(entity_count):
      TestEntity().put()

    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".test_handler_yield_key",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
        },
        shard_count=4,
        base_path="/mapreduce_base_path",
        output_writer_spec=__name__ + ".TestOutputWriter")

    testutil.execute_until_empty(self.taskqueue)
    self.assertEquals(1, len(TestOutputWriter.file_contents))
    self.assertEquals(entity_count,
                      len(TestOutputWriter.file_contents.values()[0]))


if __name__ == '__main__':
  unittest.main()
