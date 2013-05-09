#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""Testing mapreduce functionality end to end."""




import datetime
import random
import string
import unittest

from google.appengine.api import files
from google.appengine.api.files import records
from google.appengine.ext import db
from mapreduce import control
from mapreduce import handlers
from mapreduce import model
from mapreduce import output_writers
from mapreduce import test_support
from testlib import testutil
from google.appengine.ext import ndb

# pylint: disable=g-bad-name


def random_string(length):
  """Generate a random string of given length."""
  return "".join(
      random.choice(string.letters + string.digits) for _ in range(length))


class TestEntity(db.Model):
  """Test entity class."""
  int_property = db.IntegerProperty()
  dt = db.DateTimeProperty(default=datetime.datetime(2000, 1, 1))


class NdbTestEntity(ndb.Model):
  """Test entity class for NDB."""


class TestHandler(object):
  """Test handler which stores all processed entities keys.

  Properties:
    processed_entites: all processed entities.
  """

  processed_entites = []

  def __call__(self, entity):
    """Main handler process function.

    Args:
      entity: entity to process.
    """
    TestHandler.processed_entites.append(entity)

  @staticmethod
  def reset():
    """Clear processed_entites & reset delay to 0."""
    TestHandler.processed_entites = []


class SerializableHandler(object):
  """Handler that utilize serialization."""

  # The number of handler instances created.
  _instances_created = 0
  # The first few instances will keep raising errors.
  # This is to test that upon shard retry, shard creates a new handler.
  RAISE_ERRORS = 4
  # The first instance is created for validation and not used by any shard.
  TASKS_CONSUMED_BY_RETRY = RAISE_ERRORS - 1

  def __init__(self):
    self.count = 0
    self.__class__._instances_created += 1

  def __call__(self, entity):
    if self.__class__._instances_created <= self.RAISE_ERRORS:
      raise files.FinalizationError("Injected error.")
    # Increment the int property by one on every call.
    entity.int_property = self.count
    entity.put()
    self.count += 1

  @classmethod
  def reset(cls):
    cls._instances_created = 0


def test_handler_yield_key(entity):
  """Test handler which yields entity key."""
  yield entity.key()


def test_handler_yield_ndb_key(entity):
  """Test handler which yields entity key (NDB version)."""
  yield entity.key


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
    random_str = "".join(
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
    TestHandler.reset()
    TestOutputWriter.reset()
    self.original_slice_duration = handlers._SLICE_DURATION_SEC
    SerializableHandler.reset()

  def tearDown(self):
    handlers._SLICE_DURATION_SEC = self.original_slice_duration

  def testHandlerSerialization(self):
    """Test serializable handler works with MR and shard retry."""
    entity_count = 10

    for _ in range(entity_count):
      TestEntity(int_property=-1).put()

    # Force handler to serialize on every call.
    handlers._SLICE_DURATION_SEC = 0

    control.start_map(
        "test_map",
        __name__ + ".SerializableHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
        },
        shard_count=1,
        base_path="/mapreduce_base_path")

    task_run_counts = test_support.execute_until_empty(self.taskqueue)
    self.assertEquals(
        task_run_counts[handlers.MapperWorkerCallbackHandler],
        entity_count + 1 + SerializableHandler.TASKS_CONSUMED_BY_RETRY)
    vals = [e.int_property for e in TestEntity.all()]
    vals.sort()
    # SerializableHandler updates int_property to be incremental from 0 to 9.
    self.assertEquals(range(10), vals)

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

    test_support.execute_until_empty(self.taskqueue)
    self.assertEquals(entity_count, len(TestHandler.processed_entites))

  def testEntityQuery(self):
    entity_count = 1000

    for i in range(entity_count):
      TestEntity(int_property=i % 5).put()

    control.start_map(
        "test_map",
        __name__ + ".TestHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
            "filters": [("int_property", "=", 3),
                        # Test datetime can be json serialized.
                        ("dt", "=", datetime.datetime(2000, 1, 1))],
        },
        shard_count=4,
        base_path="/mapreduce_base_path")

    test_support.execute_until_empty(self.taskqueue)
    self.assertEquals(200, len(TestHandler.processed_entites))

  def testLotsOfNdbEntities(self):
    entity_count = 1000

    for i in range(entity_count):
      NdbTestEntity().put()

    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".TestHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + NdbTestEntity.__name__,
        },
        shard_count=4,
        base_path="/mapreduce_base_path")

    test_support.execute_until_empty(self.taskqueue)
    self.assertEquals(entity_count, len(TestHandler.processed_entites))

  def testInputReaderDedicatedParameters(self):
    entity_count = 100

    for i in range(entity_count):
      TestEntity().put()

    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".TestHandler",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "input_reader": {
                "entity_kind": __name__ + "." + TestEntity.__name__,
            },
        },
        shard_count=4,
        base_path="/mapreduce_base_path")

    test_support.execute_until_empty(self.taskqueue)
    self.assertEquals(entity_count, len(TestHandler.processed_entites))

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

    test_support.execute_until_empty(self.taskqueue)
    self.assertEquals(1, len(TestOutputWriter.file_contents))
    self.assertEquals(entity_count,
                      len(TestOutputWriter.file_contents.values()[0]))

  def testNdbOutputWriter(self):
    """End-to-end test with output writer."""
    entity_count = 1000

    for i in range(entity_count):
      NdbTestEntity().put()

    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".test_handler_yield_ndb_key",
        "mapreduce.input_readers.DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + NdbTestEntity.__name__,
        },
        shard_count=4,
        base_path="/mapreduce_base_path",
        output_writer_spec=__name__ + ".TestOutputWriter")

    test_support.execute_until_empty(self.taskqueue)
    self.assertEquals(1, len(TestOutputWriter.file_contents))
    self.assertEquals(entity_count,
                      len(TestOutputWriter.file_contents.values()[0]))

  def testRecordsReader(self):
    """End-to-end test for records reader."""
    input_file = files.blobstore.create()
    input_data = [str(i) for i in range(100)]

    with files.open(input_file, "a") as f:
      with records.RecordsWriter(f) as w:
        for record in input_data:
          w.write(record)
    files.finalize(input_file)
    input_file = files.blobstore.get_file_name(
        files.blobstore.get_blob_key(input_file))

    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".TestHandler",
        "mapreduce.input_readers.RecordsReader",
        {
            "file": input_file
        },
        shard_count=4,
        base_path="/mapreduce_base_path")

    test_support.execute_until_empty(self.taskqueue)
    self.assertEquals(100, len(TestHandler.processed_entites))

  def testHugeTaskPayloadTest(self):
    """Test map job with huge parameter values."""
    input_file = files.blobstore.create()
    input_data = [str(i) for i in range(100)]

    with files.open(input_file, "a") as f:
      with records.RecordsWriter(f) as w:
        for record in input_data:
          w.write(record)
    files.finalize(input_file)
    input_file = files.blobstore.get_file_name(
        files.blobstore.get_blob_key(input_file))

    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".TestHandler",
        "mapreduce.input_readers.RecordsReader",
        {
            "file": input_file,
            # the parameter will be compressed and should fit into
            # taskqueue payload
            "huge_parameter": "0" * 200000, # 200K
        },
        shard_count=4,
        base_path="/mapreduce_base_path")

    test_support.execute_until_empty(self.taskqueue)
    self.assertEquals(100, len(TestHandler.processed_entites))
    self.assertEquals([], model._HugeTaskPayload.all().fetch(100))

  def testHugeTaskUseDatastore(self):
    """Test map job with huge parameter values."""
    input_file = files.blobstore.create()
    input_data = [str(i) for i in range(100)]

    with files.open(input_file, "a") as f:
      with records.RecordsWriter(f) as w:
        for record in input_data:
          w.write(record)
    files.finalize(input_file)
    input_file = files.blobstore.get_file_name(
        files.blobstore.get_blob_key(input_file))

    mapreduce_id = control.start_map(
        "test_map",
        __name__ + ".TestHandler",
        "mapreduce.input_readers.RecordsReader",
        {
            "file": input_file,
            # the parameter can't be compressed and wouldn't fit into
            # taskqueue payload
            "huge_parameter": random_string(900000)
        },
        shard_count=4,
        base_path="/mapreduce_base_path")

    test_support.execute_until_empty(self.taskqueue)
    self.assertEquals(100, len(TestHandler.processed_entites))
    self.assertEquals([], model._HugeTaskPayload.all().fetch(100))


if __name__ == "__main__":
  unittest.main()
