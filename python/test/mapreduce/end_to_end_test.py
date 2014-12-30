#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.

"""Testing mapreduce functionality end to end."""



# Using opensource naming conventions, pylint: disable=g-bad-name

import datetime
import logging
import random
import string
import unittest

from google.appengine.ext import db

from google.appengine.ext import ndb

# pylint: disable=g-direct-third-party-import
from mapreduce import context
from mapreduce import control
from mapreduce import handlers
from mapreduce import input_readers
from mapreduce import model
from mapreduce import output_writers
from mapreduce import parameters
from mapreduce import records
from mapreduce import test_support
from mapreduce.tools import gcs_file_seg_reader
from testlib import testutil


# pylint: disable=g-import-not-at-top
try:
  import cloudstorage
  # In 25 runtime, the above code will be scrubbed to import the stub version
  # of cloudstorage. All occurences of the following if condition in MR
  # codebase is to tell it apart.
  # TODO(user): Remove after 25 runtime MR is abondoned.
  if hasattr(cloudstorage, "_STUB"):
    cloudstorage = None
  from cloudstorage import storage_api
except ImportError:
  cloudstorage = None  # CloudStorage library not available

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

  _next_instance_id = 0
  # The first few instances will keep raising errors.
  # This is to test that upon shard retry, shard creates a new handler.
  INSTANCES_THAT_RAISE_ERRORS = 3
  # The first instance is created for validation and not used by any shard.
  FAILURES_INDUCED_BY_INSTANCE = INSTANCES_THAT_RAISE_ERRORS - 1

  def __init__(self):
    self.count = 0
    self.instance = self.__class__._next_instance_id
    self.__class__._next_instance_id += 1

  def __call__(self, entity):
    if self.instance < self.INSTANCES_THAT_RAISE_ERRORS:
      raise cloudstorage.FatalError("Injected error.")
    # Increment the int property by one on every call.
    entity.int_property = self.count
    entity.put()
    self.count += 1

  @classmethod
  def reset(cls):
    cls._next_instance_id = 0


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
  def finalize_job(cls, mapreduce_state):
    pass

  @classmethod
  def create(cls, mr_spec, shard_number, shard_attempt, _writer_state=None):
    random_str = "".join(
        random.choice(string.ascii_uppercase + string.digits)
        for _ in range(64))
    cls.file_contents[random_str] = []
    return cls(random_str)

  def to_json(self):
    return {"filename": self.filename}

  @classmethod
  def from_json(cls, json_dict):
    return cls(json_dict["filename"])

  def write(self, data):
    self.file_contents[self.filename].append(data)

  def finalize(self, ctx, shard_number):
    pass


class EndToEndTest(testutil.HandlerTestBase):
  """Test mapreduce functionality end to end."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    TestHandler.reset()
    TestOutputWriter.reset()
    self.original_slice_duration = parameters.config._SLICE_DURATION_SEC
    SerializableHandler.reset()

  def tearDown(self):
    parameters.config._SLICE_DURATION_SEC = self.original_slice_duration

  def testHandlerSerialization(self):
    """Test serializable handler works with MR and shard retry."""
    entity_count = 10

    for _ in range(entity_count):
      TestEntity(int_property=-1).put()

    # Force handler to serialize on every call.
    parameters.config._SLICE_DURATION_SEC = 0

    control.start_map(
        "test_map",
        __name__ + ".SerializableHandler",
        input_readers.__name__ + ".DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
        },
        shard_count=1,
        base_path="/mapreduce_base_path")

    task_run_counts = test_support.execute_until_empty(self.taskqueue)
    self.assertEquals(
        task_run_counts[handlers.MapperWorkerCallbackHandler],
        # Shard retries + one per entity + one to exhaust input reader + one for
        # finalization.
        SerializableHandler.FAILURES_INDUCED_BY_INSTANCE + entity_count + 1 + 1)
    vals = [e.int_property for e in TestEntity.all()]
    vals.sort()
    # SerializableHandler updates int_property to be incremental from 0 to 9.
    self.assertEquals(range(10), vals)

  def testLotsOfEntities(self):
    entity_count = 1000

    for _ in range(entity_count):
      TestEntity().put()

    control.start_map(
        "test_map",
        __name__ + ".TestHandler",
        input_readers.__name__ + ".DatastoreInputReader",
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
        input_readers.__name__ + ".DatastoreInputReader",
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

    for _ in range(entity_count):
      NdbTestEntity().put()

    control.start_map(
        "test_map",
        __name__ + ".TestHandler",
        input_readers.__name__ + ".DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + NdbTestEntity.__name__,
        },
        shard_count=4,
        base_path="/mapreduce_base_path")

    test_support.execute_until_empty(self.taskqueue)
    self.assertEquals(entity_count, len(TestHandler.processed_entites))

  def testInputReaderDedicatedParameters(self):
    entity_count = 100

    for _ in range(entity_count):
      TestEntity().put()

    control.start_map(
        "test_map",
        __name__ + ".TestHandler",
        input_readers.__name__ + ".DatastoreInputReader",
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

    for _ in range(entity_count):
      TestEntity().put()

    control.start_map(
        "test_map",
        __name__ + ".test_handler_yield_key",
        input_readers.__name__ + ".DatastoreInputReader",
        {
            "entity_kind": __name__ + "." + TestEntity.__name__,
        },
        shard_count=4,
        base_path="/mapreduce_base_path",
        output_writer_spec=__name__ + ".TestOutputWriter")

    test_support.execute_until_empty(self.taskqueue)
    self.assertEquals(entity_count,
                      sum(map(len, TestOutputWriter.file_contents.values())))

  def testRecordsReader(self):
    """End-to-end test for records reader."""
    input_data = [str(i) for i in range(100)]

    bucket_name = "testbucket"
    test_filename = "testfile"
    full_filename = "/%s/%s" % (bucket_name, test_filename)

    with cloudstorage.open(full_filename, mode="w") as f:
      with records.RecordsWriter(f) as w:
        for record in input_data:
          w.write(record)

    control.start_map(
        "test_map",
        __name__ + ".TestHandler",
        input_readers.__name__ + ".GoogleCloudStorageRecordInputReader",
        {
            "input_reader": {
                "bucket_name": bucket_name,
                "objects": [test_filename]
            }
        },
        shard_count=4,
        base_path="/mapreduce_base_path")

    test_support.execute_until_empty(self.taskqueue)
    self.assertEquals(100, len(TestHandler.processed_entites))

  def testHugeTaskPayloadTest(self):
    """Test map job with huge parameter values."""
    input_data = [str(i) for i in range(100)]

    bucket_name = "testbucket"
    test_filename = "testfile"
    full_filename = "/%s/%s" % (bucket_name, test_filename)

    with cloudstorage.open(full_filename, mode="w") as f:
      with records.RecordsWriter(f) as w:
        for record in input_data:
          w.write(record)

    control.start_map(
        "test_map",
        __name__ + ".TestHandler",
        input_readers.__name__ + ".GoogleCloudStorageRecordInputReader",
        {
            "input_reader": {
                "bucket_name": bucket_name,
                "objects": [test_filename],
                # the parameter will be compressed and should fit into
                # taskqueue payload
                "huge_parameter": "0" * 200000,  # 200K
            }
        },
        shard_count=4,
        base_path="/mapreduce_base_path")

    test_support.execute_until_empty(self.taskqueue)
    self.assertEquals(100, len(TestHandler.processed_entites))
    self.assertEquals([], model._HugeTaskPayload.all().fetch(100))

  def testHugeTaskUseDatastore(self):
    """Test map job with huge parameter values."""
    input_data = [str(i) for i in range(100)]

    bucket_name = "testbucket"
    test_filename = "testfile"
    full_filename = "/%s/%s" % (bucket_name, test_filename)

    with cloudstorage.open(full_filename, mode="w") as f:
      with records.RecordsWriter(f) as w:
        for record in input_data:
          w.write(record)

    control.start_map(
        "test_map",
        __name__ + ".TestHandler",
        input_readers.__name__ + ".GoogleCloudStorageRecordInputReader",
        {
            "input_reader": {
                "bucket_name": bucket_name,
                "objects": [test_filename],
                # the parameter can't be compressed and wouldn't fit into
                # taskqueue payload
                "huge_parameter": random_string(900000)
            }
        },
        shard_count=4,
        base_path="/mapreduce_base_path")

    test_support.execute_until_empty(self.taskqueue)
    self.assertEquals(100, len(TestHandler.processed_entites))
    self.assertEquals([], model._HugeTaskPayload.all().fetch(100))


class GCSOutputWriterTestBase(testutil.CloudStorageTestBase):
  """Base class for all GCS output writer tests."""

  def setUp(self):
    super(GCSOutputWriterTestBase, self).setUp()
    self.original_slice_duration = parameters.config._SLICE_DURATION_SEC
    self.original_block_size = storage_api.StreamingBuffer._blocksize
    # Use this to adjust what is printed for debugging purpose.
    logging.getLogger().setLevel(logging.CRITICAL)
    self.writer_cls = output_writers._GoogleCloudStorageOutputWriter

    # Populate datastore with inputs.
    entity_count = 30
    for i in range(entity_count):
      TestEntity(int_property=i).put()

    # Make slice short.
    parameters.config._SLICE_DURATION_SEC = 1
    # 5 items per second. This effectively terminates a slice after
    # processing 5 items.
    self.processing_rate = 5

  def tearDown(self):
    storage_api.StreamingBuffer._blocksize = self.original_block_size
    parameters.config._SLICE_DURATION_SEC = self.original_slice_duration
    super(GCSOutputWriterTestBase, self).tearDown()


class GCSOutputWriterNoDupModeTest(GCSOutputWriterTestBase):
  """Test GCS output writer slice recovery."""

  def testSliceRecoveryWithForcedFlushing(self):
    # Force a flush to GCS on every character wrote.
    storage_api.StreamingBuffer._blocksize = 1
    storage_api.StreamingBuffer._flushsize = 1

    mr_id = control.start_map(
        "test_map",
        __name__ + ".FaultyHandler",
        input_readers.__name__ + ".DatastoreInputReader",
        {
            "input_reader": {
                "entity_kind": __name__ + "." + TestEntity.__name__,
            },
            "output_writer": {
                "bucket_name": "bucket",
                "no_duplicate": True,
            },
            "processing_rate": self.processing_rate,
        },
        output_writer_spec=(output_writers.__name__ +
                            "._GoogleCloudStorageOutputWriter"),
        shard_count=1)

    test_support.execute_until_empty(self.taskqueue)
    mr_state = model.MapreduceState.get_by_job_id(mr_id)
    # Verify MR is successful.
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS,
                     mr_state.result_status)

    # Read output info from shard states.
    shard_state = model.ShardState.find_all_by_mapreduce_state(mr_state).next()
    writer_state = shard_state.writer_state
    last_seg_index = writer_state[self.writer_cls._LAST_SEG_INDEX]
    seg_prefix = writer_state[self.writer_cls._SEG_PREFIX]

    # Verify we have 5 segs.
    self.assertEqual(4, last_seg_index)

    self._assertOutputEqual(seg_prefix, last_seg_index)

    # Check there are indeed duplicated data.
    f1 = set([line for line in cloudstorage.open(seg_prefix + "0")])
    f2 = set([line for line in cloudstorage.open(seg_prefix + "1")])
    common = f1.intersection(f2)
    self.assertEqual(set(["10\n", "11\n"]), common)

  def testSliceRecoveryWithFrequentFlushing(self):
    # Force a flush to GCS on every 8 chars.
    storage_api.StreamingBuffer._blocksize = 8
    storage_api.StreamingBuffer._flushsize = 8

    mr_id = control.start_map(
        "test_map",
        __name__ + ".FaultyHandler",
        input_readers.__name__ + ".DatastoreInputReader",
        {
            "input_reader": {
                "entity_kind": __name__ + "." + TestEntity.__name__,
            },
            "output_writer": {
                "bucket_name": "bucket",
                "no_duplicate": True,
            },
            "processing_rate": self.processing_rate,
        },
        output_writer_spec=(output_writers.__name__ +
                            "._GoogleCloudStorageOutputWriter"),
        shard_count=1)

    test_support.execute_until_empty(self.taskqueue)
    mr_state = model.MapreduceState.get_by_job_id(mr_id)
    # Verify MR is successful.
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS,
                     mr_state.result_status)

    # Read output info from shard states.
    shard_state = model.ShardState.find_all_by_mapreduce_state(mr_state).next()
    writer_state = shard_state.writer_state
    last_seg_index = writer_state[self.writer_cls._LAST_SEG_INDEX]
    seg_prefix = writer_state[self.writer_cls._SEG_PREFIX]

    # Verify we have 5 segs.
    self.assertEqual(4, last_seg_index)

    self._assertOutputEqual(seg_prefix, last_seg_index)

  def testSliceRecoveryWithNoFlushing(self):
    # Flushing is done every 256K, which means never until slice recovery.
    mr_id = control.start_map(
        "test_map",
        __name__ + ".FaultyHandler",
        input_readers.__name__ + ".DatastoreInputReader",
        {
            "input_reader": {
                "entity_kind": __name__ + "." + TestEntity.__name__,
            },
            "output_writer": {
                "bucket_name": "bucket",
                "no_duplicate": True,
            },
            "processing_rate": self.processing_rate,
        },
        output_writer_spec=(output_writers.__name__ +
                            "._GoogleCloudStorageOutputWriter"),
        shard_count=1)

    test_support.execute_until_empty(self.taskqueue)
    mr_state = model.MapreduceState.get_by_job_id(mr_id)
    # Verify MR is successful.
    self.assertEqual(model.MapreduceState.RESULT_SUCCESS,
                     mr_state.result_status)

    # Read output info from shard states.
    shard_state = model.ShardState.find_all_by_mapreduce_state(mr_state).next()
    writer_state = shard_state.writer_state
    last_seg_index = writer_state[self.writer_cls._LAST_SEG_INDEX]
    seg_prefix = writer_state[self.writer_cls._SEG_PREFIX]

    # Verify we have 5 segs.
    self.assertEqual(4, last_seg_index)

    self._assertOutputEqual(seg_prefix, last_seg_index)

  def _assertOutputEqual(self, seg_prefix, last_seg_index):
    # Read back outputs.
    reader = gcs_file_seg_reader._GCSFileSegReader(seg_prefix, last_seg_index)
    result = ""
    while True:
      tmp = reader.read(n=100)
      if not tmp:
        break
      result += tmp

    # Verify output has no duplicates.
    expected = ""
    for i in range(30):
      expected += "%s\n" % i
    self.assertEqual(expected, result)


class FaultyHandler(object):

  def __init__(self):
    self.slice_count = 0

  # pylint: disable=unused-argument
  def __setstate__(self, state):
    # Reset at beginning of each slice.
    self.slice_count = 0

  def __call__(self, entity):
    self.slice_count += 1
    yield "%s\n" % entity.int_property
    slice_id = context.get()._shard_state.slice_id
    # Raise exception when processing the 2 item in a slice every 3 slices.
    if (self.slice_count == 2 and
        (slice_id + 1) % 3 == 0):
      raise Exception("Intentionally raise an exception")


if __name__ == "__main__":
  unittest.main()
