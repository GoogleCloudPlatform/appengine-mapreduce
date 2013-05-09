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




import os
from mapreduce.lib import simplejson
import shutil
import tempfile
import time
import unittest

from google.appengine.api import yaml_errors
from google.appengine.ext import db
from mapreduce import errors
from mapreduce import handlers
from mapreduce import status
from testlib import testutil
from mapreduce import test_support
from mapreduce import mock_webapp


class TestKind(db.Model):
  """Used for testing."""

  foobar = db.StringProperty(default="meep")


def TestMap(entity):
  """Used for testing."""
  pass


class MapreduceYamlTest(unittest.TestCase):
  """Testing mapreduce.yaml-related functionality."""

  def set_up_directory_tree(self, dir_tree_contents):
    """Create directory tree from dict of path:contents entries."""
    for full_path, contents in dir_tree_contents.iteritems():
      dir_name = os.path.dirname(full_path)
      if not os.path.isdir(dir_name):
        os.makedirs(dir_name)
      f = open(full_path, 'w')
      f.write(contents)
      f.close()

  def setUp(self):
    """Initialize temporary application variable."""
    self.tempdir = tempfile.mkdtemp()

  def tearDown(self):
    """Remove temporary application directory."""
    if self.tempdir:
      shutil.rmtree(self.tempdir)

  def testFindYamlFile(self):
    """Test if mapreduce.yaml can be found with different app/library trees."""
    test_status = os.path.join(self.tempdir, "library_root", "google",
                               "appengine", "ext", "mapreduce", "status.py")
    test_mapreduce_yaml = os.path.join(self.tempdir, "application_root",
                                       "mapreduce.yaml")
    test_dict = {
        test_status: "test",
        test_mapreduce_yaml: "test",
    }
    self.set_up_directory_tree(test_dict)
    os.chdir(os.path.dirname(test_mapreduce_yaml))
    yaml_loc = status.find_mapreduce_yaml(status_file=test_status)
    self.assertEqual(test_mapreduce_yaml, yaml_loc)

  def testFindYamlFileSameTree(self):
    """Test if mapreduce.yaml can be found with the same app/library tree."""
    test_status = os.path.join(self.tempdir, "application_root", "google",
                               "appengine", "ext", "mapreduce", "status.py")
    test_mapreduce_yaml = os.path.join(self.tempdir, "application_root",
                                       "mapreduce.yaml")
    test_dict = {
        test_status: "test",
        test_mapreduce_yaml: "test",
    }
    self.set_up_directory_tree(test_dict)
    os.chdir(os.path.dirname(test_mapreduce_yaml))
    yaml_loc = status.find_mapreduce_yaml(status_file=test_status)
    self.assertEqual(test_mapreduce_yaml, yaml_loc)

  def testParseEmptyFile(self):
    """Parsing empty mapreduce.yaml file."""
    self.assertRaises(errors.BadYamlError,
                      status.parse_mapreduce_yaml,
                      "")

  def testParse(self):
    """Parsing a single document in mapreduce.yaml."""
    mr_yaml = status.parse_mapreduce_yaml(
        "mapreduce:\n"
        "- name: Mapreduce1\n"
        "  mapper:\n"
        "    handler: Handler1\n"
        "    input_reader: Reader1\n"
        "    params_validator: Validator1\n"
        "    params:\n"
        "    - name: entity_kind\n"
        "      default: Kind1\n"
        "    - name: human_supplied1\n"
        "    - name: human_supplied2\n"
        "- name: Mapreduce2\n"
        "  mapper:\n"
        "    handler: Handler2\n"
        "    input_reader: Reader2\n")

    self.assertTrue(mr_yaml)
    self.assertEquals(2, len(mr_yaml.mapreduce))

    self.assertEquals("Mapreduce1", mr_yaml.mapreduce[0].name)
    self.assertEquals("Handler1", mr_yaml.mapreduce[0].mapper.handler)
    self.assertEquals("Reader1", mr_yaml.mapreduce[0].mapper.input_reader)
    self.assertEquals("Validator1",
                      mr_yaml.mapreduce[0].mapper.params_validator)
    self.assertEquals(3, len(mr_yaml.mapreduce[0].mapper.params))
    self.assertEquals("entity_kind", mr_yaml.mapreduce[0].mapper.params[0].name)
    self.assertEquals("Kind1", mr_yaml.mapreduce[0].mapper.params[0].default)
    self.assertEquals("human_supplied1",
                      mr_yaml.mapreduce[0].mapper.params[1].name)
    self.assertEquals("human_supplied2",
                      mr_yaml.mapreduce[0].mapper.params[2].name)

    self.assertEquals("Mapreduce2", mr_yaml.mapreduce[1].name)
    self.assertEquals("Handler2", mr_yaml.mapreduce[1].mapper.handler)
    self.assertEquals("Reader2", mr_yaml.mapreduce[1].mapper.input_reader)

  def testParseOutputWriter(self):
    """Parsing a single document in mapreduce.yaml with output writer."""
    mr_yaml = status.parse_mapreduce_yaml(
        "mapreduce:\n"
        "- name: Mapreduce1\n"
        "  mapper:\n"
        "    handler: Handler1\n"
        "    input_reader: Reader1\n"
        "    output_writer: Writer1\n"
        )

    self.assertTrue(mr_yaml)
    self.assertEquals(1, len(mr_yaml.mapreduce))

    self.assertEquals("Mapreduce1", mr_yaml.mapreduce[0].name)
    self.assertEquals("Handler1", mr_yaml.mapreduce[0].mapper.handler)
    self.assertEquals("Reader1", mr_yaml.mapreduce[0].mapper.input_reader)
    self.assertEquals("Writer1", mr_yaml.mapreduce[0].mapper.output_writer)

  def testParseMissingRequiredAttrs(self):
    """Test parsing with missing required attributes."""
    self.assertRaises(errors.BadYamlError,
                      status.parse_mapreduce_yaml,
                      "mapreduce:\n"
                      "- name: Mapreduce1\n"
                      "  mapper:\n"
                      "    handler: Handler1\n")
    self.assertRaises(errors.BadYamlError,
                      status.parse_mapreduce_yaml,
                      "mapreduce:\n"
                      "- name: Mapreduce1\n"
                      "  mapper:\n"
                      "    input_reader: Reader1\n")

  def testBadValues(self):
    """Tests when some yaml values are of the wrong type."""
    self.assertRaises(errors.BadYamlError,
                      status.parse_mapreduce_yaml,
                      "mapreduce:\n"
                      "- name: Mapreduce1\n"
                      "  mapper:\n"
                      "    handler: Handler1\n"
                      "    input_reader: Reader1\n"
                      "    params:\n"
                      "    - name: $$Invalid$$\n")

  def testMultipleDocuments(self):
    """Tests when multiple documents are present."""
    self.assertRaises(errors.BadYamlError,
                      status.parse_mapreduce_yaml,
                      "mapreduce:\n"
                      "- name: Mapreduce1\n"
                      "  mapper:\n"
                      "    handler: Handler1\n"
                      "    input_reader: Reader1\n"
                      "---")

  def testOverlappingNames(self):
    """Tests when there are jobs with the same name."""
    self.assertRaises(errors.BadYamlError,
                      status.parse_mapreduce_yaml,
                      "mapreduce:\n"
                      "- name: Mapreduce1\n"
                      "  mapper:\n"
                      "    handler: Handler1\n"
                      "    input_reader: Reader1\n"
                      "- name: Mapreduce1\n"
                      "  mapper:\n"
                      "    handler: Handler1\n"
                      "    input_reader: Reader1\n")

  def testToDict(self):
    """Tests encoding the MR document as JSON."""
    mr_yaml = status.parse_mapreduce_yaml(
        "mapreduce:\n"
        "- name: Mapreduce1\n"
        "  mapper:\n"
        "    handler: Handler1\n"
        "    input_reader: Reader1\n"
        "    params_validator: Validator1\n"
        "    params:\n"
        "    - name: entity_kind\n"
        "      default: Kind1\n"
        "    - name: human_supplied1\n"
        "    - name: human_supplied2\n"
        "- name: Mapreduce2\n"
        "  mapper:\n"
        "    handler: Handler2\n"
        "    input_reader: Reader2\n")
    all_configs = status.MapReduceYaml.to_dict(mr_yaml)
    self.assertEquals(
        [
          {
            'name': 'Mapreduce1',
            'mapper_params_validator': 'Validator1',
            'mapper_params': {
              'entity_kind': 'Kind1',
              'human_supplied2': None,
              'human_supplied1': None},
            'mapper_handler': 'Handler1',
            'mapper_input_reader': 'Reader1'
          },
          {
            'mapper_input_reader': 'Reader2',
            'mapper_handler': 'Handler2',
            'name': 'Mapreduce2'
          }
        ], all_configs)

  def testToDictOutputWriter(self):
    """Tests encoding the MR document with output writer as JSON."""
    mr_yaml = status.parse_mapreduce_yaml(
        "mapreduce:\n"
        "- name: Mapreduce1\n"
        "  mapper:\n"
        "    handler: Handler1\n"
        "    input_reader: Reader1\n"
        "    output_writer: Writer1\n"
        )
    all_configs = status.MapReduceYaml.to_dict(mr_yaml)
    self.assertEquals(
        [
          {
            'name': 'Mapreduce1',
            'mapper_handler': 'Handler1',
            'mapper_input_reader': 'Reader1',
            'mapper_output_writer': 'Writer1',
          },
        ], all_configs)


class ResourceTest(testutil.HandlerTestBase):
  """Tests for the resource handler."""

  def setUp(self):
    """Sets up the test harness."""
    testutil.HandlerTestBase.setUp(self)
    self.handler = status.ResourceHandler()
    self.handler.initialize(mock_webapp.MockRequest(),
                            mock_webapp.MockResponse())
    self.handler.request.path = "/mapreduce/path"

  def testPaths(self):
    """Tests that paths are accessible."""
    self.handler.get("status")
    self.assertTrue(self.handler.response.out.getvalue().startswith(
                        "<!DOCTYPE html>"))
    self.assertEquals("text/html",
                      self.handler.response.headers["Content-Type"])

    self.handler.response.out.truncate(0)
    self.handler.get("jquery.js")
    self.assertTrue(self.handler.response.out.getvalue().startswith(
                        "/*!"))
    self.assertEquals("text/javascript",
                      self.handler.response.headers["Content-Type"])

  def testCachingHeaders(self):
    """Tests that caching headers are correct."""
    self.handler.get("status")
    self.assertEquals("public; max-age=300",
                      self.handler.response.headers["Cache-Control"])

  def testMissing(self):
    """Tests when a resource is requested that doesn't exist."""
    self.handler.get("unknown")
    self.assertEquals(404, self.handler.response.status)


class ListConfigsTest(testutil.HandlerTestBase):
  """Tests for the ListConfigsHandler."""

  def setUp(self):
    """Sets up the test harness."""
    testutil.HandlerTestBase.setUp(self)
    self.handler = status.ListConfigsHandler()
    self.handler.initialize(mock_webapp.MockRequest(),
                            mock_webapp.MockResponse())
    self.handler.request.path = "/mapreduce/command/path"
    self.handler.request.headers["X-Requested-With"] = "XMLHttpRequest"

  def testCSRF(self):
    """Test that we check the X-Requested-With header."""
    del self.handler.request.headers["X-Requested-With"]
    self.handler.get()
    self.assertEquals(403, self.handler.response.status)

  def testBasic(self):
    """Tests listing available configs."""
    old_get_yaml = status.get_mapreduce_yaml
    status.get_mapreduce_yaml = lambda: status.parse_mapreduce_yaml(
        "mapreduce:\n"
        "- name: Mapreduce1\n"
        "  mapper:\n"
        "    handler: Handler1\n"
        "    input_reader: Reader1\n"
        "    params_validator: Validator1\n"
        "    params:\n"
        "    - name: entity_kind\n"
        "      default: Kind1\n"
        "    - name: human_supplied1\n"
        "    - name: human_supplied2\n"
        "- name: Mapreduce2\n"
        "  mapper:\n"
        "    handler: Handler2\n"
        "    input_reader: Reader2\n"
        "  params_validator: MapreduceValidator\n"
        "  params:\n"
        "  - name: foo\n"
        "    value: bar\n")
    try:
      self.handler.get()
    finally:
      status.get_mapreduce_yaml = old_get_yaml

    self.assertEquals(
        {u'configs': [
          {u'mapper_params_validator': u'Validator1',
           u'mapper_params': {
              u'entity_kind': u'Kind1',
              u'human_supplied2': None,
              u'human_supplied1': None},
            u'mapper_input_reader': u'Reader1',
            u'mapper_handler': u'Handler1',
            u'name': u'Mapreduce1'},
          {u'mapper_input_reader': u'Reader2',
           u'mapper_handler': u'Handler2',
           u'name': u'Mapreduce2',
           u'params': {
               u'foo': u'bar',},
           }]},
        simplejson.loads(self.handler.response.out.getvalue()))
    self.assertEquals("text/javascript",
                      self.handler.response.headers["Content-Type"])


class ListJobsTest(testutil.HandlerTestBase):
  """Tests listing active and inactive jobs."""

  def setUp(self):
    """Sets up the test harness."""
    testutil.HandlerTestBase.setUp(self)
    self.start = handlers.StartJobHandler()
    self.start.initialize(mock_webapp.MockRequest(),
                          mock_webapp.MockResponse())
    self.start.request.path = "/mapreduce/command/start"
    self.start.request.set(
        "mapper_input_reader",
        "mapreduce.input_readers.DatastoreInputReader")
    self.start.request.set("mapper_handler", "__main__.TestMap")
    self.start.request.set("mapper_params.entity_kind", "__main__.TestKind")
    self.start.request.headers["X-Requested-With"] = "XMLHttpRequest"

    self.handler = status.ListJobsHandler()
    self.handler.initialize(mock_webapp.MockRequest(),
                            mock_webapp.MockResponse())
    self.handler.request.path = "/mapreduce/command/list"

    self.handler.request.headers["X-Requested-With"] = "XMLHttpRequest"

  def testCSRF(self):
    """Test that we check the X-Requested-With header."""
    TestKind().put()

    del self.start.request.headers["X-Requested-With"]
    self.start.post()
    self.assertEquals(403, self.start.response.status)

    del self.handler.request.headers["X-Requested-With"]
    self.handler.get()
    self.assertEquals(403, self.handler.response.status)

  def testBasic(self):
    """Tests when there are fewer than the max results to render."""
    TestKind().put()
    self.start.request.set("name", "my job 1")
    self.start.post()
    time.sleep(.1)
    self.start.request.set("name", "my job 2")
    self.start.post()
    time.sleep(.1)
    self.start.request.set("name", "my job 3")
    self.start.post()

    self.handler.get()
    result = simplejson.loads(self.handler.response.out.getvalue())
    expected_args = set([
        "active",
        "active_shards",
        "chart_url",
        "chart_width",
        "mapreduce_id",
        "name",
        "shards",
        "start_timestamp_ms",
        "updated_timestamp_ms",
        ])
    self.assertEquals(3, len(result["jobs"]))
    self.assertEquals("my job 3", result["jobs"][0]["name"])
    self.assertEquals("my job 2", result["jobs"][1]["name"])
    self.assertEquals("my job 1", result["jobs"][2]["name"])
    self.assertEquals(expected_args, set(result["jobs"][0].keys()))
    self.assertEquals(expected_args, set(result["jobs"][1].keys()))
    self.assertEquals(expected_args, set(result["jobs"][2].keys()))

  def testCursor(self):
    """Tests when a job cursor is present."""
    TestKind().put()
    self.start.request.set("name", "my job 1")
    self.start.post()
    self.start.request.set("name", "my job 2")
    self.start.post()

    self.handler.request.set("count", "1")
    self.handler.get()
    result = simplejson.loads(self.handler.response.out.getvalue())
    self.assertEquals(1, len(result["jobs"]))
    self.assertTrue("cursor" in result)

    self.handler.response.out.truncate(0)
    self.handler.request.set("count", "1")
    self.handler.request.set("cursor", result['cursor'])
    self.handler.get()
    result2 = simplejson.loads(self.handler.response.out.getvalue())
    self.assertEquals(1, len(result2["jobs"]))
    self.assertFalse("cursor" in result2)

  def testNoJobs(self):
    """Tests when there are no jobs."""
    self.handler.get()
    result = simplejson.loads(self.handler.response.out.getvalue())
    self.assertEquals({'jobs': []}, result)


class GetJobDetailTest(testutil.HandlerTestBase):
  """Tests listing job status detail."""

  def setUp(self):
    """Sets up the test harness."""
    testutil.HandlerTestBase.setUp(self)

    for _ in range(100):
      TestKind().put()
    self.start = handlers.StartJobHandler()
    self.start.initialize(mock_webapp.MockRequest(),
                          mock_webapp.MockResponse())
    self.start.request.path = "/mapreduce/command/start"
    self.start.request.set("name", "my job 1")
    self.start.request.set(
        "mapper_input_reader",
        "mapreduce.input_readers.DatastoreInputReader")
    self.start.request.set("mapper_handler", "__main__.TestMap")
    self.start.request.set("mapper_params.entity_kind", "__main__.TestKind")

    self.start.request.headers["X-Requested-With"] = "XMLHttpRequest"

    self.start.post()
    result = simplejson.loads(self.start.response.out.getvalue())
    self.mapreduce_id = result["mapreduce_id"]

    self.handler = status.GetJobDetailHandler()
    self.handler.initialize(mock_webapp.MockRequest(),
                            mock_webapp.MockResponse())
    self.handler.request.path = "/mapreduce/command/list"

    self.handler.request.headers["X-Requested-With"] = "XMLHttpRequest"

  def KickOffMapreduce(self):
    """Executes pending kickoff task."""
    test_support.execute_all_tasks(self.taskqueue)

  def testCSRF(self):
    """Test that we check the X-Requested-With header."""
    del self.handler.request.headers["X-Requested-With"]
    self.handler.get()
    self.assertEquals(403, self.handler.response.status)

  def testBasic(self):
    """Tests getting the job details."""
    self.KickOffMapreduce()
    self.handler.request.set("mapreduce_id", self.mapreduce_id)
    self.handler.get()
    result = simplejson.loads(self.handler.response.out.getvalue())

    expected_keys = set([
        "active", "chart_url", "counters", "mapper_spec", "mapreduce_id",
        "name", "result_status", "shards", "start_timestamp_ms",
        "updated_timestamp_ms", "params", "hooks_class_name", "chart_width"])
    expected_shard_keys = set([
        "active", "counters", "last_work_item", "result_status",
        "shard_description", "shard_id", "shard_number",
        "updated_timestamp_ms"])

    self.assertEquals(expected_keys, set(result.keys()))
    self.assertEquals(8, len(result["shards"]))
    self.assertEquals(expected_shard_keys, set(result["shards"][0].keys()))

  def testBeforeKickOff(self):
    """Tests getting the job details."""
    self.handler.request.set("mapreduce_id", self.mapreduce_id)
    self.handler.get()
    result = simplejson.loads(self.handler.response.out.getvalue())

    expected_keys = set([
        "active", "chart_url", "counters", "mapper_spec", "mapreduce_id",
        "name", "result_status", "shards", "start_timestamp_ms",
        "updated_timestamp_ms", "params", "hooks_class_name", "chart_width"])

    self.assertEquals(expected_keys, set(result.keys()))

  def testBadJobId(self):
    """Tests when an invalid job ID is supplied."""
    self.handler.request.set("mapreduce_id", "does not exist")
    self.handler.get()
    result = simplejson.loads(self.handler.response.out.getvalue())
    self.assertEquals(
        {"error_message": "\"Could not find job with ID 'does not exist'\"",
         "error_class": "KeyError"},
        result)


# TODO(user): Add tests for abort
# TODO(user): Add tests for cleanup

if __name__ == "__main__":
  unittest.main()
