#!/usr/bin/env python
#
# Copyright 2010 Google Inc. All Rights Reserved.





import unittest
from google.appengine.api import module_testutil
from mapreduce import context
from mapreduce import control
from mapreduce import datastore_range_iterators
from mapreduce import errors
from mapreduce import input_readers
from mapreduce import key_ranges
from mapreduce import mapper_pipeline
from mapreduce import mapreduce_pipeline
from mapreduce import model
from mapreduce import namespace_range
from mapreduce import operation as op
from mapreduce import output_writers
from mapreduce import property_range
from mapreduce import records
from mapreduce import shuffler
from mapreduce import util


class NamespaceRangeTest(module_testutil.ModuleInterfaceTest,
                         googletest.TestCase):
  """Test module interface."""

  MODULE = namespace_range


class PropertyRangeTest(module_testutil.ModuleInterfaceTest,
                        googletest.TestCase):
  """Test module interface."""

  MODULE = property_range


class KeyRangesTest(module_testutil.ModuleInterfaceTest,
                    googletest.TestCase):
  """Test module interface."""

  MODULE = key_ranges


class DatastoreRangeIteratorsTest(module_testutil.ModuleInterfaceTest,
                                  googletest.TestCase):
  """Test module interface."""

  MODULE = datastore_range_iterators


class ContextTest(module_testutil.ModuleInterfaceTest,
                  googletest.TestCase):
  """Test context module interface."""

  MODULE = context


class ControlTest(module_testutil.ModuleInterfaceTest,
                  googletest.TestCase):
  """Test control module interface."""

  MODULE = control


class CountersTest(module_testutil.ModuleInterfaceTest,
                   googletest.TestCase):
  """Test counters module interface."""

  MODULE = op.counters


class DbTest(module_testutil.ModuleInterfaceTest,
             googletest.TestCase):
  """Test db module interface."""

  MODULE = op.db


class ErrorsTest(module_testutil.ModuleInterfaceTest,
                 googletest.TestCase):
  """Test errors module interface."""

  MODULE = errors


class InputReadersTest(module_testutil.ModuleInterfaceTest,
                       googletest.TestCase):
  """Test input_readers module interface."""

  MODULE = input_readers


class ModelTest(module_testutil.ModuleInterfaceTest,
                googletest.TestCase):
  """Test model module interface."""

  MODULE = model


class OutputWritersTest(module_testutil.ModuleInterfaceTest,
                        googletest.TestCase):
  """Test output_writers module interface."""

  MODULE = output_writers


class UtilTest(module_testutil.ModuleInterfaceTest,
               googletest.TestCase):
  """Test util module interface."""

  MODULE = util


class MapperPipelineTest(module_testutil.ModuleInterfaceTest,
                         googletest.TestCase):
  """Test mapper_pipeline module interface."""

  MODULE = mapper_pipeline


class MapreducePipelineTest(module_testutil.ModuleInterfaceTest,
                            googletest.TestCase):
  """Test mapreduce_pipeline module interface."""

  MODULE = mapreduce_pipeline


class ShufflerTest(module_testutil.ModuleInterfaceTest,
                   googletest.TestCase):
  """Test shuffler module interface."""

  MODULE = shuffler


class RecordsTests(module_testutil.ModuleInterfaceTest,
                   googletest.TestCase):
  """Test records module interface."""
  MODULE = records


if __name__ == '__main__':
  googletest.main()
