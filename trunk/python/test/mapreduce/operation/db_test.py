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




from testlib import mox
import unittest

from mapreduce import context
from mapreduce import operation as op

class TestEntity(object):
  """Test entity class."""


class PutTest(unittest.TestCase):
  """Test Put operation."""

  def testPut(self):
    """Test applying Put operation."""
    m = mox.Mox()

    ctx = context.Context(None, None)
    ctx._mutation_pool = m.CreateMock(context._MutationPool)

    entity = TestEntity()
    operation = op.db.Put(entity)

    # Record calls
    ctx._mutation_pool.put(entity)

    m.ReplayAll()
    try:  # test, verify
      operation(ctx)
      m.VerifyAll()
    finally:
      m.UnsetStubs()


class DeleteTest(unittest.TestCase):
  """Test Delete operation."""

  def testDelete(self):
    """Test applying Delete operation."""
    m = mox.Mox()

    ctx = context.Context(None, None)
    ctx._mutation_pool = m.CreateMock(context._MutationPool)

    entity = TestEntity()
    operation = op.db.Delete(entity)

    # Record calls
    ctx._mutation_pool.delete(entity)

    m.ReplayAll()
    try:  # test, verify
      operation(ctx)
      m.VerifyAll()
    finally:
      m.UnsetStubs()


if __name__ == '__main__':
  unittest.main()
