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




import unittest

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import memcache
from google.appengine.api.memcache import memcache_stub
from mapreduce import quota


NS = quota._QUOTA_NAMESPACE


class QuotaTestCase(unittest.TestCase):
  """Base class for quota tests."""

  def setUp(self):
    unittest.TestCase.setUp(self)

    self.memcache = memcache_stub.MemcacheServiceStub()
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub("memcache", self.memcache)


class QuotaManagerTest(QuotaTestCase):
  """Tests for QuotaManager class."""

  def setUp(self):
    QuotaTestCase.setUp(self)
    self.quota_manager = quota.QuotaManager(memcache.Client())

  def testPut(self):
    """Test put method."""
    self.quota_manager.put("foo", 100)
    self.assertEquals(100, self.quota_manager.get("foo"))

    self.quota_manager.put("foo", 13)
    self.assertEquals(113, self.quota_manager.get("foo"))

  def testConsumeSuccess(self):
    """Test consume method when there's lot's of quota."""
    self.quota_manager.set("foo", 130)
    self.assertTrue(self.quota_manager.consume("foo", 100))
    self.assertEquals(30, self.quota_manager.get("foo"))

  def testConsumeFail(self):
    """Test consume method when there's not enough quota."""
    self.quota_manager.set("foo", 160)
    self.assertFalse(self.quota_manager.consume("foo", 300))
    self.assertEquals(160, self.quota_manager.get("foo"))

  def testConsumeSomeLotsOfQuota(self):
    """Test consume_some method when there's lots of quota."""
    self.quota_manager.set("foo", 300)
    self.assertEquals(20, self.quota_manager.consume("foo", 20, True))
    self.assertEquals(280, self.quota_manager.get("foo"))

  def testConsumeSomePartialQuota(self):
    """Test consume_some method when there's quota only for partial amount."""
    self.quota_manager.set("foo", 7)
    self.assertEquals(7, self.quota_manager.consume("foo", 20, True))
    self.assertEquals(0, self.quota_manager.get("foo"))

  def testConsumeSomeNoQuota(self):
    """Test consume_some method when there's absolutely no quota."""
    self.assertEquals(0, self.quota_manager.consume("foo", 20, True))
    self.assertEquals(0, self.quota_manager.get("foo"))

  def testSetQuota(self):
    """Test setting absolute quota value."""
    self.quota_manager.set("foo", 30)
    self.assertEquals(30, self.quota_manager.get("foo"))

  def testGetQuotaNotPresent(self):
    self.assertEquals(0, self.quota_manager.get("foo"))


class QuotaConsumerTest(QuotaTestCase):
  """Tests for QuotaConsumer class."""

  def setUp(self):
    QuotaTestCase.setUp(self)
    self.quota_manager = quota.QuotaManager(memcache.Client())
    self.consumer = quota.QuotaConsumer(self.quota_manager, "foo", 50)

  def testEnoughQuotaForBatch(self):
    """Test consuming when there's enough quota for a batch."""
    self.quota_manager.put("foo", 300)
    self.assertTrue(self.consumer.consume())
    self.assertEquals(250, self.quota_manager.get("foo"))

    self.consumer.dispose()
    self.assertEquals(299, self.quota_manager.get("foo"))

  def testConsumeMoreThanBatch(self):
    """Test consuming more than a batch."""
    self.quota_manager.put("foo", 300)
    self.assertTrue(self.consumer.consume(179))
    self.assertEquals(100, self.quota_manager.get("foo"))

    self.consumer.dispose()
    self.assertEquals(121, self.quota_manager.get("foo"))

  def testConsumeEnoughQuotaForPartialBatch(self):
    """Test consuming when there's enough quota for part of a batch."""
    self.quota_manager.put("foo", 25)
    self.assertTrue(self.consumer.consume())
    self.assertEquals(0, self.quota_manager.get("foo"))

    self.consumer.dispose()
    self.assertEquals(24, self.quota_manager.get("foo"))

  def testNotEnoughQuota(self):
    """Test consuming when there's not enough quota."""
    self.quota_manager.put("foo", 321)
    self.assertFalse(self.consumer.consume(500))
    self.assertEquals(0, self.quota_manager.get("foo"))

    self.consumer.dispose()
    self.assertEquals(321, self.quota_manager.get("foo"))

  def testCheckNoQuotaNoBatch(self):
    """Test check method when no batch quota was consumed."""
    self.quota_manager.put("foo", 30)
    self.assertTrue(self.consumer.check(25))
    self.assertFalse(self.consumer.check(50))

  def testCheckQuotaBatch(self):
    """Test check method when some quota was consumed."""
    self.quota_manager.put("foo", 100)
    self.assertTrue(self.consumer.consume(1))
    self.assertTrue(self.consumer.check(25))
    self.assertTrue(self.consumer.check(75))
    self.assertFalse(self.consumer.check(100))


if __name__ == '__main__':
  unittest.main()
