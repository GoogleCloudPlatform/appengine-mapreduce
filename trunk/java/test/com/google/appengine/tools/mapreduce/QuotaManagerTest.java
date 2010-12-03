/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

import junit.framework.TestCase;

/**
 * Tests the {@link QuotaManager} class.
 * 
 * @author frew@google.com (Fred Wulff)
 *
 */
public class QuotaManagerTest extends TestCase {
  private final LocalServiceTestHelper helper 
      = new LocalServiceTestHelper(new LocalMemcacheServiceTestConfig());

  private MemcacheService memcacheService;
  private QuotaManager manager;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    memcacheService = MemcacheServiceFactory.getMemcacheService("bob");
    manager = new QuotaManager(memcacheService);
  }
  
  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }
  
  public void testConsume() {
    manager.put("foo", 20);
    assertEquals(10, manager.consume("foo", 10));
    assertEquals(7, manager.consume("foo", 7));
    assertEquals(0, manager.consume("foo", 5));
    assertEquals(3, manager.consume("foo", 5, true));
  }
  
  public void testSet() {
    manager.put("foo", 5);
    manager.set("foo", 7);
    assertEquals(7, manager.consume("foo", 10, true));
  }
}
