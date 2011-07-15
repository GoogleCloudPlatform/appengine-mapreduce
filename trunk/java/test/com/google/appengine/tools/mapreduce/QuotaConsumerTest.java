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

import junit.framework.TestCase;

import static org.easymock.EasyMock.*;

/**
 * Tests the {@link QuotaConsumer} class.
 *
 *
 */
public class QuotaConsumerTest extends TestCase {
  private static final String BUCKET_NAME = "foo";
  private static final long BATCH_SIZE = 20;

  private QuotaManager manager;
  private QuotaConsumer consumer;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    manager = createMock(QuotaManager.class);
    consumer = new QuotaConsumer(manager, BUCKET_NAME, BATCH_SIZE);
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    verify(manager);
  }

  public void testSimpleConsume() {
    expect(manager.consume(BUCKET_NAME, BATCH_SIZE, true))
      .andReturn(BATCH_SIZE)
      .andReturn((long) 0);
    replay(manager);
    assertTrue(consumer.consume(3));
    assertTrue(consumer.consume(7));
    assertTrue(consumer.consume(10));
    assertFalse(consumer.consume(1));
  }

  public void testConsumePartial() {
    expect(manager.consume(BUCKET_NAME, BATCH_SIZE, true))
      .andReturn((long) 5)
      .andReturn((long) 0);
    replay(manager);
    assertTrue(consumer.consume(5));
    assertFalse(consumer.consume(1));
  }

  public void testCheck() {
    expect(manager.get(BUCKET_NAME))
      .andReturn((long) 10)
      .times(3);
    replay(manager);
    assertTrue(consumer.check(10));
    assertFalse(consumer.check(11));
    consumer.put(1);

    // Should be satisfied locally. If there are 4 calls seen to QuotaManager
    // this is probably the culprit
    assertTrue(consumer.check(1));

    assertTrue(consumer.check(11));
  }

  public void testDispose() {
    manager.put(BUCKET_NAME, 5);
    replay(manager);
    consumer.put(5);
    consumer.dispose();
  }
}
