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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;

import junit.framework.TestCase;

/**
 * Tests {@link RangeInputFormat}.
 *
 *
 */
public class RangeInputFormatTest extends TestCase {
  private RangeInputFormat format;
  private Configuration conf;

  public void setUp() throws Exception {
    format = new RangeInputFormat();
    conf = new Configuration(false);
  }

  public void tearDown() throws Exception {
  }

  private List<InputSplit> getSplitsForConfiguration() throws Exception {
    JobContext context = new JobContext(conf, new JobID("Foo", 1));
    return format.getSplits(context);
  }

  private void assertSplits(long[] dividerArray) throws Exception {
    List<InputSplit> splits = getSplitsForConfiguration();
    assertEquals(dividerArray.length, splits.size() + 1);
    int i = 0;
    for (InputSplit split : splits) {
      RangeInputSplit rangeSplit = (RangeInputSplit) split;
      assertEquals(dividerArray[i++], rangeSplit.getSplitStart());
      assertEquals(dividerArray[i], rangeSplit.getSplitEnd());
    }
  }

  private void setStartEndCount(long start, long end, long count) {
    conf.setLong(RangeInputFormat.RANGE_START_KEY, start);
    conf.setLong(RangeInputFormat.RANGE_END_KEY, end);
    conf.setLong(RangeInputFormat.SHARD_COUNT_KEY, count);
  }

  public void testGetSplits() throws Exception {
    setStartEndCount(0, 4, 2);
    assertSplits(new long[]{0, 2, 4});

    setStartEndCount(0, 3, 2);
    assertSplits(new long[]{0, 2, 3});

    setStartEndCount(0, 5, 5);
    assertSplits(new long[]{0, 1, 2, 3, 4, 5});

    setStartEndCount(0, 5, 10);
    assertSplits(new long[]{0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5});

    setStartEndCount(0, 1, 3);
    assertSplits(new long[]{0, 0, 1, 1});

    setStartEndCount(-1, 0, 1);
    try {
      getSplitsForConfiguration();
      fail("Should have bombed on negative start");
    } catch(InvalidConfigurationException expected) {
      // Expected
    }

    setStartEndCount(5, 5, 1);
    try {
      getSplitsForConfiguration();
      fail("Should have bombed on end <= start");
    } catch(InvalidConfigurationException expected) {
      // Expected
    }

    //Tests that longs are processed correctly and not just ints
    setStartEndCount(Integer.MAX_VALUE + 4L, Integer.MAX_VALUE + 8L, 2);
    assertSplits(new long[]{(long)Integer.MAX_VALUE + 4L, (long)Integer.MAX_VALUE + 6L, (long)Integer.MAX_VALUE + 8L});

  }
}
