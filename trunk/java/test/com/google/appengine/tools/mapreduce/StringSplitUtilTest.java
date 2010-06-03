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

import java.util.Arrays;
import java.util.List;

/**
 * Tests the {@link StringSplitUtil} class.
 * 
 * @author frew@google.com (Fred Wulff)
 *
 */
public class StringSplitUtilTest extends TestCase {

  public void testFindMidpoint() {
    assertEquals("b", StringSplitUtil.findMidpoint("a", "c"));
    assertEquals("ab", StringSplitUtil.findMidpoint("aa", "ac"));
    assertEquals("a" + (char) (127) + (char) (127 / 2),
                 StringSplitUtil.findMidpoint("a" + (char) 127, "b"));
    assertEquals("a" + (char) (127 / 2), StringSplitUtil.findMidpoint("a", "b"));
    assertEquals("a" + (char) ('b' / 2), StringSplitUtil.findMidpoint("a", "ab"));
    
    // findMidpoint should expand the start string with null characters
    assertEquals("a" + (char) 1, StringSplitUtil.findMidpoint("a", "a" + (char) 2));
    
    // findMidpoint should expand the end string with 127's
    assertEquals("a" + (char) (127 / 2), 
        StringSplitUtil.findMidpoint("a\0", "a"));
    
    assertEquals("a" + (char) ((127 + 'b') / 2), StringSplitUtil.findMidpoint("ab", "b"));
  }

  public void testSplitStrings() {
    List<String> expectedSplits = Arrays.asList("b");
    List<String> splits = StringSplitUtil.splitStrings("a", "c", 1);
    assertEquals(expectedSplits, splits);
  }

  public void testSplitStringsUneven() {
    List<String> expectedSplits = Arrays.asList("b");
    List<String> splits = StringSplitUtil.splitStrings("a", "d", 1);
    assertEquals(expectedSplits, splits);
  }

  public void testSplitStringsUnevenLots() {
    List<String> expectedSplits = Arrays.asList("a" + (char) (127 / 2), "b", "c");
    List<String> splits = StringSplitUtil.splitStrings("a", "d", 3);
    assertEquals(expectedSplits, splits);
  }

  public void testSplitStringsAlternatingLevels() {
    List<String> expectedSplits = Arrays.asList("d", "g", "m", "p", "s");
    List<String> splits = StringSplitUtil.splitStrings("a", "z", 5);
    assertEquals(expectedSplits, splits);
  }
}
