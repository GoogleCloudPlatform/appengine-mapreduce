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

import java.util.ArrayList;
import java.util.List;

/**
 * Utility functions for lexicographically splitting strings.
 *
 *
 */
class StringSplitUtil {
  private StringSplitUtil() {
  }

  // Limits for expected range to be split. Your string can contain characters
  // outside of this range, but you'll get crummy splits.
  private static final char MIN_CHAR = '\0';
  private static final char MAX_CHAR = (char) 127;

  /*
   * Given two strings, finds the string that is the lexicographical midpoint.
   * Assumes the string is in the ASCII range (0-127).
   *
   * Examples:
   * findMidpoint("a", "c") => "b"
   * findMidpoint("a", "b") => "a" + (char) ((MAX_CHAR + MIN_CHAR) / 2)
   */
  // visible for testing
  static String findMidpoint(String start, String end) {
    int minLength = Math.min(start.length(), end.length());
    int firstDiffIndex;
    for (firstDiffIndex = 0; firstDiffIndex < minLength; firstDiffIndex++) {
      char startChar = start.charAt(firstDiffIndex);
      char endChar = end.charAt(firstDiffIndex);
      if (startChar != endChar) {
        break;
      }
    }

    String commonPrefix = start.substring(0, firstDiffIndex);

    // Extend start or end as appropriate
    char startChar = firstDiffIndex < start.length() ? start.charAt(firstDiffIndex) : MIN_CHAR;
    char endChar = firstDiffIndex < end.length() ? end.charAt(firstDiffIndex) : MAX_CHAR;
    char midChar = (char) (startChar + (endChar - startChar) / 2);

    // Handle the case where the startChar and the endChar are adjacent
    // by extending the division by a place. We don't worry about averaging
    // later letters in this case.
    // (e.g. findMidpoint("a", "b") -> "a" + (char) (MAX_CHAR + MIN_CHAR / 2)
    if (midChar == startChar) {
      String extendedStart = firstDiffIndex + 1 < start.length() ? start : start + MIN_CHAR;
      String extendedEnd = firstDiffIndex + 1 < end.length() ? end : end + MAX_CHAR;
      return commonPrefix + startChar + findMidpoint(
          extendedStart.substring(firstDiffIndex + 1),
          extendedEnd.substring(firstDiffIndex + 1));
    }

    return commonPrefix + midChar;
  }

  /**
   * Recursive helper for splitStrings. Adds numSplitStrings, roughly equally
   * spaced within the range between start and end strings, to {@code splits}.
   *
   * @param start the range start string
   * @param end the range end string
   * @param numSplitStrings the number of strings to add to split
   * @param oddLevel recursive depth mod 2 for breaking ties in case numSplitStrings - 1 is odd
   * @param splits list to add strings to
   */
  private static void splitStrings(String start, String end, int numSplitStrings, boolean oddLevel,
      List<String> splits) {
    if (numSplitStrings <= 0) {
      return;
    }
    String midString = findMidpoint(start, end);
    int numSplitStringsRemaining = numSplitStrings - 1;
    int leftOffset = 0;
    int rightOffset = 0;
    // Alternate based on level whether which side of the recursion we
    // split odd numbers of split requests on.
    if (numSplitStringsRemaining % 2 == 1) {
      if (oddLevel) {
        leftOffset = 1;
      } else {
        rightOffset = 1;
      }
    }
    splitStrings(start, midString, numSplitStringsRemaining / 2 + leftOffset, !oddLevel, splits);
    splits.add(midString);
    splitStrings(midString, end, numSplitStringsRemaining / 2 + rightOffset, !oddLevel, splits);
  }

  /**
   * Returns a list of {@code numSplitStrings} strings, roughly equally spaced
   * lexicographically between start and end. The list excludes both start and
   * end.
   *
   * See {@link StringSplitUtilTest} for example inputs/outputs.
   *
   * @param start the range start value
   * @param end the range end value
   * @param numSplitStrings the number of strings to return
   * @return a list of evenly spaced strings
   */
  public static List<String> splitStrings(String start, String end, int numSplitStrings) {
    List<String> splits = new ArrayList<String>(numSplitStrings);
    splitStrings(start, end, numSplitStrings, false, splits);
    return splits;
  }
}
