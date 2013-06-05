package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.log.LogQuery;
import com.google.appengine.api.log.RequestLogs;
import com.google.appengine.tools.mapreduce.InputReader;

import junit.framework.TestCase;

import java.util.List;

/**
 * Test the LogInput class
 */
public class LogInputTest extends TestCase {
  private static final long BASE_TIME = LogInput.EARLIEST_LOG_TIME;

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  private static void runTest(
      long timeOffset, long startTimeUsec, long endTimeUsec, int numShards, long[] startTimes) {
    LogQuery logQuery = LogQuery.Builder.withDefaults();
    logQuery.startTimeUsec(timeOffset + startTimeUsec);
    logQuery.endTimeUsec(timeOffset + endTimeUsec);
    LogInput input = new LogInput(logQuery, numShards);
    List<? extends InputReader<RequestLogs>> readers = input.createReaders();
    assertEquals("Incorrect number of readers", startTimes.length, readers.size());
    for (int i = 0; i < startTimes.length; i++) {
      LogInputReader reader = (LogInputReader) readers.get(i);
      assertEquals("Incorrect start time in reader " + i, timeOffset + startTimes[i],
          (long) reader.shardLogQuery.getStartTimeUsec());
      long expectedEndTime = ((i + 1) < startTimes.length) ? startTimes[i + 1] : endTimeUsec;
      assertEquals("Incorrect end time in reader " + i, timeOffset + expectedEndTime,
          (long) reader.shardLogQuery.getEndTimeUsec());
    }
  }

  public void testCreateReadersWithEvenSplits() throws Exception {
    // 1 shard, 1 usec
    runTest(BASE_TIME, 1L, 2L, 1, new long[] { 1L });
    // 2 shards, 4 usec
    runTest(BASE_TIME, 1L, 5L, 2, new long[] { 1L, 3L });
    // 10 shards, 500 usec, 50 each
    runTest(BASE_TIME, 1L, 501L, 10,
        new long[] { 1L, 51L, 101L, 151L, 201L, 251L, 301L, 351L, 401L, 451L });
  }

  public void testCreateReadersWithUnevenSplits() throws Exception {
    // 2 shards, 3 usec, 1.5 usec or 1-2 usec each
    runTest(BASE_TIME, 1L, 4L, 2, new long[] { 1L, 3L });
    // 4 shards, 7 usec, 1.75 usec or 1-2 usec each
    runTest(BASE_TIME, 1L, 8L, 4, new long[] { 1L, 3L, 5L, 6L });
    // 7 shard, 100 usec, 14.29 usec or 14-15 usec each
    runTest(BASE_TIME, 1L, 101L, 7, new long[] { 1L, 15L, 30L, 44L, 58L, 73L, 87L });
    // Real world test: Apr 3, 2006 18:03:16.123456 through Nov 12, 2012 07:29:30.890123, 9 shards
    // 208617974766668 usec = 2414.56 days, 23179774974074.2 usec (or 268.28 days) per shard
    runTest(0L, 1144087396123456L, 1352705370890124L, 9, new long[] { 1144087396123456L,
        1167267171097530L, 1190446946071605L, 1213626721045679L, 1236806496019753L,
        1259986270993827L, 1283166045967901L, 1306345820941976L, 1329525595916050L });
  }

  public void testCreateReadersNotEnoughData() throws Exception {
    // 2 shards, 1 usec
    runTest(BASE_TIME, 1L, 2L, 2, new long[] { 1L });
    // 7 shard, 4 usec
    runTest(BASE_TIME, 1L, 5L, 7, new long[] { 1L, 2L, 3L, 4L });
  }
}