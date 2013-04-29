package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.log.LogQuery;
import com.google.appengine.api.log.RequestLogs;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Reads RequestLogs from the App Engine Logs API
 */
public class LogInput extends Input<RequestLogs> {
  private static final long serialVersionUID = 3676527210213105533L;

  public static final long EARLIEST_LOG_TIME = 1136073600000000L; // 2006-01-01 00:00 UTC

  private LogQuery logQuery;
  private int shardCount;

  /**
   * Create a new Input for getting App Engine Logs
   * 
   *  Shards are created by assuming a uniform distribution of logs over the entire time specified
   * and naively dividing the distance between the start and end times equally. If the logs are not
   * uniformly distributed between the start and end time then the distribution of work performed by
   * each shard will not be equal. For example using a start time of 0 (Jan 1, 1970) will likely
   * cause all work to be performed by the last shard(s).
   * 
   * @param logQuery
   *          a query with at least a start and end time specified. Additional query options may
   *          also be specified to indicate which log data should be read.
   * 
   * @param shardCount
   *          the desired number of shards
   */
  public LogInput(LogQuery logQuery, int shardCount) {
    Preconditions.checkArgument(logQuery.getStartTimeUsec() != null, "Start time must be provided");
    Preconditions.checkArgument(logQuery.getStartTimeUsec() >= EARLIEST_LOG_TIME,
        "Start time must be at least " + EARLIEST_LOG_TIME + " microseconds after the unix epoch.");
    Preconditions.checkArgument(logQuery.getEndTimeUsec() != null, "End time must be provided");
    Preconditions.checkArgument(logQuery.getEndTimeUsec() > logQuery.getStartTimeUsec(),
        "End time must be after start time");
    Preconditions.checkArgument(shardCount > 0, "The number of shards must be greater than 0");
    this.logQuery = logQuery;
    this.shardCount = shardCount;
  }

  @Override
  public List<? extends InputReader<RequestLogs>> createReaders() {
    long startTimeUsec = logQuery.getStartTimeUsec();
    long endTimeUsec = logQuery.getEndTimeUsec();
    // Determine the time per shard
    double perShardTimeUsec = ((double) (endTimeUsec - startTimeUsec)) / shardCount;
    // Ensure that we increment by at least 1 usec per shard
    perShardTimeUsec = (perShardTimeUsec < 1) ? 1 : perShardTimeUsec;

    List<LogInputReader> readers = new ArrayList<LogInputReader>();
    long curStartTimeUsec = startTimeUsec;
    for (int i = 1; i <= shardCount; i++) {
      if (curStartTimeUsec >= endTimeUsec) {
        // The time range is not large enough to support the requested number of shards
        break;
      }
      long curEndTimeUsec = Math.round(startTimeUsec + (i * perShardTimeUsec));
      // Ensure we do not go past the end time due to rounding
      curEndTimeUsec = (curEndTimeUsec > endTimeUsec) ? endTimeUsec : curEndTimeUsec;
      LogQuery readerLogQuery = logQuery.clone();
      readerLogQuery.startTimeUsec(curStartTimeUsec);
      readerLogQuery.endTimeUsec(curEndTimeUsec);
      LogInputReader reader = new LogInputReader(readerLogQuery);
      readers.add(reader);
      curStartTimeUsec = curEndTimeUsec;
    }
    return readers;
  }
}
