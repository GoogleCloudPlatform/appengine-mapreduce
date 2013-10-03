package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.log.LogQuery;
import com.google.appengine.api.log.LogService;
import com.google.appengine.api.log.LogServiceFactory;
import com.google.appengine.api.log.RequestLogs;
import com.google.appengine.api.search.checkers.Preconditions;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.common.annotations.VisibleForTesting;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

/**
 * Reads App Engine RequestLogs for a specified time range. Assumes that logs are uniformly
 * distributed over the time range for reporting progress.
 */
public class LogInputReader extends InputReader<RequestLogs> implements Serializable {
  private static final long serialVersionUID = 6242327077444737301L;
  private static final Logger log = Logger.getLogger(LogInputReader.class.getName());

  @VisibleForTesting
  protected final LogQuery shardLogQuery;
  private transient RequestLogs lastLog;
  private String lastOffset; // Used to restart log iterator in the correct location on new slice
  private transient Iterator<RequestLogs> logIterator;

  /**
   * Fetch all RequestLogs that satisfy the log query, which has start and end times specific to
   * this shard.
   */
  protected LogInputReader(LogQuery shardLogQuery) {
    Preconditions.checkArgument((shardLogQuery.getStartTimeUsec() == null)
        || (shardLogQuery.getEndTimeUsec() == null)
        || (shardLogQuery.getEndTimeUsec() > shardLogQuery.getStartTimeUsec()),
        "EndTime must be later than StartTime (%d>%d)", shardLogQuery.getEndTimeUsec(),
        shardLogQuery.getStartTimeUsec());
    this.shardLogQuery = shardLogQuery;
  }
  
  @Override
  public void open() {
    lastOffset = null;
    lastLog = null;
  }

  /**
   * Prepare this slice for reading. If this the first slice in the shard, then the shardStartTime
   * will be used. However if the is not the first slice use the last time read (+1, since that
   * timestamp was already read) as the starting point.
   */
  @Override
  public void beginSlice() {
    log.fine("Beginning slice in shard: " + shardLogQuery.getStartTimeUsec() + "-"
        + shardLogQuery.getEndTimeUsec());
    LogService logService = LogServiceFactory.getLogService();

    if (lastOffset != null) {
      shardLogQuery.offset(lastOffset);
    }

    // Execute the query
    logIterator = logService.fetch(shardLogQuery).iterator();
  }

  @Override
  public void endSlice() {
    if (lastLog != null) {
      lastOffset = lastLog.getOffset();
    }
  }

  /**
   * Retrieve the next RequestLog
   */
  @Override
  public RequestLogs next() throws NoSuchElementException {
    Preconditions.checkNotNull(logIterator, "Reader was not initialized via beginSlice()");
    if (logIterator.hasNext()) {
      lastLog = logIterator.next();
      return lastLog;
    } else {
      log.fine("Shard completed: " + shardLogQuery.getStartTimeUsec() + "-"
          + shardLogQuery.getEndTimeUsec());
      throw new NoSuchElementException();
    }
  }

  /**
   * Determine the approximate progress for this shard assuming the RequestLogs are uniformly
   * distributed across the entire time range.
   */
  @Override
  public Double getProgress() {
    if ((shardLogQuery.getStartTimeUsec() == null) || (shardLogQuery.getEndTimeUsec() == null)) {
      return null;
    } else if (lastLog == null) {
      return 0.0;
    } else {
      long processedTimeUsec = shardLogQuery.getEndTimeUsec() - lastLog.getEndTimeUsec();
      long totalTimeUsec = shardLogQuery.getEndTimeUsec() - shardLogQuery.getStartTimeUsec();
      return ((double) processedTimeUsec / totalTimeUsec);
    }
  }
}
