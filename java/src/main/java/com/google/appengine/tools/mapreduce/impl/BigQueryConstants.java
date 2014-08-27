package com.google.appengine.tools.mapreduce.impl;


public final class BigQueryConstants {
  private BigQueryConstants() {}

  // Big query does not allow GCS files larger than 1 TB. Limiting the max size to 500GB
  public static final Long MAX_GCS_FILE_SIZE = 500 * 1024 * 1024 * 1024L;

  public static final String BQ_SCOPE = "https://www.googleapis.com/auth/bigquery";

  public static final String GCS_FILE_NAME_FORMAT =
      "BigQueryFilesToLoad/Job-%s/Shard-%%04d/file-%%04d";

  public static final String RECORD_TYPE = "record";

  public static final double MAX_TIME_BEFORE_NEXT_POLL = 30; // in seconds

  public static final double MIN_TIME_BEFORE_NEXT_POLL = 10; // in seconds

  public static final String MIME_TYPE = "application/json";

  public static final String NEWLINE_CHARACTER = "\n";

  public static final Integer MAX_RETRIES = 5;
}
