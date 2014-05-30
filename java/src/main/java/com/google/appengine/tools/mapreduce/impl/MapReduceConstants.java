// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.cloudstorage.RetryParams;
import com.google.appengine.tools.cloudstorage.oauth.OauthRawGcsServiceFactory;

/**
 * @author ohler@google.com (Christian Ohler)
 */
public class MapReduceConstants {

  private MapReduceConstants() {}

  public static final String MAP_OUTPUT_DIR_FORMAT =
      "MapReduce/%s/MapOutput/Mapper-%04d/SortShard-%%04d";

  public static final String SORT_OUTPUT_DIR_FORMAT =
      "MapReduce/%s/ReduceInput/Sorter-%04d/ReduceShard-%04d/slice-%%04d";

  public static final String MERGE_OUTPUT_DIR_FORMAT =
      "MapReduce/%s/MergedOutput/ReduceShard-%04d/file-%%04d";

  public static final int ASSUMED_BASE_MEMORY_PER_REQUEST = 16 * 1024 * 1024;

  /**
   * Used as a rough estimate of how much memory is needed as a baseline independent of any specific
   * allocations.
   */
  public static final int ASSUMED_JVM_RAM_OVERHEAD = 32 * 1024 * 1024;

  /**
   * The maximum length of time sort should spend reading input before it starts sorting it and
   * writing it out.
   */
  public static final int MAX_SORT_READ_TIME_MILLIS = 180000;

  /**
   * The size of the input buffer passed to the GCS readers / writers.
   * Is widely used by size estimates.
   */
  public static final int DEFAULT_IO_BUFFER_SIZE = 1 * 1024 * 1024;

  public static final int GCS_IO_BLOCK_SIZE = // 256KB
      OauthRawGcsServiceFactory.createOauthRawGcsService().getChunkSizeBytes();

  public static final String MAP_OUTPUT_MIME_TYPE =
      "application/vnd.appengine.mapreduce.map-output.records";

  public static final String REDUCE_INPUT_MIME_TYPE =
      "application/vnd.appengine.mapreduce.reduce-input.records";

  public static final int MAX_WRITER_FANOUT = 32;

  /**
   * Number of files the merge stage will read at the same time.
   */
  public static final int MERGE_FANIN = 32;

  /**
   * Size (in bytes) of items to batch together in the output of the sort.
   * (A higher value saves storage cost, but needs to be small enough to not impact memory use.)
   */
  public static final int BATCHED_ITEM_SIZE_PER_EMIT = 32 * 1024;


  /**
   * Maximum display size of the lastItem in the UI.
   */
  public static final int MAX_LAST_ITEM_STRING_SIZE = 100;

  public static final int MAX_REDUCE_SHARDS = 2048;

  public static final RetryParams GCS_RETRY_PARAMETERS = new RetryParams.Builder()
      .requestTimeoutMillis(30000)
      .retryMaxAttempts(10)
      .retryMinAttempts(6)
      .maxRetryDelayMillis(30000)
      .totalRetryPeriodMillis(120000)
      .initialRetryDelayMillis(250)
      .build();
}
