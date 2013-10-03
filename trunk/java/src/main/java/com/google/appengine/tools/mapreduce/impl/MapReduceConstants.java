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
      "MapReduce/%s/MapOutput/Mapper-%04d/Reducer-%%04d";

  public static final String SORT_OUTPUT_DIR_FORMAT =
      "MapReduce/%s/ReduceInput/Shard-%04d/slice-%%04d";

  /**
   * Used in conjunction with TARGET_SORT_RAM_PROPORTIONS to work out how much to allocate for sort
   */
  public static final int ASSUMED_JVM_RAM_OVERHEAD = 32 * 1024 * 1024; 
  
  /**
   * Fraction of system ram sort will allocate. There are multiple values in case the largest
   * proportion is unavailable. If the smallest is unavailable sort will fail.
   *
   *  Ideally this would .75 which is about as much as one can allocate without risk of OOM. However
   * The App Engine scheduler may run 2 or more requests concurrently on a single instance so we
   * need to leave enough memory for all requests.
   */
  public static final double[] TARGET_SORT_RAM_PROPORTIONS = {0.30, 0.20, 0.10};
  
  /**
   * The size of the input buffer passed to the GCS reader.
   */
  public static final int INPUT_BUFFER_SIZE = 1 * 1024 * 1024;
  
  public static final int GCS_IO_BLOCK_SIZE = // 256KB
      OauthRawGcsServiceFactory.createOauthRawGcsService().getChunkSizeBytes();
  
 public static final String MAP_OUTPUT_MIME_TYPE =
      "application/vnd.appengine.mapreduce.map-output.records";

  public static final String REDUCE_INPUT_MIME_TYPE =
      "application/vnd.appengine.mapreduce.reduce-input.records";
  
  /**
   * Maximum display size of the lastItem in the UI. 
   */
  public static final int MAX_LAST_ITEM_STRING_SIZE = 100;

  public static final RetryParams GCS_RETRY_PARAMETERS = new RetryParams.Builder()
      .retryMaxAttempts(10)
      .retryMinAttempts(6)
      .requestTimeoutMillis(10000)
      .maxRetryDelayMillis(30000)
      .totalRetryPeriodMillis(60000)
      .build();

}
