// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import com.google.appengine.api.appidentity.AppIdentityServiceFailureException;
import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Settings that affect how a MapReduce is executed. May affect performance and resource usage, but
 * should not affect the result (unless the result is dependent on the performance or resource usage
 * of the computation, or if different backends, modules or different base urls have different
 * versions of the code).
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class MapReduceSettings extends MapSettings {

  private static final long serialVersionUID = 610088354289299175L;
  private static final Logger log = Logger.getLogger(MapReduceSettings.class.getName());
  public static final int DEFAULT_MAP_FANOUT = 32;
  public static final int DEFAULT_SORT_BATCH_PER_EMIT_BYTES = 32 * 1024;
  public static final int DEFAULT_SORT_READ_TIME_MILLIS = 180000;
  public static final int DEFAULT_MERGE_FANIN = 32;


  private final String bucketName;
  private final int mapFanout;
  private final Long maxSortMemory;
  private final int sortReadTimeMillis;
  private final int sortBatchPerEmitBytes;
  private final int mergeFanin;

  public static class Builder extends BaseBuilder<Builder> {

    private String bucketName;
    private int mapFanout = DEFAULT_MAP_FANOUT;
    private Long maxSortMemory;
    private int sortReadTimeMillis = DEFAULT_SORT_READ_TIME_MILLIS;
    private int sortBatchPerEmitBytes = DEFAULT_SORT_BATCH_PER_EMIT_BYTES;
    private int mergeFanin = DEFAULT_MERGE_FANIN;

    public Builder() {}

    public Builder(MapReduceSettings settings) {
      super(settings);
      this.bucketName = settings.bucketName;
      this.mapFanout = settings.mapFanout;
      this.maxSortMemory = settings.maxSortMemory;
      this.sortReadTimeMillis = settings.sortReadTimeMillis;
      this.sortBatchPerEmitBytes = settings.sortBatchPerEmitBytes;
      this.mergeFanin = settings.mergeFanin;
    }

    public Builder(MapSettings settings) {
      super(settings);
    }

    @Override
    protected Builder self() {
      return this;
    }

    /**
     * Sets the GCS bucket that will be used for temporary files. If this is not set or {@code null}
     * the app's default bucket will be used.
     */
    public Builder setBucketName(String bucketName) {
      this.bucketName = bucketName;
      return this;
    }
    
    /**
     * The maximum number of files the map stage will write to at the same time. A higher number may
     * increase the speed of the job at the expense of more memory used during the map and sort
     * phases and more intermediate files created.
     *
     * Using the default is recommended.
     */
    public Builder setMapFanout(int mapFanout) {
      Preconditions.checkArgument(mapFanout > 0);
      this.mapFanout = mapFanout;
      return this;
    }
    
    /**
     * The maximum memory the sort stage should allocate (in bytes). This is used to lower the
     * amount of memory it will use. Regardless of this setting it will not exhaust available
     * memory. Null or unset will use the default (no maximum)
     *
     * Using the default is recommended.
     */
    public Builder setMaxSortMemory(Long maxMemory) {
      Preconditions.checkArgument(maxMemory == null || maxMemory >= 0);
      this.maxSortMemory = maxMemory;
      return this;
    }
    
    /**
     * The maximum length of time sort should spend reading input before it starts sorting it and
     * writing it out.
     *
     * Using the default is recommended.
     */
    public Builder setSortReadTimeMillis(int sortReadTimeMillis) {
      Preconditions.checkArgument(sortReadTimeMillis >= 0);
      this.sortReadTimeMillis = sortReadTimeMillis;
      return this;
    }
    
    /**
     * Size (in bytes) of items to batch together in the output of the sort. (A higher value saves
     * storage cost, but needs to be small enough to not impact memory use.)
     *
     * Using the default is recommended.
     */
    public Builder setSortBatchPerEmitBytes(int sortBatchPerEmitBytes) {
      Preconditions.checkArgument(sortBatchPerEmitBytes >= 0);
      this.sortBatchPerEmitBytes = sortBatchPerEmitBytes;
      return this;
    }

    /**
     * Number of files the merge stage will read at the same time. A higher number can increase the
     * speed of the job at the expense of requiring more memory in the merge stage.
     *
     * Using the default is recommended.
     */
    public Builder setMergeFanin(int mergeFanin) {
      this.mergeFanin = mergeFanin;
      return this;
    }

    public MapReduceSettings build() {
      return new MapReduceSettings(this);
    }
  }

  private MapReduceSettings(Builder builder) {
    super(builder);
    bucketName = verifyAndSetBucketName(builder.bucketName);
    mapFanout = builder.mapFanout;
    maxSortMemory = builder.maxSortMemory;
    sortReadTimeMillis = builder.sortReadTimeMillis;
    sortBatchPerEmitBytes = builder.sortBatchPerEmitBytes;
    mergeFanin = builder.mergeFanin;
  }

  String getBucketName() {
    return bucketName;
  }

  Long getMaxSortMemory() {
    return maxSortMemory;
  }

  int getMapFanout() {
    return mapFanout;
  }

  int getSortReadTimeMillis() {
    return sortReadTimeMillis;
  }

  int getSortBatchPerEmitBytes() {
    return sortBatchPerEmitBytes;
  }

  int getMergeFanin() {
    return mergeFanin;
  }

  @Override
  public String toString() {
    return "MapReduceSettings [bucketName=" + bucketName + ", mapFanout=" + mapFanout
        + ", maxSortMemory=" + maxSortMemory + ", sortReadTimeMillis=" + sortReadTimeMillis
        + ", sortBatchPerEmitBytes=" + sortBatchPerEmitBytes + ", mergeFanin=" + mergeFanin + "]";
  }

  private static String verifyAndSetBucketName(String bucket) {
    if (Strings.isNullOrEmpty(bucket)) {
      try {
        bucket = AppIdentityServiceFactory.getAppIdentityService().getDefaultGcsBucketName();
        if (Strings.isNullOrEmpty(bucket)) {
          String message = "The BucketName property was not set in the MapReduceSettings object, "
              + "and this application does not have a default bucket configured to fall back on.";
          log.log(Level.SEVERE, message);
          throw new IllegalArgumentException(message);
        }
      } catch (AppIdentityServiceFailureException e) {
        throw new RuntimeException(
            "The BucketName property was not set in the MapReduceSettings object, "
            + "and could not get the default bucket.", e);
      }
    }
    try {
      verifyBucketIsWritable(bucket);
    } catch (Exception e) {
      throw new RuntimeException("Writeable Bucket '" + bucket + "' test failed. See "
          + "http://developers.google.com/appengine/docs/java/googlecloudstorageclient/activate"
          + " for more information on how to setup Google Cloude storage.", e);
    }
    return bucket;
  }

  private static void verifyBucketIsWritable(String bucket) throws IOException {
    GcsService gcsService = GcsServiceFactory.createGcsService(new RetryParams.Builder()
        .retryMinAttempts(2)
        .retryMaxAttempts(3)
        .totalRetryPeriodMillis(20000)
        .requestTimeoutMillis(10000)
        .build());
    GcsFilename filename = new GcsFilename(bucket, UUID.randomUUID() + ".tmp");
    if (gcsService.getMetadata(filename) != null) {
      log.warning("File '" + filename.getObjectName() + "' exists. Skipping bucket write test.");
      return;
    }
    try {
      gcsService.createOrReplace(filename, GcsFileOptions.getDefaultInstance(),
          ByteBuffer.wrap("Delete me!".getBytes(StandardCharsets.UTF_8)));
    } finally {
      gcsService.delete(filename);
    }
  }
}

