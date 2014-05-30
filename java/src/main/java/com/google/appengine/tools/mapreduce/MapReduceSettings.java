// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import com.google.appengine.api.appidentity.AppIdentityServiceFailureException;
import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.google.common.base.Strings;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Settings that affect how a MapReduce is executed.  May affect performance and
 * resource usage, but should not affect the result (unless the result is
 * dependent on the performance or resource usage of the computation, or if
 * different backends, modules or different base urls have different versions of the code).
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class MapReduceSettings extends MapSettings {

  private static final long serialVersionUID = 610088354289299175L;
  private static final Logger log = Logger.getLogger(MapReduceSettings.class.getName());

  private final String bucketName;

  public static class Builder extends BaseBuilder<Builder> {

    private String bucketName;

    public Builder() {
    }

    public Builder(MapReduceSettings settings) {
      super(settings);
      this.bucketName = settings.bucketName;
    }

    public Builder(MapSettings settings) {
      super(settings);
    }

    @Override
    protected Builder self() {
      return this;
    }

    /**
     * Sets the GCS bucket that will be used for temporary files.
     * If this is not set or {@code null} the app's default bucket will be used.
     */
    public Builder setBucketName(String bucketName) {
      this.bucketName = bucketName;
      return this;
    }

    public MapReduceSettings build() {
      return new MapReduceSettings(this);
    }
  }

  private MapReduceSettings(Builder builder) {
    super(builder);
    bucketName = verifyAndSetBucketName(builder.bucketName);
  }

  String getBucketName() {
    return bucketName;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "("
        + getBaseUrl() + ", "
        + getBackend() + ", "
        + getModule() + ", "
        + getWorkerQueueName() + ", "
        + bucketName + ", "
        + getMillisPerSlice() + ", "
        + getMaxSliceRetries() + ", "
        + getMaxShardRetries() + ")";
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
  GcsFilename filename = new GcsFilename(bucket, UUID.randomUUID().toString() + ".tmp");
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
