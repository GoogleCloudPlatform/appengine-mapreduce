package com.google.appengine.tools.mapreduce.outputs;

/**
 * Renamed to GoogleCloudStorageFileOutput
 *
 * @Deprecated Use {@link GoogleCloudStorageFileOutput}. This class will be removed in a future
 *             release.
 */
@Deprecated
public class CloudStorageFileOutput extends GoogleCloudStorageFileOutput {

  // This is the same serialVersionUID of the GoogleCloudStorageFileOutput
  private static final long serialVersionUID = 5544139634754912546L;

  public CloudStorageFileOutput(
      String bucket, String fileNamePattern, String mimeType, int shardCount) {
    super(bucket, fileNamePattern, mimeType, shardCount);
  }
}
