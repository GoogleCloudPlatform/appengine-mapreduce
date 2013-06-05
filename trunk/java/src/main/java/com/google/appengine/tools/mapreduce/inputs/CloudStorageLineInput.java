package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.cloudstorage.GcsFilename;

/**
 * Renamed to GoogleCloudStorageLineInput
 *
 * @Deprecated Use {@link GoogleCloudStorageLineInput}. This class will be removed in a future
 *             release.
 */
@Deprecated
public class CloudStorageLineInput extends GoogleCloudStorageLineInput {

  // This is the same serialVersionUID of the GoogleCloudStorageLineInput
  private static final long serialVersionUID = 5501931160319682453L;

  public CloudStorageLineInput(GcsFilename file, byte separator, int shardCount) {
    super(file, separator, shardCount);
  }

  public CloudStorageLineInput(GcsFilename file, byte separator, int shardCount, int bufferSize) {
    super(file, separator, shardCount, bufferSize);
  }
}
