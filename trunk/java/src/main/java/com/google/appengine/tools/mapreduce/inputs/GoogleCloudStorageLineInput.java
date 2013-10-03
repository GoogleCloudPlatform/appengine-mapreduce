package com.google.appengine.tools.mapreduce.inputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.GcsFileMetadata;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * CloudStorageLineInput shards files in Cloud Storage on separator boundries.
 */
public class GoogleCloudStorageLineInput extends Input<byte[]> {

  private static final long MIN_SHARD_SIZE = 1024L;

  private static final long serialVersionUID = 5501931160319682453L;

  private final GcsFilename file;
  private final byte separator;
  private final int shardCount;
  private final int bufferSize;

  public GoogleCloudStorageLineInput(GcsFilename file, byte separator, int shardCount) {
    this(file, separator, shardCount, 0);
  }

  public GoogleCloudStorageLineInput(
      GcsFilename file, byte separator, int shardCount, int bufferSize) {
    this.file = checkNotNull(file, "Null file");
    this.separator = separator;
    this.shardCount = shardCount;
    this.bufferSize = bufferSize;
  }

  @Override
  public List<? extends InputReader<byte[]>> createReaders() {
    GcsService gcsService =
        GcsServiceFactory.createGcsService(MapReduceConstants.GCS_RETRY_PARAMETERS);
    GcsFileMetadata metadata;
    try {
      metadata = gcsService.getMetadata(file);
      if (metadata == null) {
        throw new RuntimeException("File does not exist: " + file.toString());
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to read file metadata: " + file.toString(), e);
    }
    long blobSize = metadata.getLength();
    return split(file, blobSize, shardCount);
  }


  private List<? extends InputReader<byte[]>> split(GcsFilename file,
      long blobSize, int shardCount) {
    Preconditions.checkNotNull(file);
    Preconditions.checkArgument(shardCount > 0);
    Preconditions.checkArgument(blobSize >= 0);

    // Sanity check
    if (shardCount * MIN_SHARD_SIZE > blobSize) {
      shardCount = (int) (blobSize / MIN_SHARD_SIZE) + 1;
    }

    List<GoogleCloudStorageLineInputReader> result =
        new ArrayList<GoogleCloudStorageLineInputReader>();
    long startOffset = 0L;
    for (int i = 1; i < shardCount; i++) {
      long endOffset = (i * blobSize) / shardCount;
      result.add(new GoogleCloudStorageLineInputReader(file, startOffset, endOffset, separator,
          bufferSize));
      startOffset = endOffset;
    }
    result.add(new GoogleCloudStorageLineInputReader(file, startOffset, blobSize, separator));
    return result;
  }
}
