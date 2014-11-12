package com.google.appengine.tools.mapreduce.outputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * An {@link Output} that writes files to GCS segmented by specified size limit. For e.g. to write
 * files with size no greater than 100 bytes pass the segmentSizeLimit as 100.
 */
public final class SizeSegmentedGoogleCloudStorageFileOutput extends
    Output<ByteBuffer, GoogleCloudStorageFileSet> {

  private static final long serialVersionUID = 2720924098947625547L;
  private final String mimeType;
  private final String bucket;
  private final long segmentSizeLimit;
  private final String fileNamePattern;

  /**
   * @param bucket GCS bucket
   * @param segmentSizeLimit Maximum size of the files to be written to the output
   * @param fileNamePattern a java format string {@link java.util.Formatter} containing one int
   *        argument for the shard number and another int argument for the segment number for e.g.
   *        shard-%04d-segment-%04d.
   * @param mimeType The string to be passed as the mimeType to GCS.
   */
  public SizeSegmentedGoogleCloudStorageFileOutput(String bucket, long segmentSizeLimit,
      String fileNamePattern, String mimeType) {
    this.bucket = checkNotNull(bucket, "Null bucket");
    this.segmentSizeLimit = segmentSizeLimit;
    this.fileNamePattern = checkNotNull(fileNamePattern, "Null file name pattern");
    this.mimeType = checkNotNull(mimeType, "Null mime type");
  }

  private static class SizeSegmentingGoogleCloudStorageFileWriter extends
      SizeSegmentingOutputWriter {

    private static final long serialVersionUID = 1450052376124958061L;
    private final String bucket;
    private final String fileNamePattern;
    private final String mimeType;
    private final int shardNumber;
    private final List<GoogleCloudStorageFileOutputWriter> delegatedWriters;

    /**
     * @param bucket
     * @param fileNamePattern a Java format string {@link java.util.Formatter} containing one int
     *        argument for segment number. For e.g. 5 for the 5th file written by this writer
     * @param segmentSizeLimit Maximum size of the files to be written out by this writer
     */
    public SizeSegmentingGoogleCloudStorageFileWriter(String bucket, String fileNamePattern,
        int shardNumber, String mimeType, long segmentSizeLimit) {
      super(segmentSizeLimit);
      this.bucket = checkNotNull(bucket, "Null bucket");
      this.fileNamePattern = checkNotNull(fileNamePattern, "Null fileNamePattern");
      this.mimeType = checkNotNull(mimeType, "Null mime type");
      this.shardNumber = shardNumber;
      delegatedWriters = new ArrayList<>();
    }

    @Override
    protected OutputWriter<ByteBuffer> createWriter(int fileNum) {
      String fileName = String.format(fileNamePattern, shardNumber, System.currentTimeMillis());
      GoogleCloudStorageFileOutputWriter toReturn =
          new GoogleCloudStorageFileOutputWriter(new GcsFilename(bucket, fileName), mimeType);
      delegatedWriters.add(toReturn);
      return toReturn;
    }

    @Override
    public long estimateMemoryRequirement() {
      return GoogleCloudStorageFileOutputWriter.MEMORY_REQUIRED;
    }

    List<GoogleCloudStorageFileOutputWriter> getDelegatedWriters() {
      return delegatedWriters;
    }

    @Override
    public boolean allowSliceRetry() {
      return true;
    }
  }

  /**
   * Returns {@link OutputWriter}s that write files to GCS but segments the files based on the
   * maximum allowed file size. One writer can write more than one file per shard
   */
  @Override
  public List<? extends OutputWriter<ByteBuffer>> createWriters(int numShards) {
    ImmutableList.Builder<SizeSegmentingGoogleCloudStorageFileWriter> result =
        new ImmutableList.Builder<>();
    for (int i = 0; i < numShards; ++i) {
      result.add(new SizeSegmentingGoogleCloudStorageFileWriter(bucket, fileNamePattern, i,
          mimeType, segmentSizeLimit));
    }
    return result.build();
  }

  /**
   * Returns a list of all the filenames written by the output writers
   *
   * @throws IOException
   */
  @Override
  public GoogleCloudStorageFileSet finish(Collection<? extends OutputWriter<ByteBuffer>> writers)
      throws IOException {
    List<String> fileNames = new ArrayList<>();
    for (OutputWriter<ByteBuffer> writer : writers) {
      SizeSegmentingGoogleCloudStorageFileWriter segWriter =
          (SizeSegmentingGoogleCloudStorageFileWriter) writer;
      for (GoogleCloudStorageFileOutputWriter delegatedWriter : segWriter.getDelegatedWriters()) {
        fileNames.add(delegatedWriter.getFile().getObjectName());
      }
    }
    return new GoogleCloudStorageFileSet(bucket, fileNames);
  }
}
