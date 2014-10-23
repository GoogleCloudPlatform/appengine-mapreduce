package com.google.appengine.tools.mapreduce.outputs;

import static com.google.appengine.tools.mapreduce.impl.BigQueryConstants.GCS_FILE_NAME_FORMAT;
import static com.google.appengine.tools.mapreduce.impl.BigQueryConstants.MAX_BIG_QUERY_GCS_FILE_SIZE;
import static com.google.appengine.tools.mapreduce.impl.BigQueryConstants.MIME_TYPE;

import com.google.appengine.tools.mapreduce.BigQueryMarshaller;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * An {@link Output} that writes files in Google cloud storage using a format compatible with
 * bigquery ingestion.
 *
 * @param <O> type of result produced by this output.
 */
public final class BigQueryGoogleCloudStorageStoreOutput<O> extends
    Output<O, BigQueryStoreResult<GoogleCloudStorageFileSet>> {
  private static final long serialVersionUID = -8901164123465666594L;
  private final MarshallingOutput<O, GoogleCloudStorageFileSet> dataMarshallingOutput;
  private final BigQueryMarshaller<O> bigQueryMarshaller;
  private final String bucketName;
  private final String fileNamePattern;

  /**
   * @param bigQueryMarshaller use for generating the bigquery schema and marshal the data into
   *        newline delimited json.
   */
  public BigQueryGoogleCloudStorageStoreOutput(BigQueryMarshaller<O> bigQueryMarshaller,
      String bucketName, String fileNamePattern) {
    this.bigQueryMarshaller = bigQueryMarshaller;
    this.bucketName = bucketName;
    this.fileNamePattern = fileNamePattern;
    SizeSegmentedGoogleCloudStorageFileOutput sizeSegmentedOutput =
        new SizeSegmentedGoogleCloudStorageFileOutput(this.bucketName, MAX_BIG_QUERY_GCS_FILE_SIZE,
            String.format(GCS_FILE_NAME_FORMAT, this.fileNamePattern), MIME_TYPE);
    this.dataMarshallingOutput = new MarshallingOutput<>(sizeSegmentedOutput, bigQueryMarshaller);
  }

  @Override
  public List<MarshallingOutputWriter<O>> createWriters(int numShards) {
    return dataMarshallingOutput.createWriters(numShards);
  }

  @Override
  public BigQueryStoreResult<GoogleCloudStorageFileSet> finish(
      Collection<? extends OutputWriter<O>> writers) throws IOException {
    return new BigQueryStoreResult<>(dataMarshallingOutput.finish(writers),
        bigQueryMarshaller.getSchema());
  }
}
