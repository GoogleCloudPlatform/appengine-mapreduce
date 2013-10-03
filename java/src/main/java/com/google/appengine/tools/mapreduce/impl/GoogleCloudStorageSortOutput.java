package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.LevelDbOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.MarshallingOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.SlicingOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.SlicingWriterCreator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

/**
 * Defines the way data is written out by the sorter. This consists of a single GCS file containing
 * LevelDb format to separate records which are KeyValue pairs. What the key and value are is not
 * known to the sorter, so these are byte arrays that are simply passed through from the mapper.
 * A new file is created at the beginning of each slice in case the data does not all fit in memory.
 *
 */
public class GoogleCloudStorageSortOutput extends
    Output<KeyValue<ByteBuffer, Iterator<ByteBuffer>>, List<GoogleCloudStorageFileSet>> {

  private static final long serialVersionUID = 8332978108336443982L;

  @SuppressWarnings("unused")
  private static final Logger log =
      Logger.getLogger(GoogleCloudStorageSortOutput.class.getName());

  private final String bucket;
  private final int shardCount;
  private final String mrJobId;


  private static class WriterCreatorImpl implements SlicingWriterCreator<
      KeyValue<ByteBuffer, Iterator<ByteBuffer>>> {

    private static final long serialVersionUID = -6765187605013451624L;
    private String bucket;
    private String fileNamePattern;
    private int sliceNumber;
    private List<String> fileNames;

    /**
     * @param fileNamePattern a Java format string {@link java.util.Formatter} containing one int
     *        argument for the slice number.
     */
    public WriterCreatorImpl(String bucket, String fileNamePattern) {
      this.bucket = checkNotNull(bucket, "Null bucket");
      this.fileNamePattern = checkNotNull(fileNamePattern, "Null fileNamePattern");
      this.sliceNumber = 0;
      this.fileNames = new ArrayList<String>();
    }


    @Override
    public OutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>> createNextWriter() {
      String fileName = String.format(fileNamePattern, sliceNumber);
      fileNames.add(fileName);
      Marshaller<ByteBuffer> identity = Marshallers.getByteBufferMarshaller();
      sliceNumber++;
      // Uses LevelDbOutputWriter wrapping GoogleCloudStorageFileOutputWriter rather than
      // GoogleCloudStorageLevelDbOutputWriter because the padding at the end of the slice is
      // unneeded as the file is being finalized.
      return new MarshallingOutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>>(
          new LevelDbOutputWriter(new GoogleCloudStorageFileOutputWriter(
              new GcsFilename(bucket, fileName), MapReduceConstants.REDUCE_INPUT_MIME_TYPE)),
          Marshallers.getKeyValuesMarshaller(identity, identity));
    }

    public GoogleCloudStorageFileSet finish() {
      return new GoogleCloudStorageFileSet(bucket, fileNames);
    }
  }

  public GoogleCloudStorageSortOutput(String bucket, String mrJobId, int shardCount) {
    this.bucket = checkNotNull(bucket, "Null bucket");
    this.shardCount = shardCount;
    this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
  }

  @Override
  public List<? extends OutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>>> createWriters() {
    List<SlicingOutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>, WriterCreatorImpl>>
        result = new ArrayList<SlicingOutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>,
            WriterCreatorImpl>>(shardCount);
    for (int i = 0; i < shardCount; i++) {
      String formatStringForShard =
          String.format(MapReduceConstants.SORT_OUTPUT_DIR_FORMAT, mrJobId, i);
      result.add(new SlicingOutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>,
          WriterCreatorImpl>(new WriterCreatorImpl(bucket, formatStringForShard)));
    }
    return result;
  }

  @Override
  public List<GoogleCloudStorageFileSet> finish(
      Collection<? extends OutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>>> writers)
      throws IOException {
    assert writers.size() == shardCount;
    List<GoogleCloudStorageFileSet> filesByShard =
        new ArrayList<GoogleCloudStorageFileSet>(shardCount);
    for (OutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>> w : writers) {
      @SuppressWarnings("unchecked")
      SlicingOutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>, WriterCreatorImpl> writer =
          (SlicingOutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>, WriterCreatorImpl>) w;
      filesByShard.add(writer.getCreator().finish());
    }
    return filesByShard;
  }

  @Override
  public int getNumShards() {
    return shardCount;
  }

}
