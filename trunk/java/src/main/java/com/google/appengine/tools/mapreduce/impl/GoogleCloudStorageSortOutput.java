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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Defines the way data is written out by the sorter. This consists of a single GCS file containing
 * LevelDb format to separate records which are KeyValue pairs. What the key and value are is not
 * known to the sorter, so these are byte arrays that are simply passed through from the mapper.
 * A new file is created at the beginning of each slice in case the data does not all fit in memory.
 *
 */
public class GoogleCloudStorageSortOutput extends
    Output<KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>, List<GoogleCloudStorageFileSet>> {

  private static final long serialVersionUID = 8332978108336443982L;

  private final String bucket;
  private final String mrJobId;

  private static class WriterCreatorImpl implements
      SlicingWriterCreator<KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>> {

    private static final long serialVersionUID = -6765187605013451624L;

    private final String bucket;
    private final String fileNamePattern;
    private int sliceNumber;
    private final List<String> fileNames;

    /**
     * @param fileNamePattern a Java format string {@link java.util.Formatter} containing one int
     *        argument for the slice number.
     */
    public WriterCreatorImpl(String bucket, String fileNamePattern) {
      this.bucket = checkNotNull(bucket, "Null bucket");
      this.fileNamePattern = checkNotNull(fileNamePattern, "Null fileNamePattern");
      this.sliceNumber = 0;
      this.fileNames = new ArrayList<>();
    }


    @Override
    public OutputWriter<KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>> createNextWriter() {
      String fileName = String.format(fileNamePattern, sliceNumber);
      fileNames.add(fileName);
      Marshaller<ByteBuffer> identity = Marshallers.getByteBufferMarshaller();
      sliceNumber++;
      // Uses LevelDbOutputWriter wrapping GoogleCloudStorageFileOutputWriter rather than
      // GoogleCloudStorageLevelDbOutputWriter because the padding at the end of the slice is
      // unneeded as the file is being finalized.
      return new MarshallingOutputWriter<>(
          new LevelDbOutputWriter(new GoogleCloudStorageFileOutputWriter(
              new GcsFilename(bucket, fileName), MapReduceConstants.REDUCE_INPUT_MIME_TYPE)),
          Marshallers.getKeyValuesMarshaller(identity, identity));
    }

    public GoogleCloudStorageFileSet finish() {
      return new GoogleCloudStorageFileSet(bucket, fileNames);
    }
  }

  public GoogleCloudStorageSortOutput(String bucket, String mrJobId) {
    this.bucket = checkNotNull(bucket, "Null bucket");
    this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
  }

  @Override
  public List<? extends OutputWriter<KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>>>
      createWriters(int shardCount) {
    List<SlicingOutputWriter<KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>, WriterCreatorImpl>>
        result = new ArrayList<>(shardCount);
    for (int i = 0; i < shardCount; i++) {
      String formatStringForShard =
          String.format(MapReduceConstants.SORT_OUTPUT_DIR_FORMAT, mrJobId, i);
      result.add(new SlicingOutputWriter<>(new WriterCreatorImpl(bucket, formatStringForShard)));
    }
    return result;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<GoogleCloudStorageFileSet> finish(Collection<? extends OutputWriter<
      KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>>> writers) {
    List<GoogleCloudStorageFileSet> filesByShard = new ArrayList<>(writers.size());
    for (OutputWriter<?> writer : writers) {
      filesByShard.add(((SlicingOutputWriter<?, WriterCreatorImpl>) writer).getCreator().finish());
    }
    return filesByShard;
  }
}
