package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Sharder;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.LevelDbOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.MarshallingOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.ShardingOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.SliceSegmentingOutputWriter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Defines the way data is written out by the sorter. This consists of a single GCS file containing
 * LevelDb format to separate records which are KeyValue pairs. What the key and value are is not
 * known to the sorter, so these are byte arrays that are simply passed through from the mapper. A
 * new file is created at the beginning of each slice in case the data does not all fit in memory.
 *
 */
public class GoogleCloudStorageSortOutput extends
    Output<KeyValue<ByteBuffer, List<ByteBuffer>>, FilesByShard> {

  private static final long serialVersionUID = 8332978108336443982L;

  private final String bucket;
  private final String mrJobId;
  private final Sharder sharder;

  private static class ShardingOutputWriterImpl extends
      ShardingOutputWriter<ByteBuffer, List<ByteBuffer>, SlicingOutputWriterImpl> {

    private static final long serialVersionUID = -8890301705089321212L;
    private final String mrJobId;
    private final int shard;
    private final String bucket;

    ShardingOutputWriterImpl(String mrJobId, String bucket, int shard, Sharder sharder) {
      super(Marshallers.getByteBufferMarshaller(), sharder);
      this.mrJobId = mrJobId;
      this.bucket = bucket;
      this.shard = shard;
    }

    @Override
    public SlicingOutputWriterImpl createWriter(int number) {
      String formatStringForShard =
          String.format(MapReduceConstants.SORT_OUTPUT_DIR_FORMAT, mrJobId, shard, number);
      return new SlicingOutputWriterImpl(bucket, formatStringForShard);
    }

    @Override
    protected Map<Integer, SlicingOutputWriterImpl> getShardsToWriterMap() {
      return super.getShardsToWriterMap();
    }

    @Override
    public long estimateMemoryRequirement() {
      return Math.max(getShardsToWriterMap().size(), 1)
          * GoogleCloudStorageFileOutputWriter.MEMORY_REQUIRED;
    }
  }

  private static class SlicingOutputWriterImpl extends SliceSegmentingOutputWriter<
      KeyValue<ByteBuffer, List<ByteBuffer>>,
      MarshallingOutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>>> {

    private static final long serialVersionUID = -6765187605013451624L;

    private final String bucket;
    private final String fileNamePattern;
    private final List<String> fileNames;

    /**
     * @param fileNamePattern a Java format string {@link java.util.Formatter} containing one int
     *        argument for the slice number.
     */
    public SlicingOutputWriterImpl(String bucket, String fileNamePattern) {
      this.bucket = checkNotNull(bucket, "Null bucket");
      this.fileNamePattern = checkNotNull(fileNamePattern, "Null fileNamePattern");
      this.fileNames = new ArrayList<>();
    }

    @Override
    public MarshallingOutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>> createWriter(
        int sliceNumber) {
      String fileName = String.format(fileNamePattern, sliceNumber);
      boolean added = fileNames.add(fileName);
      checkArgument(added, "Create writer called twice for the same shard");
      Marshaller<ByteBuffer> identity = Marshallers.getByteBufferMarshaller();
      // Uses LevelDbOutputWriter wrapping GoogleCloudStorageFileOutputWriter rather than
      // GoogleCloudStorageLevelDbOutputWriter because the padding at the end of the slice is
      // unneeded as the file is being finalized.
      return new MarshallingOutputWriter<>(
          new LevelDbOutputWriter(new GoogleCloudStorageFileOutputWriter(
              new GcsFilename(bucket, fileName), MapReduceConstants.REDUCE_INPUT_MIME_TYPE)),
          Marshallers.getKeyValuesMarshaller(identity, identity));
    }

    @Override
    public long estimateMemoryRequirement() {
      return GoogleCloudStorageFileOutputWriter.MEMORY_REQUIRED;
    }

    public List<String> getFilesCreated() {
      return fileNames;
    }
  }

  public GoogleCloudStorageSortOutput(String bucket, String mrJobId, Sharder sharder) {
    this.bucket = checkNotNull(bucket, "Null bucket");
    this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
    this.sharder = checkNotNull(sharder, "Null sharder");
  }

  @Override
  public List<? extends OutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>>> createWriters(
      int shards) {
    List<OutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>>> result = new ArrayList<>(shards);
    for (int i = 0; i < shards; i++) {
      OutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>> shardingWriter =
          new ShardingOutputWriterImpl(mrJobId, bucket, i, sharder);
      result.add(shardingWriter);
    }
    return result;
  }

  @Override
  @SuppressWarnings("unchecked")
  public FilesByShard finish(
      Collection<? extends OutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>>> writers) {
    FilesByShard filesByShard = new FilesByShard(sharder.getNumShards(), bucket);
    for (OutputWriter<?> w : writers) {
      ShardingOutputWriterImpl writer = (ShardingOutputWriterImpl) w;
      for (Entry<Integer, SlicingOutputWriterImpl> shard :
          writer.getShardsToWriterMap().entrySet()) {
        filesByShard.addFilesToShard(shard.getKey(), shard.getValue().getFilesCreated());
      }
    }
    return filesByShard;
  }

}
