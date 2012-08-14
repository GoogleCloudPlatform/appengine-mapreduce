// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.files.FileServicePb;
import com.google.appengine.api.files.FileServicePb.KeyValues;
import com.google.appengine.api.files.KeyOrderingException;
import com.google.appengine.api.files.RecordReadChannel;
import com.google.appengine.api.files.RecordWriteChannel;
import com.google.appengine.repackaged.com.google.protobuf.ByteString;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.MapReduceSpecification;
import com.google.appengine.tools.mapreduce.impl.util.FileUtil;
import com.google.appengine.tools.mapreduce.impl.util.RetryHelper;
import com.google.appengine.tools.mapreduce.impl.util.RetryHelper.Body;
import com.google.appengine.tools.mapreduce.impl.util.RetryParams;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.appengine.tools.pipeline.Job3;
import com.google.appengine.tools.pipeline.Value;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.logging.Logger;

/**
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <K> type of intermediate keys
 * @param <V> type of intermediate values
 * @param <O> type of output values
 */
public class InMemoryShuffleJob<K, V, O> extends
    Job3<ShuffleResult<K, V, O>, List<AppEngineFile>, List<AppEngineFile>, ShuffleResult<K, V, O>> {
  private static final long serialVersionUID = -3780368160890548335L;

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(InMemoryShuffleJob.class.getName());

  private transient FileService fileService;

  private final MapReduceSpecification<?, K, V, O, ?> mrSpec;

  InMemoryShuffleJob(MapReduceSpecification<?, K, V, O, ?> mrSpec) {
    this(mrSpec, FileServiceFactory.getFileService());
  }

  /**
   * @param fileService This does not survive serialization. Rather this is intended so that tests
   *        can override the fileService.
   */
  @VisibleForTesting
  InMemoryShuffleJob(MapReduceSpecification<?, K, V, O, ?> mrSpec, FileService fileService) {
    this.mrSpec = checkNotNull(mrSpec, "Null mrSpec");
    this.fileService = fileService;
  }

  private void readObject(ObjectInputStream inputStream)
      throws IOException, ClassNotFoundException {
    inputStream.defaultReadObject();
    fileService = FileServiceFactory.getFileService();
  }

  private final class ReadRecord implements Body<ByteBuffer> {
    final AppEngineFile file;
    RecordReadChannel in;
    long position;

    ReadRecord(AppEngineFile file, RecordReadChannel in, long position) {
      this.file = file;
      this.in = in;
      this.position = position;
    }

    @Override
    public ByteBuffer run() throws IOException {
      if (in == null) {
        in = fileService.openRecordReadChannel(file, false);
        try {
          in.position(position);
        } catch (IOException e) {
          in = null;
          throw e;
        }
      }
      ByteBuffer result;
      try {
        result = in.readRecord();
      } catch (IOException e) {
        in = null;
        throw e;
      }
      try {
        position = in.position();
      } catch (IOException e) {
        in = null;
        throw new RuntimeException("Failed to get position in file: " + file, e);
      }
      return result;
    }
  }

  private List<KeyValue<K, V>> readInput(AppEngineFile file) {
    RetryParams backoffParams = new RetryParams();
    backoffParams.setInitialRetryDelayMillis(10);
    backoffParams.setRetryDelayBackoffFactor(2);
    backoffParams.setRetryMaxAttempts(10);

    ImmutableList.Builder<KeyValue<K, V>> out = ImmutableList.builder();
    ReadRecord reader = new ReadRecord(file, null, 0);

    while (true) {
      ByteBuffer record = RetryHelper.runWithRetries(reader, backoffParams);
      if (record == null) {
        break;
      }
      byte[] bytes = SerializationUtil.getBytes(record);
      try {
        FileServicePb.KeyValue proto = FileServicePb.KeyValue.parseFrom(bytes);
        K key =
            mrSpec.getIntermediateKeyMarshaller().fromBytes(proto.getKey().asReadOnlyByteBuffer());
        V value = mrSpec.getIntermediateValueMarshaller()
            .fromBytes(proto.getValue().asReadOnlyByteBuffer());
        out.add(KeyValue.of(key, value));
      } catch (IOException e) {
        throw new RuntimeException(this
            + " Failed to parse mapper output; bug in marshaller or corruption in file " + file
            + " before position " + reader.position, e);
      }
      reader = new ReadRecord(file, reader.in, reader.position);
    }
    return out.build();
  }



  private List<List<KeyValue<K, V>>> readInputs(List<AppEngineFile> files) {
    ImmutableList.Builder<List<KeyValue<K, V>>> out = ImmutableList.builder();
    for (AppEngineFile file : files) {
      List<KeyValue<K, V>> read = readInput(file);
      out.add(read);
    }
    return out.build();
  }


  private final class WriteRecord implements Body<Void> {
    final AppEngineFile file;
    RecordWriteChannel out;
    ByteBuffer data;
    int sequence;

    WriteRecord(AppEngineFile file, RecordWriteChannel out, ByteBuffer data, int sequence) {
      this.file = file;
      this.out = out;
      this.data = data;
      this.sequence = sequence;
    }

    @Override
    public Void run() throws IOException {
      if (out == null) {
        out = fileService.openRecordWriteChannel(file, false);
      }
      try {
        String asString = String.format("%010d", sequence);
        out.write(data, asString);
        return null;
      } catch (KeyOrderingException e) {
        return null; // Already written
      } catch (IOException e) {
        out = null;
        throw e;
      }
    }
  }

  private void writeOutput(AppEngineFile file, List<KeyValue<K, List<V>>> items) {
    RetryParams backoffParams = new RetryParams();
    backoffParams.setInitialRetryDelayMillis(10);
    backoffParams.setRetryDelayBackoffFactor(2);
    backoffParams.setRetryMaxAttempts(10);

    WriteRecord writer = new WriteRecord(file, null, null, 0);
    int i = 0;
    for (KeyValue<K, List<V>> item : items) {
      KeyValues.Builder kv = KeyValues.newBuilder();
      kv.setKey(ByteString.copyFrom(mrSpec.getIntermediateKeyMarshaller().toBytes(item.getKey())));
      // We never generate KeyValues with the "partial" field set since we
      // know the whole dataset fits in memory.
      for (V value : item.getValue()) {
        kv.addValue(ByteString.copyFrom(mrSpec.getIntermediateValueMarshaller().toBytes(value)));
      }
      ByteBuffer data = ByteBuffer.wrap(kv.build().toByteArray());
      writer = new WriteRecord(file, writer.out, data, i);
      RetryHelper.runWithRetries(writer, backoffParams);
      i++;
    }
  }

  private void writeOutputs(List<AppEngineFile> files, List<List<KeyValue<K, List<V>>>> data) {
    Preconditions.checkArgument(files.size() == data.size(), "%s != %s", files.size(), data.size());
    for (int i = 0; i < files.size(); i++) {
      writeOutput(files.get(i), data.get(i));
    }
  }

  @Override
  public Value<ShuffleResult<K, V, O>> run(List<AppEngineFile> mapOutputs,
      List<AppEngineFile> reduceInputs, ShuffleResult<K, V, O> shuffleResult) {
    List<List<KeyValue<K, V>>> in = readInputs(mapOutputs);
    List<List<KeyValue<K, List<V>>>> out =
        Shuffling.shuffle(in, mrSpec.getIntermediateKeyMarshaller(), reduceInputs.size());
    writeOutputs(reduceInputs, out);
    for (AppEngineFile file : shuffleResult.getReducerInputFiles()) {
      FileUtil.ensureFinalized(file);
    }
    return immediate(shuffleResult);
  }


}
