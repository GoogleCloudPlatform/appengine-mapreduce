// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.files.FileServicePb;
import com.google.appengine.api.files.FileServicePb.KeyValues;
import com.google.appengine.api.files.RecordReadChannel;
import com.google.appengine.api.files.RecordWriteChannel;
import com.google.appengine.repackaged.com.google.protobuf.ByteString;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.MapReduceSpecification;
import com.google.appengine.tools.mapreduce.impl.util.FileUtil;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.appengine.tools.pipeline.Job3;
import com.google.appengine.tools.pipeline.Value;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
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
public class InMemoryShuffleJob<K, V, O>
    extends Job3<ShuffleResult<K, V, O>, List<AppEngineFile>,
        List<AppEngineFile>, ShuffleResult<K, V, O>> {
  private static final long serialVersionUID = 176754702347404887L;

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(InMemoryShuffleJob.class.getName());

  private static final FileService FILE_SERVICE = FileServiceFactory.getFileService();

  private final String mrJobId;
  private final MapReduceSpecification<?, K, V, O, ?> mrSpec;

  InMemoryShuffleJob(String mrJobId,
      MapReduceSpecification<?, K, V, O, ?> mrSpec) {
    this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
    this.mrSpec = checkNotNull(mrSpec, "Null mrSpec");
  }

  private List<KeyValue<K, V>> readInput(AppEngineFile file) throws IOException {
    ImmutableList.Builder<KeyValue<K, V>> out = ImmutableList.builder();
    RecordReadChannel in = new NonSpammingRecordReadChannel(
        FILE_SERVICE.openReadChannel(file, false));
    while (true) {
      ByteBuffer record = in.readRecord();
      if (record == null) {
        break;
      }
      byte[] bytes = SerializationUtil.getBytes(record);
      FileServicePb.KeyValue proto = FileServicePb.KeyValue.parseFrom(bytes);
      K key = mrSpec.getIntermediateKeyMarshaller()
          .fromBytes(proto.getKey().asReadOnlyByteBuffer());
      V value = mrSpec.getIntermediateValueMarshaller()
          .fromBytes(proto.getValue().asReadOnlyByteBuffer());
      out.add(KeyValue.of(key, value));
    }
    return out.build();
  }

  private List<List<KeyValue<K, V>>> readInputs(List<AppEngineFile> files) throws IOException {
    ImmutableList.Builder<List<KeyValue<K, V>>> out = ImmutableList.builder();
    for (AppEngineFile file : files) {
      out.add(readInput(file));
    }
    return out.build();
  }

  private void writeOutput(AppEngineFile file, List<KeyValue<K, List<V>>> data)
      throws IOException {
    RecordWriteChannel out = FILE_SERVICE.openRecordWriteChannel(file, false);
    for (KeyValue<K, List<V>> item : data) {
      KeyValues.Builder kv = KeyValues.newBuilder();
      kv.setKey(
          ByteString.copyFrom(mrSpec.getIntermediateKeyMarshaller().toBytes(item.getKey())));
      // We never generate KeyValues with the "partial" field set since we
      // know the whole dataset fits in memory.
      for (V value : item.getValue()) {
        kv.addValue(
            ByteString.copyFrom(mrSpec.getIntermediateValueMarshaller().toBytes(value)));
      }
      out.write(ByteBuffer.wrap(kv.build().toByteArray()), null);
    }
  }

  private void writeOutputs(List<AppEngineFile> files, List<List<KeyValue<K, List<V>>>> data)
      throws IOException {
    Preconditions.checkArgument(files.size() == data.size(),
        "%s != %s", files.size(), data.size());
    for (int i = 0; i < files.size(); i++) {
      writeOutput(files.get(i), data.get(i));
    }
  }

  @Override public Value<ShuffleResult<K, V, O>> run(List<AppEngineFile> mapOutputs,
      List<AppEngineFile> reduceInputs, ShuffleResult<K, V, O> shuffleResult) {
    try {
      List<List<KeyValue<K, V>>> in = readInputs(mapOutputs);
      List<List<KeyValue<K, List<V>>>> out = Shuffling.shuffle(in,
          mrSpec.getIntermediateKeyMarshaller(), reduceInputs.size());
      writeOutputs(reduceInputs, out);
    } catch (IOException e) {
      // TODO(ohler): retry
      throw new RuntimeException(this + ": IOException while shuffling", e);
    }
    for (AppEngineFile file : shuffleResult.getReducerInputFiles()) {
      FileUtil.ensureFinalized(file);
    }
    return immediate(shuffleResult);
  }

}
