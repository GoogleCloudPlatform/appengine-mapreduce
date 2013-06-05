// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.files.FileServicePb;
import com.google.appengine.api.files.RecordWriteChannel;
import com.google.appengine.repackaged.com.google.protobuf.ByteString;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.util.FileUtil;
import com.google.appengine.tools.mapreduce.impl.util.RetryHelper;
import com.google.appengine.tools.mapreduce.impl.util.RetryHelper.Body;
import com.google.appengine.tools.mapreduce.impl.util.RetryParams;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <K> type of intermediate keys
 * @param <V> type of intermediate values
 */
// NOTE(ohler): Given the set of RPCs that the file service currently offers,
// the file format used by the shuffle service is unreliable -- the file service
// doesn't give us a way to determine whether an append RPC succeeded, and the
// shuffle file format cannot detect duplicate data (so unconditionally retrying
// is not an option). (We can't tell whether an append RPC succeeded because
// (a) an append RPC that failed with a timeout could still have succeeded on
// the backend, and (b) the file service buffers data in memory, so even data
// from a successful append RPC might get lost if the file service process
// crashes or its machine loses its network connection.)
public class IntermediateOutput<K, V> extends Output<KeyValue<K, V>, List<AppEngineFile>> {
  private static final long serialVersionUID = 207899202516112458L;

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(IntermediateOutput.class.getName());
  
  private static final RetryParams backoffParams = new RetryParams();
  static { // Numbers chosen to have the total backoff time between 30 and 60 seconds.
    backoffParams.setInitialRetryDelayMillis(30);
    backoffParams.setRetryMaxAttempts(10);
  }

  private static final FileService FILE_SERVICE = FileServiceFactory.getFileService();

  private static class Writer<K, V> extends OutputWriter<KeyValue<K, V>> {
    private static final long serialVersionUID = 592636863384442324L;

    private final String mrJobId;
    private final int mapShardNumber;
    private final Marshaller<K> keyMarshaller;
    private final Marshaller<V> valueMarshaller;
    // We create the file lazily so that we don't make empty files if the mapper
    // never produces any output (could be a common case).
    private AppEngineFile file = null;
    // Populated in close() unless no file was created.
    private AppEngineFile fileReadHandle = null;

    private transient RecordWriteChannel channel;

    public Writer(String mrJobId, int mapShardNumber, Marshaller<K> keyMarshaller,
        Marshaller<V> valueMarshaller) {
      this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
      this.mapShardNumber = mapShardNumber;
      this.keyMarshaller = checkNotNull(keyMarshaller, "Null keyMarshaller");
      this.valueMarshaller = checkNotNull(valueMarshaller, "Null valueMarshaller");
    }

    private void ensureOpen() {
      if (channel != null) {
        // This only works if slices are <30 seconds. TODO(ohler): close and
        // reopen every 29 seconds. Better yet, change fileproxy to not require
        // the file to be open.
        return;
      }

      RetryHelper.runWithRetries(new Body<Void>() {
        @Override
        public Void run() throws IOException {
          if (file == null) {
            file = FILE_SERVICE.createNewBlobFile(MapReduceConstants.MAP_OUTPUT_MIME_TYPE,
                mrJobId + ": map output, shard " + mapShardNumber);
          }
          channel = FILE_SERVICE.openRecordWriteChannel(file, false);
          return null;
        }
      }, backoffParams);
    }

    @Override
    public void write(KeyValue<K, V> pair) throws IOException {
      ensureOpen();
      FileServicePb.KeyValue.Builder b = FileServicePb.KeyValue.newBuilder();
      b.setKey(ByteString.copyFrom(keyMarshaller.toBytes(pair.getKey())));
      b.setValue(ByteString.copyFrom(valueMarshaller.toBytes(pair.getValue())));
      write(ByteBuffer.wrap(b.build().toByteArray()));
    }

    private void write(final ByteBuffer b) {
      RetryHelper.runWithRetries(new Body<Void>() {
        @Override
        public Void run() throws IOException {
          try {
            channel.write(b, null);
          } catch (IOException e) {
            closeChannel();
            ensureOpen();
            throw e;
          }
          return null;
        }
      }, backoffParams);
    }

    @Override
    public void endSlice() throws IOException {
      if (channel != null) {
        closeChannel();
      }
    }

    private void closeChannel() {
      RetryHelper.runWithRetries(new Body<Void>() {
        @Override
        public Void run() throws IOException {
          channel.close();
          return null;
        }
      }, backoffParams);
    }

    @Override
    public void close() throws IOException {
      if (file != null) {
        fileReadHandle = FileUtil.ensureFinalized(file);
      }
    }
  }

  private final String mrJobId;
  private final int shardCount;
  private final Marshaller<K> keyMarshaller;
  private final Marshaller<V> valueMarshaller;

  public IntermediateOutput(
      String mrJobId, int shardCount, Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller) {
    this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
    this.shardCount = shardCount;
    this.keyMarshaller = checkNotNull(keyMarshaller, "Null keyMarshaller");
    this.valueMarshaller = checkNotNull(valueMarshaller, "Null valueMarshaller");
  }

  @Override
  public List<? extends OutputWriter<KeyValue<K, V>>> createWriters() {
    ImmutableList.Builder<Writer<K, V>> out = ImmutableList.builder();
    for (int i = 0; i < shardCount; i++) {
      out.add(new Writer<K, V>(mrJobId, i, keyMarshaller, valueMarshaller));
    }
    return out.build();
  }

  @Override
  public List<AppEngineFile> finish(List<? extends OutputWriter<KeyValue<K, V>>> writers) {
    ImmutableList.Builder<AppEngineFile> out = ImmutableList.builder();
    for (OutputWriter<KeyValue<K, V>> w : writers) {
      @SuppressWarnings("unchecked")
      Writer<K, V> writer = (Writer<K, V>) w;
      if (writer.fileReadHandle != null) {
        out.add(writer.fileReadHandle);
      }
    }
    return out.build();
  }

}
