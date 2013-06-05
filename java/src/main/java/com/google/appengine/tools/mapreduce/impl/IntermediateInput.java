// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.files.FileServicePb.KeyValues;
import com.google.appengine.api.files.RecordReadChannel;
import com.google.appengine.repackaged.com.google.protobuf.ByteString;
import com.google.appengine.repackaged.com.google.protobuf.InvalidProtocolBufferException;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.ReducerInput;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

/**
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <K>
 * @param <V>
 */
public class IntermediateInput<K, V> extends Input<KeyValue<K, ReducerInput<V>>> {
  private static final long serialVersionUID = 970064558391741661L;

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(IntermediateInput.class.getName());

  private static final FileService FILE_SERVICE = FileServiceFactory.getFileService();

  /*VisibleForTesting*/
  static class Reader<K, V> extends InputReader<KeyValue<K, ReducerInput<V>>> {
    private static final long serialVersionUID = 993392238306084318L;

    private final AppEngineFile file;
    private long posBytes = 0;
    private Marshaller<K> keyMarshaller;
    private Marshaller<V> valueMarshaller;

    private transient RecordReadChannel channel;

    Reader(AppEngineFile file,
        Marshaller<K> keyMarshaller,
        Marshaller<V> valueMarshaller) {
      this.file = checkNotNull(file, "Null file");
      this.keyMarshaller = checkNotNull(keyMarshaller, "Null keyMarshaller");
      this.valueMarshaller = checkNotNull(valueMarshaller, "Null valueMarshaller");
    }

    @Override public String toString() {
      return getClass().getSimpleName() + "(" + file + " at position " + posBytes + ")";
    }

    @Override public Double getProgress() {
      // TODO(ohler): Implement this.  FileService doesn't currently have a way
      // to get the file size, though.
      return null;
    }

    private class IteratorIterator extends AbstractIterator<Iterator<V>> {
      private final ByteString expectedKey;
      private boolean previousWasPartial = true;

      IteratorIterator(ByteString expectedKey) {
        this.expectedKey = checkNotNull(expectedKey, "Null expectedKey");
      }

      @Override public String toString() {
        return getClass().getSimpleName() + "(" + Reader.this + ")";
      }

      @Override protected Iterator<V> computeNext() {
        if (!previousWasPartial) {
          return endOfData();
        }
        KeyValues proto = readProto();
        Preconditions.checkState(proto != null,
            "%s: Unexpected EOF, previous KeyValues was partial; key=%s", this, expectedKey);
        Preconditions.checkState(expectedKey.equals(proto.getKey()),
            "%s: Expected key %s, got %s", this, expectedKey, proto.getKey());
        previousWasPartial = proto.getPartial();
        return makeIterator(proto);
      }
    }

    /*VisibleForTesting*/ RecordReadChannel openChannel() {
      try {
        return FILE_SERVICE.openRecordReadChannel(file, false);
      } catch (IOException e) {
        throw new RuntimeException(this + ": opening read channel failed", e);
      }
    }

    private void ensureOpen() {
      // RecordReadChannel can't be closed, so we don't.
      //
      // TODO(ohler): reopen only every 29 seconds, not every time.  Better yet,
      // change fileproxy to no longer require the file to be open, and don't
      // reopen at all.
      channel = openChannel();
      try {
        channel.position(posBytes);
      } catch (IOException e) {
        throw new RuntimeException(this + ": position(" + posBytes + ") failed", e);
      }
    }

    /*Nullable*/ private KeyValues readProto() {
      ensureOpen();
      ByteBuffer record;
      try {
        record = channel.readRecord();
      } catch (EOFException e) {
        record = null;
      } catch (IOException e) {
        throw new RuntimeException(this + ": Failed to read record", e);
      }
      try {
        posBytes = channel.position();
      } catch (IOException e) {
        throw new RuntimeException(this + ": Failed to get position()", e);
      }
      if (record == null) {
        return null;
      }
      byte[] bytes = SerializationUtil.getBytes(record);
      try {
        return KeyValues.parseFrom(bytes);
      } catch (InvalidProtocolBufferException e) {
        // TODO(ohler): abort mapreduce
        throw new RuntimeException(this + ": Failed to parse protobuf: "
            + SerializationUtil.prettyBytes(bytes), e);
      }
    }

    private Iterator<V> makeIterator(KeyValues proto) {
      return Iterators.transform(proto.getValueList().iterator(),
          new Function<ByteString, V>() {
            @Override public V apply(ByteString in) {
              try {
                return valueMarshaller.fromBytes(in.asReadOnlyByteBuffer());
              } catch (IOException e) {
                // TODO(ohler): abort mapreduce
                throw new RuntimeException(
                    Reader.this + ": " + valueMarshaller + " failed to parse value: " + in, e);
              }
            }
          });
    }

    @Override public KeyValue<K, ReducerInput<V>> next() {
      KeyValues proto = readProto();
      if (proto == null) {
        throw new NoSuchElementException();
      }
      K key;
      try {
        key = keyMarshaller.fromBytes(proto.getKey().asReadOnlyByteBuffer());
      } catch (IOException e) {
        // TODO(ohler): abort mapreduce
        throw new RuntimeException(
            this + ": " + keyMarshaller + " failed to parse key from " + proto, e);
      }
      return KeyValue.of(key,
          ReducerInputs.fromIterator(
              Iterators.concat(makeIterator(proto),
                  proto.getPartial()
                  ? Iterators.concat(new IteratorIterator(proto.getKey()))
                  : Iterators.<V>emptyIterator())));
    }
  }

  private final List<AppEngineFile> files;
  private final Marshaller<K> keyMarshaller;
  private final Marshaller<V> valueMarshaller;

  public IntermediateInput(List<AppEngineFile> files,
      Marshaller<K> keyMarshaller,
      Marshaller<V> valueMarshaller) {
    // NOTE: When the MR framework constructs this input and calls
    // createReaders(), the files are not yet finalized, but they will be
    // finalized before readers are used (obviously).  This simplifies the data
    // flow in the MR framework slightly, and is no additional burden on this
    // class.
    this.files = ImmutableList.copyOf(files);
    this.keyMarshaller = checkNotNull(keyMarshaller, "Null keyMarshaller");
    this.valueMarshaller = checkNotNull(valueMarshaller, "Null valueMarshaller");
  }

  @Override public List<? extends InputReader<KeyValue<K, ReducerInput<V>>>> createReaders() {
    ImmutableList.Builder<Reader<K, V>> out = ImmutableList.builder();
    for (AppEngineFile file : files) {
      out.add(new Reader<K, V>(file, keyMarshaller, valueMarshaller));
    }
    return out.build();
  }

}
