package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.CorruptDataException;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.impl.proto.KeyValuePb;
import com.google.common.collect.AbstractIterator;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

/**
 * Marshalls KeyValue pairs where the value is an iterator.
 * Used to marshal and unmarshal data from the sort to the reducer.
 *
 * For internal use only. User code cannot safely depend on this class.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class KeyValuesMarshaller<K, V> extends Marshaller<KeyValue<K, ? extends Iterable<V>>> {
  private static final long serialVersionUID = -469910411827845614L;
  private final Marshaller<K> keyMarshaller;
  private final Marshaller<V> valueMarshaller;

  public KeyValuesMarshaller(Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller) {
    this.keyMarshaller = checkNotNull(keyMarshaller, "Null keyMarshaller");
    this.valueMarshaller = checkNotNull(valueMarshaller, "Null valueMarshaller");
  }

  @Override
  public ByteBuffer toBytes(KeyValue<K, ? extends Iterable<V>> keyValues) {
    KeyValuePb.KeyValues.Builder b = KeyValuePb.KeyValues.newBuilder();
    b.setKey(ByteString.copyFrom(keyMarshaller.toBytes(keyValues.getKey())));
    Iterable<V> values = keyValues.getValue();
    for (V value : values) {
      b.addValue(ByteString.copyFrom(valueMarshaller.toBytes(value)));
    }
    return ByteBuffer.wrap(b.build().toByteArray());
  }

  private final class ByteStringTranslatingIterator implements Iterable<V>, Serializable {

    private static final long serialVersionUID = 4745545835439721881L;

    private final Iterable<ByteString> source;

    private ByteStringTranslatingIterator(Iterable<ByteString> source) {
      this.source = source;
    }

    @Override
    public Iterator<V> iterator() {
      final Iterator<ByteString> iter = source.iterator();
      return new AbstractIterator<V>() {
        @Override
        protected V computeNext() {
          if (!iter.hasNext()) {
            return endOfData();
          }
          ByteString next = iter.next();
          return valueMarshaller.fromBytes(next.asReadOnlyByteBuffer());
        }
      };
    }
  }

  @Override
  public KeyValue<K, Iterable<V>> fromBytes(ByteBuffer input) {
    KeyValuePb.KeyValues proto;
    try {
      proto = KeyValuePb.KeyValues.parseFrom(ByteString.copyFrom(input));
    } catch (InvalidProtocolBufferException e) {
      throw new CorruptDataException(e);
    }
    K key = keyMarshaller.fromBytes(proto.getKey().asReadOnlyByteBuffer());
    List<ByteString> values = proto.getValueList();
    return KeyValue.<K, Iterable<V>>of(key, new ByteStringTranslatingIterator(values));
  }
}
