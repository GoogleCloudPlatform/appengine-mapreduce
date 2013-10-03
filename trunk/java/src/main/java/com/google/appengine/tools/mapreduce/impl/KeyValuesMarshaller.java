package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.files.FileServicePb;
import com.google.appengine.repackaged.com.google.protobuf.ByteString;
import com.google.appengine.repackaged.com.google.protobuf.InvalidProtocolBufferException;
import com.google.appengine.tools.mapreduce.CorruptDataException;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

/**
 * Marshalls KeyValue pairs where the value is an iterator. 
 * Used to marshal and unmarshal data from the sort to the reducer.
 *
 * @param <K> key type
 * @param <V> value type
 */
class KeyValuesMarshaller<K, V> extends Marshaller<KeyValue<K, Iterator<V>>> {
  private static final long serialVersionUID = -469910411827845614L;
  private final Marshaller<K> keyMarshaller;
  private final Marshaller<V> valueMarshaller;

  KeyValuesMarshaller(Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller) {
    this.keyMarshaller = checkNotNull(keyMarshaller, "Null keyMarshaller");
    this.valueMarshaller = checkNotNull(valueMarshaller, "Null valueMarshaller");
  }
  
  @Override
  public ByteBuffer toBytes(KeyValue<K, Iterator<V>> keyValues) {
    FileServicePb.KeyValues.Builder b = FileServicePb.KeyValues.newBuilder();
    b.setKey(ByteString.copyFrom(keyMarshaller.toBytes(keyValues.getKey())));
    Iterator<V> values = keyValues.getValue();
    while (values.hasNext()) {
      b.addValue(ByteString.copyFrom(valueMarshaller.toBytes(values.next())));
    }
    return ByteBuffer.wrap(b.build().toByteArray());
  }

  private final class ByteStringTranslatingIterator implements Iterator<V> {
    private final Iterator<ByteString> iter;

    private ByteStringTranslatingIterator(Iterator<ByteString> iter) {
      this.iter = iter;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public V next() {
      ByteString next = iter.next();
      return valueMarshaller.fromBytes(next.asReadOnlyByteBuffer());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public KeyValue<K, Iterator<V>> fromBytes(ByteBuffer input) {
    FileServicePb.KeyValues proto;
    try {
      proto = FileServicePb.KeyValues.parseFrom(ByteString.copyFrom(input));
    } catch (InvalidProtocolBufferException e) {
      throw new CorruptDataException(e);
    }
    K key = keyMarshaller.fromBytes(proto.getKey().asReadOnlyByteBuffer());
    List<ByteString> values = proto.getValueList();

    return new KeyValue<K, Iterator<V>>(
        key, new ByteStringTranslatingIterator(values.iterator()));
  }


}
