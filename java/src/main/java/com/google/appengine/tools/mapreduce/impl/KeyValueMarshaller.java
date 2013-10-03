package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.files.FileServicePb;
import com.google.appengine.repackaged.com.google.protobuf.ByteString;
import com.google.appengine.repackaged.com.google.protobuf.InvalidProtocolBufferException;
import com.google.appengine.tools.mapreduce.CorruptDataException;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.apphosting.api.AppEngineInternal;

import java.nio.ByteBuffer;

/**
 * Marshalls KeyValue pairs given a Marshaller for the Key and the Value.
 * Used to marshall and unmarshall data from the Mapper to the Sort.
 * 
 * @param <K> key type
 * @param <V> value type
 */
@AppEngineInternal
public class KeyValueMarshaller<K, V> extends Marshaller<KeyValue<K, V>> {

  private static final long serialVersionUID = 4804959968008959514L;
  private final Marshaller<K> keyMarshaller;
  private final Marshaller<V> valueMarshaller;

  public KeyValueMarshaller(Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller) {
    this.keyMarshaller = checkNotNull(keyMarshaller, "Null keyMarshaller");
    this.valueMarshaller = checkNotNull(valueMarshaller, "Null valueMarshaller");
  }

  @Override
  public ByteBuffer toBytes(KeyValue<K, V> keyValues) {
    FileServicePb.KeyValues.Builder b = FileServicePb.KeyValues.newBuilder();
    b.setKey(ByteString.copyFrom(keyMarshaller.toBytes(keyValues.getKey())));
    b.addValue(ByteString.copyFrom(valueMarshaller.toBytes(keyValues.getValue())));
    return ByteBuffer.wrap(b.build().toByteArray());
  }

  @Override
  public KeyValue<K, V> fromBytes(ByteBuffer input) {
    FileServicePb.KeyValues proto;
    try {
      proto = FileServicePb.KeyValues.parseFrom(ByteString.copyFrom(input));
    } catch (InvalidProtocolBufferException e) {
      throw new CorruptDataException(e);
    }
    K key = keyMarshaller.fromBytes(proto.getKey().asReadOnlyByteBuffer());
    V value = valueMarshaller.fromBytes(proto.getValue(0).asReadOnlyByteBuffer());
    return new KeyValue<K, V>(key, value);
  }

}
