// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Some {@link Marshaller}s and related utilities.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class Marshallers {

  private Marshallers() {}

  private static class SerializationMarshaller<T extends Serializable> extends Marshaller<T> {
    private static final long serialVersionUID = 401446902678227352L;

    @Override public ByteBuffer toBytes(T object) {
      return ByteBuffer.wrap(SerializationUtil.serializeToByteArrayNoHeader(object));
    }

    @SuppressWarnings("unchecked")
    @Override public T fromBytes(ByteBuffer in) throws IOException {
      return (T) SerializationUtil.deserializeFromByteBufferNoHeader(in);
    }
  }

  /**
   * Returns a {@code Marshaller} that uses Java Serialization.  Works for any
   * type that implements {@link Serializable}, but is not space-efficient for
   * boxed primitives like {@link Long} or {@link Double}.
   */
  public static <T extends Serializable> Marshaller<T> getSerializationMarshaller() {
    return new SerializationMarshaller<T>();
  }

  /**
   * Returns a {@code Marshaller} for {@code String}s.  It is not specified what
   * encoding will be used.
   */
  // TODO(ohler): Replace this with getUtf8StringMarshaller.  Serialization
  // isn't that much worse in many cases (most characters from the Basic
  // Multilingual plane are the same as UTF-8), and the overhead (typecode etc.)
  // are just a few bytes.  But it uses "modified UTF-8", and we have no excuse
  // to encode U+0000 as two bytes and supplementary characters as six bytes
  // rather than four.
  //
  // In toBytes(), we might also want to compute the exact output array size
  // beforehand (or at least make a conservative estimate and then one copy)
  // rather than using a growing array.
  public static Marshaller<String> getStringMarshaller() {
    return new SerializationMarshaller<String>();
  }

  private static class LongMarshaller extends Marshaller<Long> {
    private static final long serialVersionUID = 646739857959433591L;

    @Override public ByteBuffer toBytes(Long x) {
      ByteBuffer out = ByteBuffer.allocate(8).putLong(x);
      out.rewind();
      return out;
    }

    @Override public Long fromBytes(ByteBuffer in) throws IOException {
      if (in.remaining() != 8) {
        throw new IOException("Expected 8 bytes, not " + in.remaining());
      }
      in.order(ByteOrder.BIG_ENDIAN);
      return in.getLong();
    }
  }

  /**
   * Returns a {@code Marshaller} for {@code Long}s that uses a more efficient
   * representation than {@link #getSerializationMarshaller}.
   */
  public static Marshaller<Long> getLongMarshaller() {
    return new LongMarshaller();
  }

  private static class IntegerMarshaller extends Marshaller<Integer> {
    private static final long serialVersionUID = 116841732914441971L;

    @Override public ByteBuffer toBytes(Integer x) {
      ByteBuffer out = ByteBuffer.allocate(4).putInt(x);
      out.rewind();
      return out;
    }

    @Override public Integer fromBytes(ByteBuffer in) throws IOException {
      if (in.remaining() != 4) {
        throw new IOException("Expected 4 bytes, not " + in.remaining());
      }
      in.order(ByteOrder.BIG_ENDIAN);
      return in.getInt();
    }
  }

  /**
   * Returns a {@code Marshaller} for {@code Integers}s that uses a more
   * efficient representation than {@link #getSerializationMarshaller}.
   */
  public static Marshaller<Integer> getIntegerMarshaller() {
    return new IntegerMarshaller();
  }

  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  private static class VoidMarshaller extends Marshaller<Void> {
    private static final long serialVersionUID = 534040781414531156L;

    @Override public ByteBuffer toBytes(Void x) {
      return ByteBuffer.wrap(EMPTY_BYTE_ARRAY);
    }

    @Override public Void fromBytes(ByteBuffer in) throws IOException {
      if (in.remaining() != 0) {
        throw new IOException("Expected 0 bytes, not " + in.remaining());
      }
      return null;
    }
  }

  /**
   * Returns a {@code Marshaller} for {@code Void}.
   */
  public static Marshaller<Void> getVoidMarshaller() {
    return new VoidMarshaller();
  }

  private static class KeyValueMarshaller<K, V> extends Marshaller<KeyValue<K, V>> {
    private static final long serialVersionUID = 494784801710376720L;

    private final Marshaller<K> keyMarshaller;
    private final Marshaller<V> valueMarshaller;

    private KeyValueMarshaller(Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller) {
      this.keyMarshaller = keyMarshaller;
      this.valueMarshaller = valueMarshaller;
    }

    @Override public ByteBuffer toBytes(KeyValue<K, V> pair) {
      ByteBuffer key = keyMarshaller.toBytes(pair.getKey());
      ByteBuffer value = valueMarshaller.toBytes(pair.getValue());
      ByteBuffer out = ByteBuffer.allocate(4 + key.remaining() + value.remaining());
      out.putInt(key.remaining());
      out.put(key);
      out.put(value);
      out.rewind();
      return out;
    }

    @Override public KeyValue<K, V> fromBytes(ByteBuffer in) throws IOException {
      in.order(ByteOrder.BIG_ENDIAN);
      int keyLength = in.getInt();
      int oldLimit = in.limit();
      K key;
      try {
        in.limit(in.position() + keyLength);
        key = keyMarshaller.fromBytes(in.slice());
      } finally {
        in.limit(oldLimit);
      }
      in.position(in.position() + keyLength);
      V value = valueMarshaller.fromBytes(in.slice());
      return KeyValue.of(key, value);
    }
  }

  /**
   * Returns a {@code Marshaller} for key-value pairs based on
   * {@code keyMarshaller} and {@code valueMarshaller}.
   */
  public static <K, V> Marshaller<KeyValue<K, V>> getKeyValueMarshaller(Marshaller<K> keyMarshaller,
      Marshaller<V> valueMarshaller) {
    return new KeyValueMarshaller<K, V>(keyMarshaller, valueMarshaller);
  }

}
