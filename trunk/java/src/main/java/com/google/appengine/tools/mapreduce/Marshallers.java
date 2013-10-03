// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.mapreduce.impl.KeyValueMarshaller;
import com.google.appengine.tools.mapreduce.impl.KeyValuesMarshaller;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.common.base.Charsets;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.util.Iterator;

/**
 * Some {@link Marshaller}s and related utilities.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class Marshallers {

  private Marshallers() {}

  private static class SerializationMarshaller<T extends Serializable> extends Marshaller<T> {
    private static final long serialVersionUID = 401446902678227352L;

    @Override 
    public ByteBuffer toBytes(T object) {
      return ByteBuffer.wrap(SerializationUtil.serializeToByteArrayNoHeader(object));
    }

    @SuppressWarnings("unchecked")
    @Override
    public T fromBytes(ByteBuffer in) {
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

  private static class StringMarshaller extends Marshaller<String> {
    private static final long serialVersionUID = -7496989898703029904L;

    @Override
    public ByteBuffer toBytes(String object) {
      return Charsets.UTF_8.encode(object);
    }

    @Override
    public String fromBytes(ByteBuffer b) {
      try {
        return Charsets.UTF_8.newDecoder().decode(b).toString();
      } catch (CharacterCodingException e) {
        throw new CorruptDataException("Could not decode string ", e);
      }
    }
  }
  
  /**
   * Returns a {@code Marshaller} for {@code String}s. They will be encoded in UTF-8.
   */
  public static Marshaller<String> getStringMarshaller() {
    return new StringMarshaller();
  }

  private static class LongMarshaller extends Marshaller<Long> {
    private static final long serialVersionUID = 646739857959433591L;

    @Override
    public ByteBuffer toBytes(Long x) {
      /* This xor is done to get an unsigned representation that sorts lexicographically */
      ByteBuffer out = ByteBuffer.allocate(8).putLong(x ^ Long.MIN_VALUE);
      out.rewind();
      return out;
    }

    @Override
    public Long fromBytes(ByteBuffer in) {
      if (in.remaining() != 8) {
        throw new CorruptDataException("Expected 8 bytes, not " + in.remaining());
      }
      in.order(ByteOrder.BIG_ENDIAN);
      return in.getLong() ^ Long.MIN_VALUE;
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

    @Override 
    public ByteBuffer toBytes(Integer x) {
      /* This xor is done to get an unsigned representation that sorts lexicographically */
      ByteBuffer out = ByteBuffer.allocate(4).putInt(x ^ Integer.MIN_VALUE);
      out.rewind();
      return out;
    }

    @Override
    public Integer fromBytes(ByteBuffer in) {
      if (in.remaining() != 4) {
        throw new CorruptDataException("Expected 4 bytes, not " + in.remaining());
      }
      in.order(ByteOrder.BIG_ENDIAN);
      return in.getInt() ^ Integer.MIN_VALUE;
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

    @Override 
    public ByteBuffer toBytes(Void x) {
      return ByteBuffer.wrap(EMPTY_BYTE_ARRAY);
    }

    @Override 
    public Void fromBytes(ByteBuffer in) {
      if (in.remaining() != 0) {
        throw new CorruptDataException("Expected 0 bytes, not " + in.remaining());
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

  /**
   * Does nothing. Is useful for cases where a marshaller is required but not wanted.
   */
  public static class ByteBufferMarshaller extends Marshaller<ByteBuffer> {
    private static final long serialVersionUID = -8188886996472169025L;
    
    @Override
    public ByteBuffer toBytes(ByteBuffer object) {
      return object.slice();
    }
    
    @Override
    public ByteBuffer fromBytes(ByteBuffer b)  {
      return b.slice();
    }
    
  }
  
  /**
   * Returns a {@code Marshaller} for {@code ByteBuffer}.
   */
  public static Marshaller<ByteBuffer> getByteBufferMarshaller() {
    return new ByteBufferMarshaller();
  }


  /**
   * Returns a {@code Marshaller} for key-value pairs based on
   * {@code keyMarshaller} and {@code valueMarshaller}.
   */
  public static <K, V> Marshaller<KeyValue<K, V>> getKeyValueMarshaller(Marshaller<K> keyMarshaller,
      Marshaller<V> valueMarshaller) {
    return new KeyValueMarshaller<K, V>(keyMarshaller, valueMarshaller);
  }
  
  /**
   * Returns a {@code Marshaller} for key-values pairs based on
   * {@code keyMarshaller} and {@code valueMarshaller}.
   */
  public static <K, V> Marshaller<KeyValue<K, Iterator<V>>> getKeyValuesMarshaller(
      Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller) {
    return new KeyValuesMarshaller<K, V>(keyMarshaller, valueMarshaller);
  }

}
