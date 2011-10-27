// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.files.FileServicePb.KeyValue;
import com.google.appengine.api.files.FileServicePb.KeyValues;
import com.google.appengine.repackaged.com.google.protobuf.ByteString;
import com.google.appengine.repackaged.com.google.protobuf.InvalidProtocolBufferException;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Predefined {@link Serializer}s for built-in types.
 *
 */
public final class Serializers {


  /**
   * Convenience method for creating {@link StringEncoder}.
   * @return an instance of {@link StringEncoder}.
   */
  public static Serializer<String> strings() {
    return new StringEncoder();
  }

  /**
   * Convenience method for creating {@link BooleanEncoder}.
   * @return an instance of {@link BooleanEncoder}.
   */
  public static Serializer<Boolean> bools() {
    return new BooleanEncoder();
  }

  /**
   * Convenience method for creating {@link ByteEncoder}.
   * @return an instance of {@link ByteEncoder}.
   */
  public static Serializer<Byte> bytes() {
    return new ByteEncoder();
  }

  /**
   * Convenience method for creating {@link ShortEncoder}.
   * @return an instance of {@link ShortEncoder}.
   */
  public static Serializer<Short> shorts() {
    return new ShortEncoder();
  }

  /**
   * Convenience method for creating {@link CharacterEncoder}.
   * @return an instance of {@link CharacterEncoder}.
   */
  public static Serializer<Character> chars() {
    return new CharacterEncoder();
  }

  /**
   * Convenience method for creating {@link IntegerEncoder}.
   * @return an instance of {@link IntegerEncoder}.
   */
  public static Serializer<Integer> ints() {
    return new IntegerEncoder();
  }

  /**
   * Convenience method for creating {@link FloatEncoder}.
   * @return an instance of {@link FloatEncoder}.
   */
  public static Serializer<Float> floats() {
    return new FloatEncoder();
  }

  /**
   * Convenience method for creating {@link LongEncoder}.
   * @return an instance of {@link LongEncoder}.
   */
  public static Serializer<Long> longs() {
    return new LongEncoder();
  }

  /**
   * Convenience method for creating {@link DoubleEncoder}.
   * @return an instance of {@link DoubleEncoder}.
   */
  public static Serializer<Double> doubles() {
    return new DoubleEncoder();
  }

  /**
   * Convenience method for creating {@link ByteArrayEncoder}.
   * @return an instance of {@link ByteArrayEncoder}.
   */
  public static Serializer<byte[]> byteArray() {
    return new ByteArrayEncoder();
  }

  /**
   * {@link Serializer} for {@link String} type.
   */
  public static final class StringEncoder implements Serializer<String> {

    /**
     * Encodes a {@link String} as a UTF-8 {@link ByteBuffer}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public ByteBuffer encode(String value) throws SerializerException {
      ByteBuffer result;
      try {
        return result = ByteBuffer.wrap(value.getBytes("UTF-8"));
      } catch (UnsupportedEncodingException e) {
        throw new SerializerException("Can't encode string because it doesn't support UTF-8.");
      }
    }

    /**
     * Decodes a {@link String} from a {@link ByteBuffer} encoded using UTF-8.
     * @see Serializer#decode(java.nio.ByteBuffer)
     */
    @Override
    public String decode(ByteBuffer buffer) throws SerializerException {
      byte[] b = new byte[buffer.remaining()]; // we have to make copy because buffer is readonly
      buffer.get(b, 0, buffer.remaining());
      try {
        return new String(b, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new SerializerException("Can't encode string because it doesn't support UTF-8.");
      }
    }
  }

  /**
   * {@link Serializer} for {@link Boolean} type.
   */
  public static final class BooleanEncoder implements Serializer<Boolean> {

    /**
     * Encodes a {@link Boolean}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public ByteBuffer encode(Boolean value) {
      ByteBuffer bb = ByteBuffer.allocate(1);
      bb.put((byte) (value ? 1 : 0));
      bb.flip();
      return bb;
    }

    /**
     * Decodes a {@link Boolean}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public Boolean decode(ByteBuffer buffer) throws SerializerException  {
      if (buffer.remaining() != 1) {
        throw new SerializerException("The ByteBuffer does't contain a Boolean.");
      }
      byte b = buffer.get();
      return b == 1;
    }
  }

  /**
   * {@link Serializer} for {@link Byte} type.
   */
  public static final class ByteEncoder implements Serializer<Byte> {

    /**
     * Encodes a {@link Byte}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public ByteBuffer encode(Byte value) {
      return (ByteBuffer) ByteBuffer.allocate(1).put(value).flip();
    }

    /**
     * Decodes a {@link Byte}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public Byte decode(ByteBuffer buffer) throws SerializerException {
      if (buffer.remaining() != 1) {
        throw new SerializerException("The ByteBuffer does't contain a Byte.");
      }
      return buffer.get();
    }

  }

  /**
   * {@link Serializer} for {@link Short} type.
   */
  public static final class ShortEncoder implements Serializer<Short> {

    /**
     * Encodes a {@link Short}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public ByteBuffer encode(Short value) {
      return (ByteBuffer) ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).flip();
    }

    /**
     * Decodes a {@link Short}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public Short decode(ByteBuffer buffer) throws SerializerException {
      if (buffer.remaining() != Short.SIZE / Byte.SIZE) {
        throw new SerializerException("The ByteBuffer does't contain a Short.");
      }
      return buffer.getShort();
    }
  }

  /**
   * {@link Serializer} for {@link Character} type.
   */
  public static final class CharacterEncoder implements Serializer<Character> {

    /**
     * Encodes a {@link Character}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public ByteBuffer encode(Character value) {
      return (ByteBuffer) ByteBuffer.allocate(Character.SIZE / Byte.SIZE).putChar(value).flip();
    }

    /**
     * Decodes a {@link Character}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public Character decode(ByteBuffer buffer) throws SerializerException {
      if (buffer.remaining() != Character.SIZE / Byte.SIZE) {
        throw new SerializerException("The ByteBuffer does't contain Character.");
      }
      return buffer.getChar();
    }
  }

  /**
   * {@link Serializer} for {@link Integer} type.
   */
  public static final class IntegerEncoder implements Serializer<Integer> {

    /**
     * Encodes a {@link Integer}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public ByteBuffer encode(Integer value) {
      return (ByteBuffer) ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).flip();
    }

    /**
     * Decodes a {@link Integer}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public Integer decode(ByteBuffer buffer) throws SerializerException {
      if (buffer.remaining() != Integer.SIZE / Byte.SIZE) {
        throw new SerializerException("The ByteBuffer does't contain a Integer.");
      }
      return buffer.getInt();
    }
  }

  /**
   * {@link Serializer} for {@link Float} type.
   */
  public static final class FloatEncoder implements Serializer<Float> {

    /**
     * Encodes a {@link Float}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public ByteBuffer encode(Float value) {
      return (ByteBuffer) ByteBuffer.allocate(Float.SIZE / Byte.SIZE).putFloat(value).flip();
    }

    /**
     * Decodes a {@link Float}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public Float decode(ByteBuffer buffer) throws SerializerException {
      if (buffer.remaining() != Float.SIZE / Byte.SIZE) {
        throw new SerializerException("The ByteBuffer does't contain a Float.");
      }
      return buffer.getFloat();
    }
  }

  /**
   * {@link Serializer} for {@link Long} type.
   */
  public static final class LongEncoder implements Serializer<Long> {

    /**
     * Encodes a {@link Long}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public ByteBuffer encode(Long value) {
      return (ByteBuffer) ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).flip();
    }

    /**
     * Decodes a {@link Long}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public Long decode(ByteBuffer buffer) throws SerializerException {
      if (buffer.remaining() != Long.SIZE / Byte.SIZE) {
        throw new SerializerException("The ByteBuffer does't contain a Long.");
      }
      return buffer.getLong();
    }
  }

  /**
   * {@link Serializer} for {@link Double} type.
   */
  public static final class DoubleEncoder implements Serializer<Double> {

    /**
     * Encodes a {@link Double}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public ByteBuffer encode(Double value) {
      return (ByteBuffer) ByteBuffer.allocate(Double.SIZE / Byte.SIZE).putDouble(value).flip();
    }

    /**
     * Decodes a {@link Double}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public Double decode(ByteBuffer buffer) throws SerializerException {
      if (buffer.remaining() != Double.SIZE / Byte.SIZE) {
        throw new SerializerException("The ByteBuffer does't contain a Double.");
      }
      return buffer.getDouble();
    }
  }

  /**
   * {@link Serializer} for {@code byte[]}  type.
   *
   */
  public static final class ByteArrayEncoder implements Serializer<byte[]> {

    /**
     * Encodes a {@link Double}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public ByteBuffer encode(byte[] value) {
      return ByteBuffer.wrap(Arrays.copyOf(value, value.length));
    }

    /**
     * Decodes a {@link Double}.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public byte[] decode(ByteBuffer buffer) {
      byte[] result = new byte[buffer.remaining()];
      buffer.get(result);
      return result;
    }
  }

  /**
   * {@link Serializer} for {@link KeyValue} protocol buffer.
   */
  public static final class KeyValueEncoder implements Serializer<KeyValue> {

    /**
     * Encodes a {@link KeyValue} protocol buffer.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public ByteBuffer encode(KeyValue type) {
      return type.toByteString().asReadOnlyByteBuffer();
    }

    /**
     * Decodes a {@link KeyValue} protocol buffer.
     * @see Serializer#decode(java.nio.ByteBuffer)
     */
    @Override
    public KeyValue decode(ByteBuffer buffer) throws SerializerException {
      try {
        return KeyValue.parseFrom(ByteString.copyFrom(buffer));
      } catch (InvalidProtocolBufferException exception) {
        throw new SerializerException("The ByteBuffer did not contain a valid " +
                                       "KeyValue Protocol Buffer.");
      }
    }
  }

  /**
   * {@link Serializer} for {@link KeyValue} protocol buffer.
   */
  public static final class KeyValuesEncoder implements Serializer<KeyValues> {

    /**
     * Encodes a {@link KeyValues} protocol buffer.
     * @see Serializer#encode(java.lang.Object)
     */
    @Override
    public ByteBuffer encode(KeyValues type) {
      return type.toByteString().asReadOnlyByteBuffer();
    }

    /**
     * Decodes a {@link KeyValues} protocol buffer.
     * @see Serializer#decode(java.nio.ByteBuffer)
     */
    @Override
    public KeyValues decode(ByteBuffer buffer) throws SerializerException {
      try {
        return KeyValues.parseFrom(ByteString.copyFrom(buffer));
      } catch (InvalidProtocolBufferException exception) {
        throw new SerializerException("The ByteBuffer did not contain a " +
                                       "valid KeyValues Protocol Buffer.");
      }
    }
  }
}
