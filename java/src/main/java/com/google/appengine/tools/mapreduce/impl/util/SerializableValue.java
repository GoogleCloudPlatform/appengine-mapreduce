package com.google.appengine.tools.mapreduce.impl.util;

import com.google.appengine.tools.mapreduce.Marshaller;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

public final class SerializableValue<T> implements Serializable {

  private static final long serialVersionUID = -5188676157133889956L;

  private transient T value;
  private final Marshaller<T> marshaller;

  private SerializableValue(Marshaller<T> marshaller, T value) {
    this.marshaller = marshaller;
    this.value = value;
  }

  public static <T> SerializableValue<T> of(Marshaller<T> marshaller, T value) {
    return new SerializableValue<>(marshaller, value);
  }

  public T getValue() {
    return value;
  }

  private void readObject(ObjectInputStream aInputStream) throws ClassNotFoundException,
      IOException {
    aInputStream.defaultReadObject();
    value = marshaller.fromBytes(ByteBuffer.wrap((byte[]) aInputStream.readObject()));
  }

  private void writeObject(ObjectOutputStream aOutputStream) throws IOException {
    aOutputStream.defaultWriteObject();
    ByteBuffer byteBuffer = marshaller.toBytes(value);
    aOutputStream.writeObject(SerializationUtil.getBytes(byteBuffer.slice()));
    // In case marshalling modified the item
    value = marshaller.fromBytes(byteBuffer);
  }
}
