/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.mapreduce.v2.impl.Writables;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.logging.Logger;

/**
 * Utility methods for serializing and deserializing with an arbitrary
 * serializer registered with {@link SerializationFactory}.
 *
 */
public class SerializationUtil {
  private static final Logger log = Logger.getLogger(SerializationUtil.class.getName());

  private SerializationUtil() {
  }

  @SuppressWarnings("unchecked")
  private static ByteArrayOutputStream serializeToByteArrayOutputStream(Configuration conf,
      Object toSerialize) {
    SerializationFactory serializationFactory = new SerializationFactory(conf);
    Serializer serializer = serializationFactory.getSerializer(toSerialize.getClass());
    ByteArrayOutputStream serializationStream = new ByteArrayOutputStream();
    try {
      serializer.open(serializationStream);
      serializer.serialize(toSerialize);
      return serializationStream;
    } catch (IOException ioe) {
      throw new RuntimeException(
          "Got an IOException from a ByteArrayOutputStream. This should never happen.", ioe);
    }
  }

  public static byte[] serializeToByteArray(Configuration conf, Object toSerialize) {
    return serializeToByteArrayOutputStream(conf, toSerialize).toByteArray();
  }

  /**
   * Serialize an object to a string. This differs from
   * {@link Writables#createStringFromWritable(org.apache.hadoop.io.Writable)}
   * because it uses {@code conf}'s serialization preferences to support arbitrary
   * serialization mechanisms using {@link SerializationFactory}.
   *
   * @param conf a Configuration containing any serialization preferences
   * @param toSerialize the object to serialize
   * @return the serialized string
   */
  @SuppressWarnings("unchecked")
  public static String serializeToString(Configuration conf, Object toSerialize) {
    try {
      return serializeToByteArrayOutputStream(conf, toSerialize).toString("UTF8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Your JDK doesn't understand UTF8", e);
    }
  }

  /**
   * Deserialize an object from a byte array. This uses {@code conf}'s
   * serialization preferences to support arbitrary serialization mechanisms
   * using {@link SerializationFactory}.
   *
   * @param conf the configuration to use for serialization preferences
   * @param expectedClass a type token to set the return type
   * @param className the name of the class to deserialize to
   * @param toDeserialize the serialized object as a byte array
   * @param initialState an object with initial state. Some deserializers may
   * throw this away. You can pass {@code null} to signify that there is no
   * initial state.
   */
  @SuppressWarnings("unchecked")
  public static <T> T deserializeFromByteArray(Configuration conf, Class<T> expectedClass,
      String className, byte[] toDeserialize, T initialState) {
    log.fine("Trying to deserialize: " + className);
    SerializationFactory serializationFactory = new SerializationFactory(conf);
    try {
      Class<?> deserializationClass = conf.getClassByName(className);
      if (!expectedClass.isAssignableFrom(deserializationClass)) {
        throw new ClassCastException("Attempted to deserialize a "
            + deserializationClass.getCanonicalName() + " but expected a "
            + expectedClass.getCanonicalName());
      }
      Deserializer<T> deserializer =
        serializationFactory.getDeserializer(
            (Class <T>) deserializationClass);
      ByteArrayInputStream inputStream = new ByteArrayInputStream(toDeserialize);
      deserializer.open(inputStream);
      return deserializer.deserialize(initialState);
    } catch (ClassNotFoundException e) {
      // If we're deserializing, then we should have already seen this class
      // and this is strictly a programming error. Hence the RuntimeException.
      throw new RuntimeException("Couldn't get class for deserializing " + className, e);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("JDK doesn't understand UTF8", e);
    } catch (IOException e) {
      throw new RuntimeException(
          "Got an IOException from a ByteArrayInputStream. This should never happen.", e);
    }
  }

  /**
   * Deserialize an object from a string. This differs from
   * {@link Writables#initializeWritableFromString(String, org.apache.hadoop.io.Writable)
   * because it uses {@code conf}'s serialization preferences to support
   * arbitrary serialization mechanisms using {@link SerializationFactory}.
   *
   * @param conf the configuration to use for serialization preferences
   * @param expectedClass a type token to set the return type
   * @param className the name of the class to deserialize to
   * @param toDeserialize the serialized object as a string
   * @param initialState an object with initial state. Some deserializers may
   * throw this away. You can pass {@code null} to signify that there is no
   * initial state.
   */
  @SuppressWarnings("unchecked")
  public static <T> T deserializeFromString(Configuration conf, Class<T> expectedClass,
      String className, String toDeserialize, T initialState) {
    try {
      return deserializeFromByteArray(conf, expectedClass, className,
          toDeserialize.getBytes("UTF8"), initialState);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Your JDK doesn't understand UTF8", e);
    }
  }
}
