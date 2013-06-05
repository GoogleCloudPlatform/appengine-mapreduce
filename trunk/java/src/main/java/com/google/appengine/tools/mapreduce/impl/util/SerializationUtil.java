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

package com.google.appengine.tools.mapreduce.impl.util;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 *
 */
public class SerializationUtil {

  private SerializationUtil() {
  }

  /**
   * Returns an object whose toString() returns a presentation of {@code length} bytes
   * from {@code bytes} starting at {@code offset}.
   */
  public static Object prettyBytes(final byte[] bytes, final int offset, final int length) {
    return new Object() {
      @Override public String toString() {
        StringBuilder out = new StringBuilder("[");
        for (int i = offset; i < length; i++) {
          if (i != offset) {
            out.append(" ");
          }
          out.append(String.format("%02x", bytes[i]));
        }
        return out + "]@" + String.format("%x", System.identityHashCode(bytes));
      }
    };
  }

  public static Object prettyBytes(byte[] bytes) {
    return prettyBytes(bytes, 0, bytes.length);
  }

  public static Object prettyBytes(final ByteBuffer buf) {
    // Be lazy since getBytes() can be expensive.
    return new Object() {
      @Override public String toString() {
        return "" + prettyBytes(getBytes(buf));
      }
    };
  }

  public static Serializable deserializeFromByteArray(byte[] bytes) throws IOException {
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));
    try {
      return deserializeFromStream(in);
    } finally {
      in.close();
    }
  }

  private static InputStream newInputStream(final ByteBuffer buf) {
    return new InputStream() {
      @Override public int read() {
        if (!buf.hasRemaining()) {
          return -1;
        }
        return buf.get();
      }

      @Override public int read(byte[] bytes, int off, int len) {
        if (!buf.hasRemaining()) {
          return -1;
        }
        int toRead = Math.min(len, buf.remaining());
        buf.get(bytes, off, toRead);
        return toRead;
      }
    };
  }

  public static <T> T deserializeFromByteBufferNoHeader(ByteBuffer bytes) throws IOException {
    ObjectInputStream in =
        new ObjectInputStream(newInputStream(bytes)) {
          @Override protected void readStreamHeader() throws IOException {
            // do nothing
          }
        };
    try {
      T value = deserializeFromStream(in);
      if (in.read() != -1) {
        throw new IOException("Trailing bytes in " + bytes + " after reading " + value);
      }
      return value;
    } finally {
      in.close();
    }
  }

  public static <T> T deserializeFromByteArrayNoHeader(byte[] bytes) throws IOException {
    return deserializeFromByteBufferNoHeader(ByteBuffer.wrap(bytes));
  }

  public static <T> T deserializeFromStream(ObjectInputStream in) throws IOException {
    try {
      @SuppressWarnings("unchecked")
      T obj = (T) in.readObject();
      return obj;
    } catch (ClassNotFoundException e) {
      throw new IOException("Deserialization error", e);
    }
  }

  public static Serializable deserializeFromDatastoreProperty(Entity entity, String propertyName)
      throws IOException {
    return deserializeFromByteArray(((Blob) entity.getProperty(propertyName)).getBytes());
  }

  public static Serializable deserializeFromDatastorePropertyUnchecked(
      Entity entity, String propertyName) {
    try {
      return deserializeFromDatastoreProperty(entity, propertyName);
    } catch (IOException e) {
      throw new RuntimeException("Deserialization error", e);
    }
  }

  public static byte[] serializeToByteArray(Serializable o) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream(bytes);
      try {
        out.writeObject(o);
      } finally {
        out.close();
      }
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Can't serialize object: " + o, e);
    }
  }

  public static byte[] serializeToByteArrayNoHeader(Serializable o) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      ObjectOutputStream out =
          new ObjectOutputStream(bytes) {
            @Override protected void writeStreamHeader() throws IOException {
              // do nothing
            }
          };
      try {
        out.writeObject(o);
      } finally {
        out.close();
      }
      byte[] buf = bytes.toByteArray();
      return buf;
    } catch (IOException e) {
      throw new RuntimeException("Can't serialize object: " + o, e);
    }
  }

  public static byte[] getBytes(ByteBuffer in) {
    if (in.hasArray() && in.position() == 0
        && in.arrayOffset() == 0 && in.array().length == in.limit()) {
      return in.array();
    } else {
      byte[] buf = new byte[in.remaining()];
      in.get(buf);
      return buf;
    }
  }

}
