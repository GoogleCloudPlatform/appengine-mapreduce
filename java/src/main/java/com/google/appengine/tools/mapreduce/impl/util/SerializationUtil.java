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
import com.google.appengine.tools.mapreduce.CorruptDataException;
import com.google.appengine.tools.mapreduce.Marshaller;

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

  public static Serializable deserializeFromByteArray(byte[] bytes) {
    try {
      ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));
      try {
        return deserializeFromStream(in);
      } finally {
        in.close();
      }
    } catch (IOException e) {
      throw new CorruptDataException("Deserialization error", e);
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

  public static <T> T deserializeFromByteBufferNoHeader(ByteBuffer bytes) {
    ObjectInputStream in = null;
    CorruptDataException e = null;
    try {
      in = new ObjectInputStream(newInputStream(bytes)) {
        @Override
        protected void readStreamHeader() {
          // do nothing
        }
      };
      T value = deserializeFromStream(in);
      if (in.read() != -1) {
        throw new CorruptDataException("Trailing bytes in " + bytes + " after reading " + value);
      }
      return value;
    } catch (IOException e1) {
      e = new CorruptDataException(e1);
      throw e;
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e2) {
          if (e == null) {
            throw new RuntimeException(e2);
          } else {
            throw e;
          }
        }
      }
    }
  }

  public static <T> T deserializeFromByteArrayNoHeader(byte[] bytes) {
    return deserializeFromByteBufferNoHeader(ByteBuffer.wrap(bytes));
  }

  public static <T> T deserializeFromStream(ObjectInputStream in) {
    try {
      @SuppressWarnings("unchecked")
      T obj = (T) in.readObject();
      return obj;
    } catch (ClassNotFoundException | IOException e) {
      throw new CorruptDataException("Deserialization error", e);
    }
  }

  public static <T extends Serializable> T deserializeFromDatastoreProperty(
      Entity entity, String propertyName) {
    @SuppressWarnings("unchecked")
    T obj = (T) deserializeFromByteArray(((Blob) entity.getProperty(propertyName)).getBytes());
    return obj;
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
            @Override protected void writeStreamHeader() {
              // do nothing
            }
          };
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

  public static byte[] getBytes(ByteBuffer in) {
    if (in.hasArray() && in.position() == 0
        && in.arrayOffset() == 0 && in.array().length == in.limit()) {
      return in.array();
    } else {
      byte[] buf = new byte[in.remaining()];
      int position = in.position();
      in.get(buf);
      in.position(position);
      return buf;
    }
  }

  public static <T> void writeObjectToOutputStreamUsingMarshaller(T object,
      Marshaller<T> marshaller, ObjectOutputStream oout) throws IOException {
    if (object == null) {
      oout.writeInt(-1);
    } else {
      ByteBuffer buf = marshaller.toBytes(object);
      int length = buf.remaining();
      oout.writeInt(length);
      oout.write(getBytes(buf));
    }
  }

  public static <T> T readObjectFromObjectStreamUsingMarshaller(Marshaller<T> marshaller,
      ObjectInputStream oin) throws IOException {
    int length = oin.readInt();
    if (length == -1) {
      return null;
    }
    byte[] buf = new byte[length];
    readUntilFull(oin, buf);
    return marshaller.fromBytes(ByteBuffer.wrap(buf));
  }

  private static void readUntilFull(InputStream in, byte[] buf) throws IOException {
    int offset = 0;
    int length = buf.length;
    while (offset < buf.length) {
      int read = in.read(buf, offset, length);
      if (read < 0) {
        throw new CorruptDataException("Could not fill buffer up to requested size: " + buf.length
            + " was only able to read " + offset + " bytes.");
      }
      offset += read;
      length -= read;
    }
  }
}
