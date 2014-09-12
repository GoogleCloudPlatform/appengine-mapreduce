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
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.tools.mapreduce.CorruptDataException;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

/**
 * A serialization utility class.
 *
 */
public class SerializationUtil {

  private static final Logger log = Logger.getLogger(SerializationUtil.class.getName());
  private static final DatastoreService DATASTORE = DatastoreServiceFactory.getDatastoreService();
  // 1MB - 200K slack for the rest of the properties and entity overhead
  private static final int MAX_BLOB_BYTE_SIZE = 1024 * 1024 - 200 * 1024;
  private static final String SHARDED_VALUE_KIND = "MR-ShardedValue";
  private static final Function<Entity, Key> ENTITY_TO_KEY = new Function<Entity, Key>() {
    @Override
    public Key apply(Entity entity) {
      return entity.getKey();
    }
  };

  /**
   * Type of compression to optionally use when serializing/deserializing objects.
   */
  public enum CompressionType {

    NONE(1) {
      @Override
      ObjectInputStream wrap(ObjectInputStream sink) {
        return sink;
      }

      @Override
      ObjectOutputStream wrap(ObjectOutputStream dest) {
        return dest;
      }
    },
    GZIP(2) {
      @Override
      ObjectInputStream wrap(ObjectInputStream sink) throws IOException {
        final Inflater inflater =  new Inflater(true);
        InputStream in = new InflaterInputStream(sink, inflater) {
          @Override public void close() throws IOException {
            try {
              super.close();
            } finally {
              inflater.end();
            }
          }
        };
        return new ConciseObjectInputStream(in, true);
      }

      @Override
      ObjectOutputStream wrap(ObjectOutputStream dest) throws IOException {
        final Deflater deflater =  new Deflater(Deflater.BEST_COMPRESSION, true);
        OutputStream out = new DeflaterOutputStream(dest, deflater) {
          @Override public void close() throws IOException {
            try {
              super.close();
            } finally {
              deflater.end();
            }
          }
        };
        return new ConciseObjectOutputStream(out, true);
      }
    };

    private static final Map<Byte, CompressionType> FLAG_TO_COMPRESSION_TYPE = new HashMap<>();
    private final Flag flag;

    static {
      for (CompressionType compressionType : values()) {
        FLAG_TO_COMPRESSION_TYPE.put(compressionType.flag.id, compressionType);
      }
    }

    private CompressionType(int id) {
      flag = new Flag((byte) id);
    }

    abstract ObjectInputStream wrap(ObjectInputStream sink) throws IOException;

    abstract ObjectOutputStream wrap(ObjectOutputStream dest) throws IOException;

    private static CompressionType getByFlag(Flag flag) {
      return FLAG_TO_COMPRESSION_TYPE.get(flag.id);
    }

    private Flag getFlag() {
      return flag;
    }
  }

  private static class Flag implements Externalizable {

    private static final long serialVersionUID = 1L;
    private byte id;

    @SuppressWarnings("unused")
    public Flag() {
      // Needed for serialization
    }

    private Flag(byte id) {
      this.id = id;
    }

    private CompressionType getCompressionType() {
      return CompressionType.getByFlag(this);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeByte(id);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
      id = in.readByte();
    }
  }

  private static class ConciseObjectInputStream extends ObjectInputStream {

    private final boolean ignoreHeader;

    public ConciseObjectInputStream(InputStream in, boolean ignoreHeader) throws IOException {
      super(in);
      this.ignoreHeader = ignoreHeader;
    }

    @Override
    protected void readStreamHeader() throws StreamCorruptedException, IOException {
      if (!ignoreHeader) {
        super.readStreamHeader();
      }
    }

    @Override
    protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
      ObjectStreamClass streamClass = super.readClassDescriptor();
      // Flag.class descriptor was replaced with Object.class descriptor in order to make
      // the descriptor smaller. We need to replace it back.
      if (Object.class.getName().equals(streamClass.getName())) {
        return ObjectStreamClass.lookup(Flag.class);
      } else {
        return streamClass;
      }
    }
  }

  private static class ConciseObjectOutputStream extends ObjectOutputStream {

    private final boolean ignoreHeader;

    public ConciseObjectOutputStream(OutputStream in, boolean ignoreHeader) throws IOException {
      super(in);
      this.ignoreHeader = ignoreHeader;
    }

    @Override
    protected void writeStreamHeader() throws IOException {
      if (!ignoreHeader) {
        super.writeStreamHeader();
      }
    }

    @Override
    protected void writeClassDescriptor(ObjectStreamClass desc) throws IOException {
      // Replace Flag.class descriptor with Object.class descriptor as it is smaller and could
      // not be provided otherwise.
      if (Flag.class.getName().equals(desc.getName())) {
        ObjectStreamClass streamClass = ObjectStreamClass.lookupAny(Object.class);
        super.writeClassDescriptor(streamClass);
      } else {
        super.writeClassDescriptor(desc);
      }
    }
  }

  private static class ByteBufferInputStream extends InputStream {

      private final ByteBuffer byteBuffer;

      public ByteBufferInputStream(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
      }

      @Override
      public int read() {
        if (!byteBuffer.hasRemaining()) {
          return -1;
        }
        return byteBuffer.get() & 0xFF;
      }

      @Override
      public int read(byte[] bytes, int offset, int length) {
        if (!byteBuffer.hasRemaining()) {
          return -1;
        }

        int toRead = Math.min(length, byteBuffer.remaining());
        byteBuffer.get(bytes, offset, toRead);
        return toRead;
      }
  }

  private SerializationUtil() {
    // Utility class
  }

  public static Serializable deserializeFromByteArray(byte[] bytes) {
    return deserializeFromByteArray(bytes, false);
  }

  public static <T> T deserializeFromByteBuffer(ByteBuffer bytes, final boolean ignoreHeader) {
    return deserializeFromStream(new ByteBufferInputStream(bytes), ignoreHeader);
  }

  public static <T> T deserializeFromByteArray(byte[] bytes, boolean ignoreHeader) {
    return deserializeFromStream(new ByteArrayInputStream(bytes), ignoreHeader);
  }

  @SuppressWarnings({"unchecked", "resource"})
  private static <T> T deserializeFromStream(InputStream in, final boolean ignoreHeader) {
    ObjectInputStream oin = null;
    CorruptDataException e = null;
    try {
      oin = new ConciseObjectInputStream(in, ignoreHeader);
      Object value = oin.readObject();
      if (value instanceof Flag) {
        CompressionType compression = ((Flag) value).getCompressionType();
        oin = compression.wrap(oin);
        value = oin.readObject();
      }
      return (T) value;
    } catch (IOException | ClassNotFoundException e1) {
      e = new CorruptDataException("Deserialization error: " + e1.getMessage(), e1);
      throw e;
    } finally {
      if (oin != null) {
        try {
          oin.close();
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

  public static <T extends Serializable> T deserializeFromDatastoreProperty(
      Entity entity, String property) {
    return deserializeFromDatastoreProperty(entity, property, false);
  }

  @SuppressWarnings("unchecked")
  public static <T extends Serializable> T deserializeFromDatastoreProperty(
      Entity entity, String property, boolean lenient) {
    Object value = entity.getProperty(property);
    try {
      byte[] bytes;
      if (value instanceof Blob) {
        bytes = ((Blob) value).getBytes();
      } else {
        Collection<Key> keys = (Collection<Key>) value;
        Map<Key, Entity> shards = DATASTORE.get(keys);
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        for (Key key : keys) {
          Entity shard = shards.get(key);
          if (shard == null) {
            throw new CorruptDataException("Missing data shard " + key);
          }
          byte[] shardBytes = ((Blob) shard.getProperty("content")).getBytes();
          bout.write(shardBytes, 0, shardBytes.length);
        }
        bytes = bout.toByteArray();
      }
      return (T) deserializeFromByteArray(bytes);
    } catch (RuntimeException ex) {
      if (lenient) {
        log.info("Deserialization of " + entity.getKey() + "#" + property + " failed: "
            + ex.getMessage() + ", returning null instead.");
        return null;
      }
      throw ex;
    }
  }

  public static void serializeToDatastoreProperty(
      Transaction tx, Entity entity, String property, Serializable o) {
    serializeToDatastoreProperty(tx, entity, property, o, null);
  }

  public static Iterable<Key> getShardedValueKeysFor(Transaction tx, Key parent, String property) {
    Query query = new Query(SHARDED_VALUE_KIND);
    query.setAncestor(parent);
    if (property != null) {
      query.setFilter(new FilterPredicate("property", FilterOperator.EQUAL, property));
    }
    query.setKeysOnly();
    PreparedQuery preparedQuery = DATASTORE.prepare(tx, query);
    return Iterables.transform(preparedQuery.asIterable(), ENTITY_TO_KEY);
  }

  public static void serializeToDatastoreProperty(
      Transaction tx, Entity entity, String property, Serializable o, CompressionType compression) {
    byte[] bytes = serializeToByteArray(o, false, compression);

    // deleting previous shards
    List<Key> toDelete = Lists.newArrayList(getShardedValueKeysFor(tx, entity.getKey(), property));

    Object value;
    if (bytes.length < MAX_BLOB_BYTE_SIZE) {
      value = new Blob(bytes);
      DATASTORE.delete(tx, toDelete);
    } else {
      int shardId = 0;
      int offset = 0;
      List<Entity> shards = new ArrayList<>(bytes.length / MAX_BLOB_BYTE_SIZE + 1);
      while (offset < bytes.length) {
        int limit = offset + MAX_BLOB_BYTE_SIZE;
        byte[] chunk = Arrays.copyOfRange(bytes, offset, Math.min(limit, bytes.length));
        offset = limit;
        String keyName = String.format("shard-%02d", ++shardId);
        Entity shard = new Entity(SHARDED_VALUE_KIND, keyName, entity.getKey());
        shard.setProperty("property", property);
        shard.setUnindexedProperty("content", new Blob(chunk));
        shards.add(shard);
      }
      if (shards.size() < toDelete.size()) {
        DATASTORE.delete(tx, toDelete.subList(shards.size(), toDelete.size()));
      }
      value = DATASTORE.put(tx, shards);
    }
    entity.setUnindexedProperty(property, value);
  }

  public static byte[] serializeToByteArray(Serializable o) {
    return serializeToByteArray(o, false, null);
  }

  public static byte[] serializeToByteArray(Serializable o, boolean ignoreHeader) {
    return serializeToByteArray(o, ignoreHeader, null);
  }

  @SuppressWarnings("resource")
  public static byte[] serializeToByteArray(
      Serializable o, final boolean ignoreHeader, /*Nullable*/ CompressionType compression) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ObjectOutputStream out = null;
    try {
      out = new ConciseObjectOutputStream(bytes, ignoreHeader);
      if (compression == null) {
        out.writeObject(o);
      } else {
        out.writeObject(compression.getFlag());
        out = compression.wrap(out);
        out.writeObject(o);
      }
      out.flush();
      out.close();
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Can't serialize object: " + o, e);
    } finally {
      try {
        // We want to make sure deflater end method is called
        if (out != null) {
          out.close();
        }
      } catch (IOException ignore) {
        // ignore
      }
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

  public static <T extends Serializable> T clone(T toClone) {
    byte[] bytes = SerializationUtil.serializeToByteArray(toClone);
    return (T) SerializationUtil.deserializeFromByteArray(bytes);
  }
}
