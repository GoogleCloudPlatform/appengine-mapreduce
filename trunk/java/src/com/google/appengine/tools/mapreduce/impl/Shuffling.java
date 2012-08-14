// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import com.google.common.base.Function;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author ohler@google.com (Christian Ohler)
 */
public class Shuffling {

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(Shuffling.class.getName());

  private Shuffling() {}

  private static final Function<KeyValue<byte[], ?>, byte[]> KEY_FUNCTION =
      new Function<KeyValue<byte[], ?>, byte[]>() {
    @Override public byte[] apply(KeyValue<byte[], ?> pair) {
      return pair.getKey();
    }
  };

  public static final Ordering<byte[]> KEY_ORDERING =
      Ordering.from(UnsignedBytes.lexicographicalComparator());

  public static final Ordering<KeyValue<byte[], ?>> KEY_VALUE_ORDERING_BY_KEY =
      KEY_ORDERING.onResultOf(KEY_FUNCTION);

  public static int reduceShardFor(byte[] key, int reduceShardCount) {
    // TODO(ohler): Use the same hash function as shuffle service would.
    int targetShard = Arrays.hashCode(key) % reduceShardCount;
    if (targetShard < 0) {
      targetShard += reduceShardCount;
    }
    return targetShard;
  }

  public static int reduceShardFor(ByteBuffer key, int reduceShardCount) {
    return reduceShardFor(SerializationUtil.getBytes(key), reduceShardCount);
  }

  // Wrapper around byte[] that implements hashCode and equals the way we
  // need, for use as MultiMap keys etc.
  private static class Bytes implements Comparable<Bytes> {
    private final byte[] bytes;
    private final int hashCode;

    Bytes(byte[] bytes) {
      this.bytes = checkNotNull(bytes, "Null bytes");
      hashCode = Arrays.hashCode(bytes);
    }

    @Override public int hashCode() {
      return hashCode;
    }

    @Override public boolean equals(Object o) {
      if (o == this) { return true; }
      if (!(o instanceof Bytes)) { return false; }
      Bytes other = (Bytes) o;
      return hashCode == other.hashCode
          && Arrays.equals(bytes, other.bytes);
    }

    @Override public int compareTo(Bytes other) {
      return Shuffling.KEY_ORDERING.compare(bytes, other.bytes);
    }
  }

  private static <K, V> List<KeyValue<Bytes, V>> keysToBytes(
      Marshaller<K> keyMarshaller, Iterable<KeyValue<K, V>> in) {
    ImmutableList.Builder<KeyValue<Bytes, V>> out = ImmutableList.builder();
    for (KeyValue<K, V> pair : in) {
      Bytes key = new Bytes(SerializationUtil.getBytes(keyMarshaller.toBytes(pair.getKey())));
      out.add(KeyValue.of(key, pair.getValue()));
    }
    return out.build();
  }

  private static <V> List<ListMultimap<Bytes, V>> groupByShardAndKey(List<KeyValue<Bytes, V>> in,
      int shardCount) {
    List<ListMultimap<Bytes, V>> out = Lists.newArrayListWithCapacity(shardCount);
    for (int i = 0; i < shardCount; i++) {
      out.add(ArrayListMultimap.<Bytes, V>create());
    }
    for (KeyValue<Bytes, V> pair : in) {
      out.get(reduceShardFor(pair.getKey().bytes, shardCount))
          .put(pair.getKey(), pair.getValue());
    }
    return out;
  }

  // Also turns the keys back from Bytes into K.
  private static <K, V> List<KeyValue<K, List<V>>> multimapToList(
      Marshaller<K> keyMarshaller, ListMultimap<Bytes, V> in) {
    List<Bytes> keys = Ordering.natural().sortedCopy(in.keySet());
    ImmutableList.Builder<KeyValue<K, List<V>>> out = ImmutableList.builder();
    for (Bytes keyBytes : keys) {
      K key;
      try {
        key = keyMarshaller.fromBytes(ByteBuffer.wrap(keyBytes.bytes));
      } catch (IOException e) {
        throw new RuntimeException(keyMarshaller + ".fromBytes() threw IOException on "
            + SerializationUtil.prettyBytes(keyBytes.bytes),
            e);
      }
      out.add(KeyValue.of(key, in.get(keyBytes)));
    }
    return out.build();
  }

  public static <K, V> List<List<KeyValue<K, List<V>>>> shuffle(
      List<List<KeyValue<K, V>>> mapperOutputs,
      Marshaller<K> keyMarshaller, int reduceShardCount) {
    List<ListMultimap<Bytes, V>> buckets =
        groupByShardAndKey(keysToBytes(keyMarshaller, Iterables.concat(mapperOutputs)),
            reduceShardCount);
    ImmutableList.Builder<List<KeyValue<K, List<V>>>> out = ImmutableList.builder();
    for (int i = 0; i < reduceShardCount; i++) {
      out.add(multimapToList(keyMarshaller, buckets.get(i)));
    }
    return out.build();
  }

}
