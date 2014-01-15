// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Sharder;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.UnsignedBytes;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * A shuffle utility used by {@link InProcessMapReduce}.
 *
 * @author ohler@google.com (Christian Ohler)
 */
class Shuffling {

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
    Sharder sharder = new HashingSharder(shardCount);
    for (KeyValue<Bytes, V> pair : in) {
      out.get(sharder.getShardForKey(ByteBuffer.wrap(pair.getKey().bytes)))
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
      K key = keyMarshaller.fromBytes(ByteBuffer.wrap(keyBytes.bytes));
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
