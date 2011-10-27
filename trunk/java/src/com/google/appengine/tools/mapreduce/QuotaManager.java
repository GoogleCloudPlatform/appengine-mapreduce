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

import com.google.appengine.api.memcache.MemcacheService;

/**
 * A best effort quota service based on memcache.
 *
 *
 */
public class QuotaManager {
  /**
   * The default memcache namespace for quota buckets.
   */
  public static final String DEFAULT_NAMESPACE = "quota";

  // memcache doesn't allow you to decrement below 0, so we add this offset
  // to all memcache values.
  private static final long OFFSET = 1L << 32;

  private final MemcacheService memcacheService;

  /**
   *
   * @param memcacheService the memcache service to use for storing quota
   */
  public QuotaManager(MemcacheService memcacheService) {
    this.memcacheService = memcacheService;
  }

  /**
   *
   * @param memcacheService the memcache service to use for storing quota
   * @param namespace the memcache namespace to use for storing quota
   * @deprecated Use single argument constructor with a namespaced {@link MemcacheService}
   */
  @Deprecated
  public QuotaManager(MemcacheService memcacheService, String namespace) {
    this.memcacheService = memcacheService;

    if (namespace == null) {
      this.memcacheService.setNamespace(DEFAULT_NAMESPACE);
    } else {
      this.memcacheService.setNamespace(namespace);
    }
  }

  /**
   * Add the given amount of quota to the bucket.
   *
   * @param bucket the name of the bucket to add quota to
   * @param amount the amount of quota to add
   */
  public void put(String bucket, long amount) {
    memcacheService.increment(bucket, amount, OFFSET);
  }

  /**
   * Attempts to consume the given amount of quota from the given bucket.
   * Fails there is not enough quota available to satisfy the request.
   *
   * @param bucket the name of the bucket to consume quota from
   * @param amount the amount of quota to consume
   * @return amount if there is enough quota, 0 otherwise.
   */
  public long consume(String bucket, long amount) {
    return consume(bucket, amount, false);
  }

  /**
   * Attempts to consume the given amount of quota from the given bucket.
   * If there is not enough quota available to satisfy the request:
   * <ul><li>If consumeSome is true, then consumes all available quota.
   *     <li>If consumeSome is false, doesn't consume any quota.
   * </ul>
   *
   * @param bucket the name of the bucket to consume quota from
   * @param amount the amount of quota to consume
   * @param consumeSome whether to settle for less quota than amount
   * @return the amount of quota actually consumed
   */
  public long consume(String bucket, long amount, boolean consumeSome) {
    long newQuota = memcacheService.increment(bucket, -amount, OFFSET);

    if (newQuota >= OFFSET) {
      return amount;
    }

    if (consumeSome && OFFSET - newQuota < amount) {
      put(bucket, OFFSET - newQuota);
      return amount - (OFFSET - newQuota);
    } else {
      put(bucket, amount);
      return 0;
    }
  }

  /**
   * Get the amount of quota in the given bucket.
   *
   * @param bucket the name of the bucket to get quota from
   * @return the amount of quota in the bucket
   */
  public long get(String bucket) {
    Object amountObject = memcacheService.get(bucket);
    if (amountObject == null) {
      return 0;
    }
    return ((Long) amountObject) - OFFSET;
  }

  /**
   * Sets the amount of quota available to the given amount.
   *
   * @param bucket the name of the bucket to set quota for
   * @param amount the amount of quota to set available
   */
  public void set(String bucket, long amount) {
    memcacheService.put(bucket, amount + OFFSET);
  }
}
