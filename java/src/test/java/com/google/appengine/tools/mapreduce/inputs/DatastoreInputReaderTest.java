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
package com.google.appengine.tools.mapreduce.inputs;

import static com.google.appengine.api.datastore.Entity.KEY_RESERVED_PROPERTY;
import static com.google.appengine.api.datastore.Query.CompositeFilterOperator.AND;
import static com.google.appengine.api.datastore.Query.FilterOperator.GREATER_THAN_OR_EQUAL;
import static com.google.appengine.api.datastore.Query.FilterOperator.LESS_THAN;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.CompositeFilter;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 */
public class DatastoreInputReaderTest extends TestCase {

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());

  private static final String ENTITY_KIND_NAME = "Bob";
  private Query all;
  private Query namespaceQuery;

  private DatastoreService ds;

  private static Query createQuery(String namespace, String kind, String property,
      Object lowerBound, Object upperBound) {
    ImmutableList<Filter> f = ImmutableList.<Filter>builder()
        .add(new FilterPredicate(property, GREATER_THAN_OR_EQUAL, lowerBound))
        .add(new FilterPredicate(property, LESS_THAN, upperBound)).build();
    return BaseDatastoreInput.createQuery(namespace, kind).setFilter(new CompositeFilter(AND, f));
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    ds = DatastoreServiceFactory.getDatastoreService();
    all = BaseDatastoreInput.createQuery(null, ENTITY_KIND_NAME);
    namespaceQuery =  BaseDatastoreInput.createQuery("ns", ENTITY_KIND_NAME);
  }

  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
    BaseDatastoreInputReader.resetTicker();
  }

  public void testOneEntityOnly() throws Exception {
    Key key = populateData(1, null).get(0);
    DatastoreInputReader reader = new DatastoreInputReader(all);
    reader.beginSlice();
    assertEquals(key, reader.next().getKey());
    try {
      reader.next();
      fail();
    } catch (NoSuchElementException e) {
      // ok
    }
  }

  private List<Key> populateData(int size, String namespace) {
    Collection<Entity> entities = new ArrayList<>(size);
    String ns = NamespaceManager.get();
    try {
      if (namespace != null) {
        NamespaceManager.set(namespace);
      }
      for (int i = 0; i < size; i++) {
        entities.add(new Entity(ENTITY_KIND_NAME));
      }
    } finally {
      NamespaceManager.set(ns);
    }
    List<Key> keys = ds.put(entities);
    Collections.sort(keys);
    return keys;
  }

  public void testReadAllData() throws Exception {
    List<Key> keys = populateData(300, null);
    validateReadAllData(all, keys);
  }

  public void testReadAllDataWithNamespace() throws Exception {
    List<Key> keys = populateData(300, namespaceQuery.getNamespace());
    validateReadAllData(namespaceQuery, keys);
  }

  private void validateReadAllData(Query query, List<Key> keys) {
    DatastoreInputReader reader = new DatastoreInputReader(query);
    List<Key> readKeys = readAllFromReader(reader);
    assertEquals(keys.size(), readKeys.size());
    assertEquals(keys, readKeys);
  }

  private List<Key> readAllFromReader(DatastoreInputReader reader) {
    List<Key> readKeys = new ArrayList<>();
    int entriesPerSlice = 20;
    int count = 0;
    final int entriesPerQueryExpiration = 8;
    boolean finish = false;
    BaseDatastoreInputReader.setTickerForTesting(new Ticker() {
      int tickerCount = 0;
      @Override
      public long read() {
        return ++tickerCount % entriesPerQueryExpiration == 0 ? 0 : System.nanoTime();
      }
    });
    reader.beginSlice();
    while (!finish) {
      try {
        Entity entity = reader.next();
        readKeys.add(entity.getKey());
      } catch (NoSuchElementException e) {
        finish = true;
      }
      if (++count % entriesPerSlice == 0) {
        reader.endSlice();
        reader.beginSlice();
      }
    }
    reader.endSlice();
    BaseDatastoreInputReader.resetTicker();
    return readKeys;
  }

  public void testSerialization() throws Exception {
    List<Key> keys = populateData(300, null);
    List<Key> readKeys = new ArrayList<>();
    Query query =
        createQuery(null, ENTITY_KIND_NAME, KEY_RESERVED_PROPERTY, keys.get(100), keys.get(200));
    DatastoreInputReader reader = new DatastoreInputReader(query);
    reader.beginSlice();
    while (true) {
      try {
        Entity entity = reader.next();
        readKeys.add(entity.getKey());
      } catch (NoSuchElementException e) {
        break;
      }

      reader.endSlice();
      byte[] bytes = SerializationUtil.serializeToByteArray(reader);
      reader = (DatastoreInputReader) SerializationUtil.deserializeFromByteArray(bytes);
      reader.beginSlice();
    }

    assertEquals(100, readKeys.size());
    assertEquals(keys.subList(100, 200), readKeys);
  }

  public void testStartEndKey() throws Exception {
    List<Key> keys = populateData(300, null);
    Query query =
        createQuery(null, ENTITY_KIND_NAME, KEY_RESERVED_PROPERTY, keys.get(100), keys.get(200));
    DatastoreInputReader reader = new DatastoreInputReader(query);
    List<Key> readKeys = readAllFromReader(reader);
    assertEquals(100, readKeys.size());
    assertEquals(keys.subList(100, 200), readKeys);
  }
}
