package com.google.appengine.tools.mapreduce.outputs;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.BigQueryFieldMode;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.impl.BigQueryMarshallerByType;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.appengine.tools.mapreduce.testmodels.Child;
import com.google.appengine.tools.mapreduce.testmodels.Father;
import com.google.common.collect.Lists;

import junit.framework.TestCase;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigQueryGoogleCloudStorageStoreOutputTest extends TestCase {
  private static final String BUCKET = "test-bigquery-loader";

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper();

  @Override
  protected void setUp() throws Exception {
    helper.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    helper.tearDown();
  }

  @Test
  public void testBigQueryResult() throws IOException {
    BigQueryGoogleCloudStorageStoreOutput<Father> creator =
        new BigQueryGoogleCloudStorageStoreOutput<Father>(
            new BigQueryMarshallerByType<Father>(Father.class), BUCKET, "testJob");

    List<MarshallingOutputWriter<Father>> writers = creator.createWriters(5);
    List<MarshallingOutputWriter<Father>> finished = new ArrayList<>();
    for (MarshallingOutputWriter<Father> writer : writers) {
      writer.beginShard();
      writer.beginSlice();
      writer = SerializationUtil.clone(writer);
      writer.write(new Father(true, "Father",
          Lists.newArrayList(new Child("Childone", 1), new Child("childtwo", 2))));
      writer.endSlice();
      writer.beginSlice();
      writer.write(new Father(true, "Father",
          Lists.newArrayList(new Child("Childone", 1), new Child("childtwo", 2))));
      writer.endSlice();
      writer = SerializationUtil.clone(writer);
      writer.beginSlice();
      writer.write(new Father(true, "Father",
          Lists.newArrayList(new Child("Childone", 1), new Child("childtwo", 2))));
      writer.endSlice();
      writer.endShard();
      finished.add(writer);
    }
    BigQueryStoreResult<GoogleCloudStorageFileSet> result = creator.finish(finished);
    assertEquals(5, result.getResult().getNumFiles());

    TableFieldSchema f1 = new TableFieldSchema().setType("boolean").setName("married")
        .setMode(BigQueryFieldMode.REQUIRED.getValue());
    TableFieldSchema f2 = new TableFieldSchema().setType("string").setName("name");
    TableFieldSchema f3 = new TableFieldSchema().setName("sons").setType("record")
        .setMode(BigQueryFieldMode.REPEATED.getValue());
    f3.setFields(Lists.newArrayList(new TableFieldSchema().setType("integer").setName("age")
        .setMode(BigQueryFieldMode.REQUIRED.getValue()),
        new TableFieldSchema().setName("fullName").setType("string")));

    TableSchema actual = result.getSchema();
    TableSchema expected = new TableSchema().setFields(Lists.newArrayList(f1, f2, f3));
    compareFields(expected.getFields(), actual.getFields());
  }

  private void compareFields(List<TableFieldSchema> expected, List<TableFieldSchema> actual) {
    if (expected == null) {
      assertNull(actual);
      return;
    }
    Map<String, TableFieldSchema> expectedMap = new HashMap<>();
    for (TableFieldSchema expectedField : expected) {
      expectedMap.put(expectedField.getName(), expectedField);
    }
    for (TableFieldSchema actualField : actual) {
      TableFieldSchema expectedField = expectedMap.remove(actualField.getName());
      assertNotNull("Missing expected field for " + actualField, expectedField);
      assertEquals(expectedField.getDescription(), actualField.getDescription());
      assertEquals(expectedField.getMode(), actualField.getMode());
      assertEquals(expectedField.getName(), actualField.getName());
      assertEquals(expectedField.getType(), actualField.getType());
      compareFields(expectedField.getFields(), actualField.getFields());
    }
    assertTrue("Missing actual values for " + expectedMap, expectedMap.isEmpty());
  }
}
