package com.google.appengine.tools.mapreduce.outputs;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.appengine.tools.development.testing.LocalFileServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.BigQueryFieldMode;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.impl.BigQueryMarshallerByType;
import com.google.appengine.tools.mapreduce.testModels.Child;
import com.google.appengine.tools.mapreduce.testModels.Father;
import com.google.common.collect.Lists;

import junit.framework.TestCase;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

public class BigQueryGoogleCloudStorageStoreOutputTest extends TestCase {
  private static final String BUCKET = "test-bigquery-loader";

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalFileServiceTestConfig());

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    helper.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    helper.tearDown();
  }

  @Test
  public void testBigQueryResult() throws IOException, ClassNotFoundException {
    BigQueryGoogleCloudStorageStoreOutput<Father> creator =
        new BigQueryGoogleCloudStorageStoreOutput<Father>(
            new BigQueryMarshallerByType<Father>(Father.class), BUCKET, "testJob");

    List<MarshallingOutputWriter<Father>> writers = creator.createWriters(5);
    List<MarshallingOutputWriter<Father>> finished = new ArrayList<>();
    for (MarshallingOutputWriter<Father> writer : writers) {
      writer.beginShard();
      writer.beginSlice();
      writer = reconstruct(writer);
      writer.write(new Father(true, "Father",
          Lists.newArrayList(new Child("Childone", 1), new Child("childtwo", 2))));
      writer.endSlice();
      writer.beginSlice();
      writer.write(new Father(true, "Father",
          Lists.newArrayList(new Child("Childone", 1), new Child("childtwo", 2))));
      writer.endSlice();
      writer = reconstruct(writer);
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
    assertTrue(actual.equals(expected));
  }

  @SuppressWarnings("unchecked")
  private static <T> T reconstruct(T obj) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
      oout.writeObject(obj);
    }
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bout.toByteArray()))) {
      return (T) in.readObject();
    }
  }
}
