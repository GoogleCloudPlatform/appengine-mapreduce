package com.google.appengine.tools.mapreduce;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.appengine.tools.mapreduce.impl.BigQueryMarshallerByType;
import com.google.appengine.tools.mapreduce.testmodels.ClassWithArray;
import com.google.appengine.tools.mapreduce.testmodels.Father;
import com.google.appengine.tools.mapreduce.testmodels.Man;
import com.google.appengine.tools.mapreduce.testmodels.ParameterizedClass;
import com.google.appengine.tools.mapreduce.testmodels.Person;
import com.google.appengine.tools.mapreduce.testmodels.SampleClassWithNonParametricList;
import com.google.appengine.tools.mapreduce.testmodels.SimplAnnotatedJson;
import com.google.appengine.tools.mapreduce.testmodels.SimpleJson;
import com.google.appengine.tools.mapreduce.testmodels.SimpleJsonWithWrapperTypes;
import com.google.common.collect.Lists;

import junit.framework.TestCase;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Test cases for schema generation
 */
public class BigQuerySchemaMarshallerTest extends TestCase {

  private class BigQuerySchemaMarshallerTester<T> {
    BigQueryMarshallerByType<T> schemaMarshaller;

    public BigQuerySchemaMarshallerTester(BigQueryMarshallerByType<T> schemaMarshaller) {
      this.schemaMarshaller = schemaMarshaller;
    }

    /**
     * asserts each field of expected schema and generated schema
     *
     * @param expected {@code TableSchema}
     */
    public void testSchema(TableSchema expected) {

      List<TableFieldSchema> nonRecordExpFields = getAllNonRecordFields(expected.getFields());
      List<TableFieldSchema> nonRecordActFields =
          getAllNonRecordFields(schemaMarshaller.getSchema().getFields());

      Comparator<TableFieldSchema> fieldSchemaComprator = new Comparator<TableFieldSchema>() {
        @Override
        public int compare(TableFieldSchema o1, TableFieldSchema o2) {
          return o1.toString().compareTo(o2.toString());
        }

      };
      Collections.sort(nonRecordActFields, fieldSchemaComprator);
      Collections.sort(nonRecordExpFields, fieldSchemaComprator);

      assertEquals(nonRecordExpFields.size(), nonRecordActFields.size());
      assertEquals(nonRecordExpFields, nonRecordActFields);
    }

    /**
     * Recursively retrieves all the simple type fields from the fields of type "record".
     */
    private List<TableFieldSchema> getAllNonRecordFields(List<TableFieldSchema> fields) {
      List<TableFieldSchema> toRet = Lists.newArrayList();
      for (TableFieldSchema tfs : fields) {
        if (tfs.getType().equals("record")) {
          toRet.addAll(getAllNonRecordFields(tfs.getFields()));
        } else {
          toRet.add(tfs);
        }
      }
      return toRet;
    }

  }

  public void testSchemaWithSimpleFields() {
    BigQuerySchemaMarshallerTester<SimpleJson> tester = new BigQuerySchemaMarshallerTester<
        SimpleJson>(new BigQueryMarshallerByType<>(SimpleJson.class));

    tester.testSchema(new TableSchema().setFields(Lists.newArrayList(new TableFieldSchema()
        .setName("id").setType("integer")
        .setMode(BigQueryFieldMode.REQUIRED.getValue()), new TableFieldSchema().setName("name")
        .setType("string").setMode(BigQueryFieldMode.REQUIRED.getValue()))));
  }

  public void testSchemaWithAnnotatedName() {
    BigQuerySchemaMarshallerTester<SimplAnnotatedJson> tester = new BigQuerySchemaMarshallerTester<
        SimplAnnotatedJson>(
        new BigQueryMarshallerByType<SimplAnnotatedJson>(SimplAnnotatedJson.class));

    tester.testSchema(new TableSchema().setFields(Lists.newArrayList(
        new TableFieldSchema().setName("id").setType("string"), new TableFieldSchema()
            .setName("niceName").setType("string").setMode("nullable"), new TableFieldSchema()
            .setName("intField").setType("integer")
            .setMode(BigQueryFieldMode.REQUIRED.getValue()))));

  }

  public void testSchemaWithArrayField() {
    BigQuerySchemaMarshallerTester<ClassWithArray> tester = new BigQuerySchemaMarshallerTester<
        ClassWithArray>(new BigQueryMarshallerByType<>(ClassWithArray.class));

    TableFieldSchema f1 = new TableFieldSchema().setType("integer").setName("id")
        .setMode(BigQueryFieldMode.REQUIRED.getValue());
    TableFieldSchema f2 = new TableFieldSchema().setName("name").setType("string");
    TableFieldSchema f3 = new TableFieldSchema().setName("values").setType("string")
        .setMode(BigQueryFieldMode.REPEATED.getValue());

    TableSchema exp = new TableSchema().setFields(Lists.newArrayList(f1, f2, f3));

    tester.testSchema(exp);
  }

  public void testSchemaWithNestedFields() {
    BigQuerySchemaMarshallerTester<Person> tester = new BigQuerySchemaMarshallerTester<Person>(
        new BigQueryMarshallerByType<Person>(Person.class));

    TableFieldSchema f1 = new TableFieldSchema().setType("integer").setName("age")
        .setMode(BigQueryFieldMode.REQUIRED.getValue());
    TableFieldSchema f2 = new TableFieldSchema().setType("float").setName("height")
        .setMode(BigQueryFieldMode.REQUIRED.getValue());
    TableFieldSchema f3 = new TableFieldSchema().setType("float").setName("weight")
        .setMode(BigQueryFieldMode.REQUIRED.getValue());
    TableFieldSchema f4 = new TableFieldSchema().setType("string").setName("gender");
    TableFieldSchema f5 = new TableFieldSchema().setType("record").setName("gender");
    TableFieldSchema f51 = new TableFieldSchema().setType("integer").setName("number")
        .setMode(BigQueryFieldMode.REQUIRED.getValue());
    TableFieldSchema f52 = new TableFieldSchema().setType("integer").setName("areaCode")
        .setMode(BigQueryFieldMode.REQUIRED.getValue());

    tester.testSchema(new TableSchema().setFields(
        Lists.newArrayList(new TableFieldSchema().setType("string").setName("fullName"),
            f1,
            f2,
            f3,
            f4,
            f5.setFields(Lists.newArrayList(f51, f52)))));
  }

  public void testSchemaWithBigIgnoreAnnotations() {
    BigQuerySchemaMarshallerTester<Man> tester =
        new BigQuerySchemaMarshallerTester<Man>(new BigQueryMarshallerByType<Man>(Man.class));
    TableFieldSchema f1 = new TableFieldSchema().setType("string").setName("name");
    TableFieldSchema f2 = new TableFieldSchema().setType("string").setName("gender");

    tester.testSchema(new TableSchema().setFields(Lists.newArrayList(f1, f2)));
  }

  public void testSchemaWithRepeatedNestedRecord() {
    BigQuerySchemaMarshallerTester<Father> tester = new BigQuerySchemaMarshallerTester<Father>(
        new BigQueryMarshallerByType<Father>(Father.class));

    TableFieldSchema f1 = new TableFieldSchema().setType("boolean").setName("married")
        .setMode(BigQueryFieldMode.REQUIRED.getValue());
    TableFieldSchema f2 = new TableFieldSchema().setType("string").setName("name");
    TableFieldSchema f3 = new TableFieldSchema().setName("sons").setType("record")
        .setMode(BigQueryFieldMode.REPEATED.getValue());
    f3.setFields(Lists.newArrayList(
        new TableFieldSchema().setName("fullName").setType("string"), new TableFieldSchema()
            .setType("integer").setName("age").setMode(BigQueryFieldMode.REQUIRED.getValue())));

    tester.testSchema(new TableSchema().setFields(Lists.newArrayList(f1, f2, f3)));
  }

  public void testSchemaForClassWithWrapperType() {
    BigQuerySchemaMarshallerTester<SimpleJsonWithWrapperTypes> tester =
        new BigQuerySchemaMarshallerTester<SimpleJsonWithWrapperTypes>(
            new BigQueryMarshallerByType<>(SimpleJsonWithWrapperTypes.class));

    tester.testSchema(new TableSchema().setFields(Lists.newArrayList(
        new TableFieldSchema().setName("id").setType("integer"),
        new TableFieldSchema().setName("name").setType("string"),
        new TableFieldSchema().setName("value").setType("float"))));
  }

  @SuppressWarnings("rawtypes")
  public void testSchemaForParameterizedTypes() {
    try {
      BigQuerySchemaMarshallerTester<ParameterizedClass> tester =
          new BigQuerySchemaMarshallerTester<ParameterizedClass>(
              new BigQueryMarshallerByType<>(ParameterizedClass.class));

      tester.testSchema(new TableSchema().setFields(Lists.newArrayList(new TableFieldSchema()
          .setName("id").setType("integer").setMode(BigQueryFieldMode.REQUIRED.getValue()),
          new TableFieldSchema().setName("name").setType("string"))));
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Cannot marshal " + ParameterizedClass.class.getSimpleName()
              + ". Parameterized type other than Collection<T> cannot be marshalled into consistent BigQuery data.",
          e.getMessage());
    }
  }

  public void testSchemaForTypesWithNonParameterizedCollection() {
    try {
      BigQuerySchemaMarshallerTester<SampleClassWithNonParametricList> tester =
          new BigQuerySchemaMarshallerTester<SampleClassWithNonParametricList>(
              new BigQueryMarshallerByType<>(SampleClassWithNonParametricList.class));

      tester.testSchema(new TableSchema().setFields(Lists.newArrayList(new TableFieldSchema()
          .setName("id").setType("integer").setMode(BigQueryFieldMode.REQUIRED.getValue()),
          new TableFieldSchema().setName("name").setType("string"))));
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Cannot marshal a non-parameterized Collection field " + "l" + " into BigQuery data",
          e.getMessage());
    }
  }

  private class ClassForInnerClassTest {
    @SuppressWarnings("unused")
    int id;
    @SuppressWarnings("unused")
    String name;

    @SuppressWarnings("unused")
    public ClassForInnerClassTest(int id, String name) {
      this.id = id;
      this.name = name;
    }
  }

  public void testSchemaForInnerClass() {
    BigQuerySchemaMarshallerTester<ClassForInnerClassTest> tester =
        new BigQuerySchemaMarshallerTester<ClassForInnerClassTest>(
            new BigQueryMarshallerByType<>(ClassForInnerClassTest.class));

    tester.testSchema(new TableSchema().setFields(Lists.newArrayList(new TableFieldSchema()
        .setName("id").setType("integer").setMode(BigQueryFieldMode.REQUIRED.getValue()),
        new TableFieldSchema().setName("name").setType("string"))));
  }
}
