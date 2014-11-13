package com.google.appengine.tools.mapreduce.impl;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.appengine.tools.mapreduce.BigQueryMarshaller;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Validates and generates {@link TableSchema} of a bigquery table and marshals objects into json
 * for loading into that table using annotations and reflection on type T.
 *
 * @param <T> type of the object to be marshalled.
 */
public final class BigQueryMarshallerByType<T> extends BigQueryMarshaller<T> {

  private static final long serialVersionUID = 4204111891641696390L;
  private final static ObjectMapper objectMapper = new ObjectMapper();
  private static final String NEWLINE_CHARACTER = "\n";
  private final Class<T> type;
  private final BigQuerySchemaMarshallerByType<T> schemaMarshaller;
  private final BigQueryDataMarshallerByType dataMarshaller;

  /**
   * @param type of the object to be marshalled by this marshaller.
   * @param marshallers map of field to {@link BigqueryFieldMarshaller} for fields that are
   *        not/cannot be resolved to one of the following types : String, StringBuilder,
   *        StringBuffer, Character, char, Integer, int, BigInteger, Short, short, Long, Float,
   *        float, Double, double, BigDecimal, Boolean, boolean, Currency, Date, URI, Path.
   */
  public BigQueryMarshallerByType(Class<T> type, Map<Field, BigqueryFieldMarshaller> marshallers) {
    this.type = type;
    this.schemaMarshaller = new BigQuerySchemaMarshallerByType<>(type, marshallers);
    this.dataMarshaller = new BigQueryDataMarshallerByType(marshallers);
  }

  public BigQueryMarshallerByType(Class<T> type) {
    this(type, null);
  }

  @Override
  public TableSchema getSchema() {
    return schemaMarshaller.getSchema();
  }

  /**
   * Validates that the type of the object to serialize is the same as the type for which schema was
   * generated. The type should match exactly as interface and abstract classes are not supported.
   */
  @Override
  public ByteBuffer toBytes(T object) {
    if (!type.equals(object.getClass())) {
      throw new RuntimeException(
          "The type of the object does not match the parameter type of this marshaller. ");
    }
    Map<String, Object> jsonObject = dataMarshaller.mapFieldNameToValue(object);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      objectMapper.writeValue(out, jsonObject);
      out.write(NEWLINE_CHARACTER.getBytes());
    } catch (IOException e) {
      throw new RuntimeException("Error in serializing to bigquery json " + jsonObject, e);
    }
    return ByteBuffer.wrap(out.toByteArray());
  }

  @Override
  public T fromBytes(ByteBuffer b) {
    throw new UnsupportedOperationException("Not implemented yet.");
  }
}
