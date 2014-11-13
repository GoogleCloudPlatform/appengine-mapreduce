package com.google.appengine.tools.mapreduce;

import com.google.api.services.bigquery.model.TableSchema;


/**
 * An implementation of this class should serialize the objects of type T into newline separated
 * json as expected by the bigquery load jobs. It should also provide an implementation for
 * generating the schema({@link TableSchema}) of the bigquery table.
 *
 * @param <T> type of the object to be marshalled
 */
public abstract class BigQueryMarshaller<T> extends Marshaller<T> {
  private static final long serialVersionUID = 5170161329883029808L;

  public abstract TableSchema getSchema();
}
