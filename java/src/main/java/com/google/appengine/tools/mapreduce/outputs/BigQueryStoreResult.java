package com.google.appengine.tools.mapreduce.outputs;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.impl.util.SerializableValue;

import java.io.Serializable;

/**
 * Result of bigQuery staging process. For e.g. currently bigquery can only load data from files
 * stored in Google cloud storage(GCS). So Google Cloud Storage(GCS) is the staging area. R for GCS
 * is {@link GoogleCloudStorageFileSet}.
 *
 * @param <R> type of result produced by the staging process {@link Output}.
 */
public final class BigQueryStoreResult<R> implements Serializable {

  private static final long serialVersionUID = 3843348927621484947L;
  private final R result;
  private final SerializableValue<TableSchema> serializableSchema;

  /**
   * @param result of writing data to the staging area.
   * @param schema a wrapper around {@link TableSchema} to make it serializable.
   */
  public BigQueryStoreResult(R result, TableSchema schema) {
    this.result = result;
    this.serializableSchema =
        SerializableValue.of(Marshallers.getGenericJsonMarshaller(TableSchema.class), schema);
  }

  public R getResult() {
    return result;
  }

  public TableSchema getSchema() {
    return serializableSchema.getValue();
  }
}
