package com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline;

import static java.util.concurrent.Executors.callable;

import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.cloudstorage.RetryHelper;
import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTaskState;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardRetryState;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunner;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * A pipeline job to delete persistent data for a range of shards of a sharded job.
 */
public class DeleteShardsInfos extends Job0<Void> {

  private static final long serialVersionUID = -4342214189527672009L;

  private final String jobId;
  private final int start;
  private final int end;

  DeleteShardsInfos(String jobId, int start, int end) {
    this.jobId = jobId;
    this.start = start;
    this.end = end;
  }

  private static void addParentKeyToList(List<Key> list, Key parent) {
    for (Key child : SerializationUtil.getShardedValueKeysFor(null, parent, null)) {
      list.add(child);
    }
    list.add(parent);
  }

  @Override
  public Value<Void> run() {
    final List<Key> toDelete = new ArrayList<>((end - start) * 2);
    for (int i = start; i < end; i++) {
      String taskId = ShardedJobRunner.getTaskId(jobId, i);
      addParentKeyToList(toDelete, IncrementalTaskState.Serializer.makeKey(taskId));
      addParentKeyToList(toDelete, ShardRetryState.Serializer.makeKey(taskId));
    }
    RetryHelper.runWithRetries(callable(new Runnable() {
      @Override
      public void run() {
        DatastoreServiceFactory.getDatastoreService().delete(null, toDelete);
      }
    }), ShardedJobRunner.DATASTORE_RETRY_FOREVER_PARAMS, ShardedJobRunner.EXCEPTION_HANDLER);
    return null;
  }

  @Override
  public String getJobDisplayName() {
    return "DeleteShardsInfos: " + jobId + "[" + start + "-" + end + "]";
  }
}
