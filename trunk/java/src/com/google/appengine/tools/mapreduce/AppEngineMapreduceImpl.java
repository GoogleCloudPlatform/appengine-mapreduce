// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.tools.mapreduce.v2.impl.MapReduceState;
import com.google.appengine.tools.mapreduce.v2.impl.handlers.Controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;


/**
 * Real implementation of AppEngineMapreduce. Functionality for controlling and observing mapreduce
 * jobs.
 *
 */
class AppEngineMapreduceImpl implements AppEngineMapreduce {

  @Override
  public String start(Configuration configuration, String name, String basePath) {
    if (!basePath.endsWith("/")) {
      basePath += "/";
    }
    return Controller.handleStart(configuration, name, basePath);
  }

  @Override
  public MapReduceState getState(String jobId) {
    DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();
    try {
      return MapReduceState.getMapReduceStateFromJobID(datastoreService, JobID.forName(jobId));
    } catch (EntityNotFoundException ignored) {
      return null;
    }
  }
}
