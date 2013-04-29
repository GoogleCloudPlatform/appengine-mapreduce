// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileServicePb.GetShuffleStatusResponse;

import java.util.List;

/**
 *
 */
public interface ShuffleService {

  void shuffle(String shuffleId, List<AppEngineFile> inputFiles,
      List<AppEngineFile> outputFiles, ShuffleCallback callback);

  // TODO(ohler): Don't use protobuf return type
  GetShuffleStatusResponse getStatus(String shuffleId);

}
