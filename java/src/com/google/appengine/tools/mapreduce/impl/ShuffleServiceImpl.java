// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;


import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileServicePb.GetCapabilitiesRequest;
import com.google.appengine.api.files.FileServicePb.GetCapabilitiesResponse;
import com.google.appengine.api.files.FileServicePb.GetShuffleStatusRequest;
import com.google.appengine.api.files.FileServicePb.GetShuffleStatusResponse;
import com.google.appengine.api.files.FileServicePb.ShuffleEnums;
import com.google.appengine.api.files.FileServicePb.ShuffleInputSpecification;
import com.google.appengine.api.files.FileServicePb.ShuffleOutputSpecification;
import com.google.appengine.api.files.FileServicePb.ShuffleRequest;
import com.google.appengine.repackaged.com.google.protobuf.InvalidProtocolBufferException;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.apphosting.api.ApiProxy;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.logging.Logger;

/**
 *
 * @author ohler@google.com (Christian Ohler)
 */
class ShuffleServiceImpl implements ShuffleService {
  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(ShuffleServiceImpl.class.getName());
  
  @Override
  public void shuffle(String shuffleId,
      List<AppEngineFile> inputFiles, List<AppEngineFile> outputFiles,
      ShuffleCallback callback) {
    ShuffleRequest.Builder request = ShuffleRequest.newBuilder()
        .setShuffleName(shuffleId);
    for (AppEngineFile inputFile : inputFiles) {
      // NOTE: This does nothing for blobstore files.  It does NOT check for "writable:".
      Preconditions.checkArgument(inputFile.isReadable(), "Not readable: %s", inputFile);
      request.addInput(
          ShuffleInputSpecification.newBuilder()
              .setPath(inputFile.getFullPath())
              // HACK(ohler): This is supposedly the default but the shuffler
              // crashes if we don't specify it.
              .setFormat(ShuffleEnums.InputFormat.RECORDS_KEY_VALUE_PROTO_INPUT)
              .build());
    }
    ShuffleOutputSpecification.Builder output = ShuffleOutputSpecification.newBuilder();
    // HACK(ohler): This is supposedly the default but let's set it just in case
    // the shuffler crashes if we don't (as it does for the input format).
    output.setFormat(ShuffleEnums.OutputFormat.RECORDS_KEY_MULTI_VALUE_PROTO_OUTPUT);
    for (AppEngineFile outputFile : outputFiles) {
      // NOTE: This does nothing for blobstore files.  It does NOT check for "writable:".
      Preconditions.checkArgument(outputFile.isWritable(), "Not writable: %s", outputFile);
      output.addPath(outputFile.getFullPath());
    }
    request.setOutput(output.build());
    // This currently needs to be 0.
    request.setShuffleSizeBytes(0);
    ShuffleRequest.Callback.Builder callbackProto = ShuffleRequest.Callback.newBuilder()
        .setUrl(callback.getUrl())
        .setMethod(callback.getMethod());
    if (callback.getAppVersionId() != null) {
      callbackProto.setAppVersionId(callback.getAppVersionId());
    } else {
      // Leave unset, or set to ApiProxy.getCurrentEnvironment().getVersionId()?
      callbackProto.setAppVersionId(ApiProxy.getCurrentEnvironment().getVersionId());
    }
    log.info("versionId: " + ApiProxy.getCurrentEnvironment().getVersionId());
    if (callback.getQueue() != null) {
      callbackProto.setQueue(callback.getQueue());
    }
    request.setCallback(callbackProto.build());

    log.info("Starting shuffle job " + shuffleId + " with callback " + callback
        + ": " + request.build());
    if (!isAvailable()) {
      throw new RuntimeException("not available");
    }
    ApiProxy.ApiConfig config = new ApiProxy.ApiConfig();
    config.setDeadlineInSeconds(30.0);
    // TODO(ohler): handle exceptions
    byte[] response = ApiProxy.makeSyncCall("file", "Shuffle", request.build().toByteArray(),
        config);
    // response has no data, no point in parsing it
  }

  @Override
  public boolean isAvailable() {
    byte[] responseBytes = ApiProxy.makeSyncCall("file", "GetCapabilities",
        GetCapabilitiesRequest.newBuilder().build().toByteArray());
    GetCapabilitiesResponse response;
    try {
      response = GetCapabilitiesResponse.parseFrom(responseBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to parse GetCapabilitiesResponse: "
          + SerializationUtil.prettyBytes(responseBytes), e);
    }
    return response.getShuffleAvailable();
  }

  // TODO(ohler): Don't use protobuf return type
  @Override
  public GetShuffleStatusResponse getStatus(String shuffleId) {
    byte[] responseBytes = ApiProxy.makeSyncCall("file", "GetShuffleStatus",
        GetShuffleStatusRequest.newBuilder().setShuffleName(shuffleId).build().toByteArray());
    GetShuffleStatusResponse response;
    try {
      response = GetShuffleStatusResponse.parseFrom(responseBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to parse GetShuffleStatusResponse: "
          + SerializationUtil.prettyBytes(responseBytes), e);
    }
    return response;
  }

}
