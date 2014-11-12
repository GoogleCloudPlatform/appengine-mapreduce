// Copyright 2014 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.appengine.tools.mapreduce.servlets;

import java.io.Serializable;
import java.util.Arrays;

class ShufflerParams implements Serializable {

  private static final long serialVersionUID = 1381831224713193323L;

  private String shufflerQueue;
  private String gcsBucket;
  private String[] inputFileNames;
  private String outputDir;

  private int outputShards;
  private String callbackQueue;
  private String callbackModule;
  private String callbackVersion;
  private String callbackPath;

  ShufflerParams() {} // Needed by json

  /**
   * @return the shufflerQueue
   */
  public String getShufflerQueue() {
    return shufflerQueue;
  }

  /**
   * @param shufflerQueue the shufflerQueue to set
   */
  public void setShufflerQueue(String shufflerQueue) {
    this.shufflerQueue = shufflerQueue;
  }

  /**
   * @return the gcsBucket
   */
  public String getGcsBucket() {
    return gcsBucket;
  }

  /**
   * @param gcsBucket the gcsBucket to set
   */
  public void setGcsBucket(String gcsBucket) {
    this.gcsBucket = gcsBucket;
  }

  /**
   * @return the inputFileNames
   */
  public String[] getInputFileNames() {
    return inputFileNames;
  }

  /**
   * @param inputFileNames the inputFileNames to set
   */
  public void setInputFileNames(String[] inputFileNames) {
    this.inputFileNames = inputFileNames;
  }

  /**
   * @return the outputDir
   */
  public String getOutputDir() {
    if (outputDir == null) {
      return "";
    }
    if (outputDir.endsWith("/")) {
      return outputDir.substring(0, outputDir.length() - 1);
    }
    return outputDir;
  }

  /**
   * @param outputDir the outputDir to set
   */
  public void setOutputDir(String outputDir) {
    this.outputDir = outputDir;
  }

  /**
   * @return the outputShards
   */
  public int getOutputShards() {
    return outputShards;
  }

  /**
   * @param outputShards the outputShards to set
   */
  public void setOutputShards(int outputShards) {
    this.outputShards = outputShards;
  }

  /**
   * @return the callbackQueue
   */
  public String getCallbackQueue() {
    return callbackQueue;
  }

  /**
   * @param callbackQueue the callbackQueue to set
   */
  public void setCallbackQueue(String callbackQueue) {
    this.callbackQueue = callbackQueue;
  }

  /**
   * @return the callbackModule
   */
  public String getCallbackModule() {
    return callbackModule;
  }

  /**
   * @param callbackModule the callbackModule to set
   */
  public void setCallbackModule(String callbackModule) {
    this.callbackModule = callbackModule;
  }

  /**
   * @return the callbackVersion
   */
  public String getCallbackVersion() {
    return callbackVersion;
  }

  /**
   * @param callbackVersion the callbackVersion to set
   */
  public void setCallbackVersion(String callbackVersion) {
    this.callbackVersion = callbackVersion;
  }

  /**
   * @return the callbackPath
   */
  public String getCallbackPath() {
    if (outputDir == null) {
      return "";
    }
    return callbackPath;
  }

  /**
   * @param callbackPath the callbackPath to set
   */
  public void setCallbackPath(String callbackPath) {
    this.callbackPath = callbackPath;
  }

  @Override
  public String toString() {
    return "ShufflerParams [shufflerQueue=" + shufflerQueue + ", gcsBucket=" + gcsBucket
        + ", outputDir=" + outputDir + ", inputFileNames=" + Arrays.toString(inputFileNames)
        + ", outputShards=" + outputShards + ", callbackQueue=" + callbackQueue
        + ", callbackModule=" + callbackModule + ", callbackVersion=" + callbackVersion
        + ", callbackPath=" + callbackPath + "]";
  }
}