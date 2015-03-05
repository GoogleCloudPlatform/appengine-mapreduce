package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.impl.util.SplitUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * A convenience datastructure to track sets of files on a per-shard basis
 */
public final class FilesByShard implements Serializable {

  private static final long serialVersionUID = -4160169134959332304L;
  private final String bucket;
  private List<List<String>> allFiles;
  private int shardCount;

  public FilesByShard(int shardCount, String bucket) {
    Preconditions.checkArgument(shardCount > 0);
    this.shardCount = shardCount;
    this.bucket = Preconditions.checkNotNull(bucket);
    this.allFiles = initAllFiles(shardCount);
  }

  public GoogleCloudStorageFileSet getFilesForShard(int shardNumber) {
    Preconditions.checkArgument(shardNumber < shardCount);
    List<String> fileNames = allFiles.get(shardNumber);
    return new GoogleCloudStorageFileSet(getBucket(), fileNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(allFiles, bucket, shardCount);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    FilesByShard other = (FilesByShard) obj;
    return Objects.equals(allFiles, other.allFiles)
        && Objects.equals(bucket, other.bucket)
        && Objects.equals(shardCount, other.shardCount);
  }

  public void addFilesToShard(int shardNumber, Iterable<String> newFiles) {
    Preconditions.checkArgument(shardNumber < shardCount);
    List<String> fileNames = allFiles.get(shardNumber);
    Iterables.addAll(fileNames, newFiles);
  }

  public void addFileToShard(int shardNumber, String newFile) {
    Preconditions.checkArgument(shardNumber < shardCount);
    List<String> fileNames = allFiles.get(shardNumber);
    fileNames.add(newFile);
  }

  public int getShardCount() {
    return shardCount;
  }

  @Override
  public String toString() {
    return "FilesByShard [bucket=" + getBucket() + ", allFiles=" + allFiles + ", shardCount="
        + shardCount + "]";
  }

  public String getBucket() {
    return bucket;
  }

  /**
   * Splits the provided input if needed to ensure there are approximately {@code targetNumShards}
   * shards. (Note that it will not combine files from different input shards into a single output
   * shard)
   */
  public void splitShards(int targetNumShards) {
    int origionalShardCount = getShardCount();
    int splitFactor = targetNumShards / origionalShardCount;
    if (splitFactor <= 1) {
      return;
    }
    List<List<String>> oldAllFiles = allFiles;
    shardCount = splitFactor * origionalShardCount;
    allFiles = initAllFiles(shardCount);
    int shard = 0;
    for (List<String> files : oldAllFiles) {
      if (!files.isEmpty()) {
        for (List<String> forShard : SplitUtil.split(files, splitFactor, false)) {
          addFilesToShard(shard++, forShard);
        }
      }
    }
  }

  private static ArrayList<List<String>> initAllFiles(int shardCount) {
    ArrayList<List<String>> result = new ArrayList<>(shardCount);
    for (int i = 0; i < shardCount; i++) {
      result.add(new ArrayList<String>(1));
    }
    return result;
  }
}
