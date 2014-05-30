package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * An ordered list of files in GCS.
 */
public final class GoogleCloudStorageFileSet implements Serializable {

  private static final long serialVersionUID = -3864142913836657244L;
  private final String bucketName;
  private final List<String> fileNames;

  public GoogleCloudStorageFileSet(String bucketName, List<String> fileNames) {
    this.bucketName = Preconditions.checkNotNull(bucketName);
    this.fileNames = ImmutableList.copyOf(fileNames);
  }

  public int getNumFiles() {
    return getFileNames().size();
  }

  public GcsFilename getFile(int i) {
    if (i < 0 || i >= getNumFiles()) {
      throw new IllegalArgumentException("Invalid file number: " + i);
    }
    return new GcsFilename(getBucketName(), getFileNames().get(i));
  }

  public List<GcsFilename> getFiles() {
    List<GcsFilename> result = new ArrayList<>(getFileNames().size());
    for (String name : getFileNames()) {
      result.add(new GcsFilename(getBucketName(), name));
    }
    return result;
  }

  @Override
  public String toString() {
    return "GoogleCloudStorageFileSet [bucketName=" + getBucketName() + ", fileNames="
        + getFileNames() + "]";
  }

  @Override
  public int hashCode() {
    return Objects.hash(bucketName, fileNames);
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
    return Objects.equals(bucketName, ((GoogleCloudStorageFileSet) obj).bucketName)
        && Objects.equals(fileNames, ((GoogleCloudStorageFileSet) obj).fileNames);
  }

  public String getBucketName() {
    return bucketName;
  }

  public List<String> getFileNames() {
    return fileNames;
  }
}
