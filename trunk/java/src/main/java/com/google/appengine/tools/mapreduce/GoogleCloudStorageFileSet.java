package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * An ordered list of files in GCS.
 */
public class GoogleCloudStorageFileSet implements Serializable {

  private static final long serialVersionUID = -3864142913836657244L;
  private String bucketName;
  private List<String> fileNames;

  public GoogleCloudStorageFileSet(String bucketName, List<String> fileNames) {
    this.bucketName = Preconditions.checkNotNull(bucketName);
    this.fileNames = new ArrayList<String>(fileNames);
  }

  public int getNumFiles() {
    return fileNames.size();
  }

  public GcsFilename getFile(int i) {
    if (i < 0 || i >= getNumFiles()) {
      throw new IllegalArgumentException("Invalid file number: " + i);
    }
    return new GcsFilename(bucketName, fileNames.get(i));
  }

  public List<GcsFilename> getAllFiles() {
    List<GcsFilename> result = new ArrayList<GcsFilename>(fileNames.size());
    for (String name : fileNames) {
      result.add(new GcsFilename(bucketName, name));
    }
    return result;
  }

  @Override
  public final boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o == null || !(o instanceof GoogleCloudStorageFileSet)) {
      return false;
    }
    GoogleCloudStorageFileSet other = (GoogleCloudStorageFileSet) o;
    return Objects.equal(bucketName, other.bucketName) && Objects.equal(fileNames, other.fileNames);
  }

  @Override
  public final int hashCode() {
    return Objects.hashCode(bucketName, fileNames);
  }

  @Override
  public String toString() {
    return "GoogleCloudStorageFileSet [bucketName=" + bucketName + ", fileNames=" + fileNames + "]";
  }

}
