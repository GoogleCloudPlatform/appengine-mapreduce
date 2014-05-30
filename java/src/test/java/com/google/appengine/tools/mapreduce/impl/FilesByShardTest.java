package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;

import junit.framework.TestCase;

import java.util.Arrays;
import java.util.List;

public class FilesByShardTest extends TestCase {


  public void testAdd() {
    FilesByShard fbs = new FilesByShard(10, "Foo");
    fbs.addFileToShard(1, "Bar");
    fbs.addFileToShard(1, "Baz");
    fbs.addFileToShard(1, "Bat");
    GoogleCloudStorageFileSet forShard = fbs.getFilesForShard(1);
    List<GcsFilename> files = forShard.getFiles();
    assertEquals(3, files.size());
    assertEquals("Foo", files.get(0).getBucketName());
    assertEquals("Bar", files.get(0).getObjectName());
    assertEquals("Baz", files.get(1).getObjectName());
    assertEquals("Bat", files.get(2).getObjectName());
  }

  public void testAddAll() {
    FilesByShard fbs = new FilesByShard(10, "Foo");
    fbs.addFilesToShard(1, Arrays.asList("Bar", "Baz", "Bat"));
    GoogleCloudStorageFileSet forShard = fbs.getFilesForShard(1);
    List<GcsFilename> files = forShard.getFiles();
    assertEquals(3, files.size());
    assertEquals("Foo", files.get(0).getBucketName());
    assertEquals("Bar", files.get(0).getObjectName());
    assertEquals("Baz", files.get(1).getObjectName());
    assertEquals("Bat", files.get(2).getObjectName());
  }

  public void testEmptyShards() {
    FilesByShard fbs = new FilesByShard(10, "Foo");
    GoogleCloudStorageFileSet forShard = fbs.getFilesForShard(1);
    List<GcsFilename> files = forShard.getFiles();
    assertEquals(0, files.size());
  }

  public void testSplit() {
    FilesByShard fbs = new FilesByShard(2, "Foo");
    fbs.addFilesToShard(0, Arrays.asList("Bar0", "Baz0", "Bat0"));
    fbs.addFilesToShard(1, Arrays.asList("Bar1", "Baz1", "Bat1"));
    fbs.splitShards(4);
    assertEquals(4, fbs.getShardCount());
    assertEquals(Arrays.asList("Bar0", "Baz0"), fbs.getFilesForShard(0).getFileNames());
    assertEquals(Arrays.asList("Bat0"), fbs.getFilesForShard(1).getFileNames());
    assertEquals(Arrays.asList("Bar1", "Baz1"), fbs.getFilesForShard(2).getFileNames());
    assertEquals(Arrays.asList("Bat1"), fbs.getFilesForShard(3).getFileNames());
  }

  public void testSplitNotNeeded() {
    FilesByShard fbs = new FilesByShard(5, "Foo");
    fbs.addFilesToShard(0, Arrays.asList("Bar0", "Baz0", "Bat0"));
    fbs.addFilesToShard(1, Arrays.asList("Bar1", "Baz1", "Bat1"));
    fbs.addFilesToShard(2, Arrays.asList("Bar2", "Baz2", "Bat2"));
    fbs.addFilesToShard(3, Arrays.asList("Bar3", "Baz3", "Bat3"));
    fbs.addFilesToShard(4, Arrays.asList("Bar4", "Baz4", "Bat4"));
    FilesByShard copy = new FilesByShard(5, "Foo");
    copy.addFilesToShard(0, Arrays.asList("Bar0", "Baz0", "Bat0"));
    copy.addFilesToShard(1, Arrays.asList("Bar1", "Baz1", "Bat1"));
    copy.addFilesToShard(2, Arrays.asList("Bar2", "Baz2", "Bat2"));
    copy.addFilesToShard(3, Arrays.asList("Bar3", "Baz3", "Bat3"));
    copy.addFilesToShard(4, Arrays.asList("Bar4", "Baz4", "Bat4"));
    for (int i = 0; i < 10; i++) {
      fbs.splitShards(i);
      assertEquals(copy, fbs);
    }
    fbs.splitShards(10);
    assertFalse(copy.equals(fbs));
  }

  public void testSparseSplit() {
    FilesByShard fbs = new FilesByShard(5, "Foo");
    fbs.addFilesToShard(0, Arrays.asList("Bar0", "Baz0", "Bat0"));
    fbs.addFilesToShard(1, Arrays.asList("Bar1", "Baz1", "Bat1"));
    fbs.addFilesToShard(2, Arrays.asList("Bar2", "Baz2", "Bat2"));
    fbs.addFilesToShard(3, Arrays.asList("Bar3", "Baz3", "Bat3"));
    fbs.addFilesToShard(4, Arrays.asList("Bar4", "Baz4", "Bat4"));
    fbs.splitShards(100);
    assertEquals(100, fbs.getShardCount());
    for (int i = 0; i < 15; i++) {
      assertEquals(1, fbs.getFilesForShard(i).getFiles().size());
    }
    for (int i = 15; i < 100; i++) {
      assertEquals(0, fbs.getFilesForShard(i).getFiles().size());
    }
  }

  public void testSplitWithRemainder() {
    FilesByShard fbs = new FilesByShard(5, "Foo");
    fbs.addFilesToShard(0, Arrays.asList("Bar0", "Baz0", "Bat0"));
    fbs.addFilesToShard(1, Arrays.asList("Bar1", "Baz1", "Bat1"));
    fbs.addFilesToShard(2, Arrays.asList("Bar2", "Baz2", "Bat2"));
    fbs.addFilesToShard(3, Arrays.asList("Bar3", "Baz3", "Bat3"));
    fbs.addFilesToShard(4, Arrays.asList("Bar4", "Baz4", "Bat4"));
    fbs.splitShards(17);
    assertEquals(15, fbs.getShardCount());
    for (int i = 0; i < 15; i++) {
      assertEquals(1, fbs.getFilesForShard(i).getFiles().size());
    }
  }
}
