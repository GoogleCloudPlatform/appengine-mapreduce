// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreService;
import com.google.appengine.api.blobstore.BlobstoreServiceFactory;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.files.FinalizationException;
import com.google.appengine.api.files.LockException;
import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsOutputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.development.testing.LocalBlobstoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

import junit.framework.TestCase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

/**
 */
@SuppressWarnings("deprecation")
abstract class BlobstoreInputTestCase extends TestCase {

  BlobKey blobKey;
  long blobSize;
  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalBlobstoreServiceTestConfig(),
      new LocalDatastoreServiceTestConfig());

  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

  protected void createFile(String record, int recordsCount)
      throws IOException, FileNotFoundException, FinalizationException, LockException {

    String filename = UUID.randomUUID().toString();
    GcsFilename gcsFilename = new GcsFilename("bucket", filename);
    GcsService gcsService = GcsServiceFactory.createGcsService();
    try (GcsOutputChannel writeChannel = gcsService.createOrReplace(
        gcsFilename, new GcsFileOptions.Builder().mimeType("application/bin").build())) {
      for (int i = 0; i < recordsCount; i++) {
        writeChannel.write(ByteBuffer.wrap(record.getBytes()));
      }
    }
    BlobstoreService blobstore = BlobstoreServiceFactory.getBlobstoreService();
    blobKey = blobstore.createGsBlobKey("/gs/bucket/" + filename);
    blobSize = gcsService.getMetadata(gcsFilename).getLength();
    Entity entity = new Entity("__BlobInfo__", blobKey.getKeyString());
    entity.setProperty("size", blobSize);
    entity.setProperty("content_type", "application/bin");
    entity.setProperty("md5_hash", filename);
    entity.setProperty("creation", new Date());
    entity.setProperty("filename", filename);
    DatastoreServiceFactory.getDatastoreService().put(entity);
  }
}
