// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.blobstore.BlobInfoFactory;
import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.files.FileWriteChannel;
import com.google.appengine.tools.development.testing.LocalBlobstoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalFileServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

import junit.framework.TestCase;

import java.nio.ByteBuffer;

/**
 */
abstract class BlobstoreInputTestCase extends TestCase {

  public static final String RECORD = "01234567890\n";
  public static final int RECORDS_COUNT = 100;

  // ------------------------------ FIELDS ------------------------------

  BlobKey blobKey;
  long blobSize;
  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalBlobstoreServiceTestConfig(),
      new LocalFileServiceTestConfig(),
      new LocalDatastoreServiceTestConfig());

// ------------------------ OVERRIDING METHODS ------------------------

  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper.setUp();

    FileService fileService = FileServiceFactory.getFileService();
    AppEngineFile blobFile = fileService.createNewBlobFile("application/bin");
    FileWriteChannel writeChannel = fileService.openWriteChannel(blobFile, true);
    for (int i = 0; i < RECORDS_COUNT; i++) {
      writeChannel.write(ByteBuffer.wrap(RECORD.getBytes()));
    }
    writeChannel.closeFinally();
    blobKey = fileService.getBlobKey(blobFile);
    blobSize = new BlobInfoFactory().loadBlobInfo(blobKey).getSize();
  }

  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }
}
