package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsOutputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.development.testing.LocalBlobstoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalFileServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

import junit.framework.TestCase;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 */
abstract class CloudStorageLineInputTestCase extends TestCase {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalBlobstoreServiceTestConfig(),
      new LocalFileServiceTestConfig(),
      new LocalDatastoreServiceTestConfig());

  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper.setUp();
  }

  long createFile(GcsFilename filename, String record, int recordsCount) throws IOException {
    GcsService gcsService = GcsServiceFactory.createGcsService();
    GcsOutputChannel writeChannel = gcsService.createOrReplace(filename,
        GcsFileOptions.builder().withMimeType("application/bin"));
    for (int i = 0; i < recordsCount; i++) {
      writeChannel.write(ByteBuffer.wrap(record.getBytes()));
    }
    writeChannel.close();
    return gcsService.getMetadata(filename).getLength();
  }

  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }
}
