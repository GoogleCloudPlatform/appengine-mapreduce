/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreService;
import com.google.appengine.api.blobstore.ByteRange;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;
import com.google.common.primitives.Bytes;

import junit.framework.TestCase;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * Unit test for {@code BlobstoreReadableByteChannelFactory}
 * @author idk@google.com (Igor Kushnirskiy)
 */
public class BlobstoreReadableByteChannelFactoryTest extends TestCase {

  static class StubBlobstoreService implements BlobstoreService {
    private final BlobKey blobKey;
    private final byte[] data;

    StubBlobstoreService(BlobKey blobKey, byte[] data) {
      this.blobKey = Preconditions.checkNotNull(blobKey);
      this.data = Preconditions.checkNotNull(data);
    }

    @Override
    public byte[] fetchData(BlobKey blobKey, long start, long end) {
      assertEquals(this.blobKey, blobKey);
      return Bytes.toArray(Bytes.asList(data).subList((int) start, (int) end));
    }

    @Override
    public String createUploadUrl(String s) {
      return null;
    }

    @Override
    public void serve(BlobKey blobKey, HttpServletResponse httpServletResponse) {
    }

    @Override
    public void serve(BlobKey blobKey, String rangeHeader,
        HttpServletResponse httpServletResponse) {
    }

    @Override
    public void serve(BlobKey blobKey, ByteRange byteRange,
        HttpServletResponse httpServletResponse) {
    }

    @Override
    public ByteRange getByteRange(HttpServletRequest httpServletRequest) {
      return null;
    }

    @Override
    public void delete(BlobKey... blobKeys) {
    }

    @Override
    public Map<String, BlobKey> getUploadedBlobs(HttpServletRequest httpServletRequest) {
      return null;
    }
  }


  public void testReadableByteChannel() throws Exception {

    String testString = "Hello world! I am ReadableByteChannel."; //every character is one byte long
    final byte[] testData = testString.getBytes();
    final BlobKey testKey = new BlobKey("foo");

    BlobstoreReadableByteChannelFactory blobstoreReadableByteChannelFactory
        = new BlobstoreReadableByteChannelFactory();
    blobstoreReadableByteChannelFactory.setBlobstoreService(
        new StubBlobstoreService(testKey, testData));
    blobstoreReadableByteChannelFactory.setBlobKeyToSize(new Function<BlobKey, Long>() {
      @Override
      public Long apply(BlobKey key) {
        assertEquals(testKey, key);
        return (long) testData.length;
      }
    });

    for (int i = 0; i < testString.length(); i++) {
      InputStream input = Channels.newInputStream(blobstoreReadableByteChannelFactory
          .getReadableByteChannel(testKey, i));
      Reader reader = new InputStreamReader(input);

      assertEquals(testString.substring(i), CharStreams.toString(reader));
    }
  }
}
