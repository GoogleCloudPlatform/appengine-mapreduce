// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.outputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.files.FileWriteChannel;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.util.FileUtil;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An {@link OutputWriter} that writes bytes to a blob file that it creates.
 *
 * @author ohler@google.com (Christian Ohler)
 */
@SuppressWarnings("deprecation")
public final class BlobFileOutputWriter extends OutputWriter<ByteBuffer> {

  private static final long serialVersionUID = 744830273254378964L;

  private static final FileService FILE_SERVICE = FileServiceFactory.getFileService();

  private final String fileName;
  private final String mimeType;
  private AppEngineFile file;

  private transient FileWriteChannel channel;

  public BlobFileOutputWriter(String fileName, String mimeType) {
    this.fileName = checkNotNull(fileName, "Null fileName");
    this.mimeType = checkNotNull(mimeType, "Null mimeType");
  }

  @Override
  public void beginShard() throws IOException {
    file = FILE_SERVICE.createNewBlobFile(mimeType, fileName);
  }

  @Override
  public void beginSlice() throws IOException {
    channel = FILE_SERVICE.openWriteChannel(file, false);
  }

  @Override
  public void write(ByteBuffer bytes) throws IOException {
    Preconditions.checkState(channel != null, "%s: channel was not created", this);
    while (bytes.hasRemaining()) {
      try {
        channel.write(bytes);
      } catch (IOException ex) {
        // Consider the possibility of an expired channel
        try {
          channel.close();
        } catch (IOException ignore) {
          // ignore
        }
        channel = FILE_SERVICE.openWriteChannel(file, false);
        channel.write(bytes);
      }
    }
  }

  @Override
  public void endSlice() throws IOException {
    channel.close();
  }

  @Override
  public void endShard() {
    file = FileUtil.ensureFinalized(file);
  }

  /**
   * Returns the file or null if file was not finalized.
   */
  public AppEngineFile getFile() {
    if (file == null || !file.hasFinalizedName()) {
      return null;
    }
    return file;
  }
}
