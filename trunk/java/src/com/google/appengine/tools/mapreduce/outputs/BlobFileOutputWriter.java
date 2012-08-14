// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.outputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.files.FileWriteChannel;
import com.google.appengine.tools.mapreduce.LifecycleListenerRegistry;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Worker;
import com.google.appengine.tools.mapreduce.impl.util.FileUtil;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An {@link OutputWriter} that writes bytes to a blob file that it creates.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class BlobFileOutputWriter extends OutputWriter<ByteBuffer> {
  private static final long serialVersionUID = 744830273254378964L;

  private static final FileService FILE_SERVICE = FileServiceFactory.getFileService();

  private final String fileName;
  private final String mimeType;
  private AppEngineFile file = null;
  private boolean closed = false;

  private transient FileWriteChannel channel;

  BlobFileOutputWriter(String fileName, String mimeType) {
    this.fileName = checkNotNull(fileName, "Null fileName");
    this.mimeType = checkNotNull(mimeType, "Null mimeType");
  }

  public static BlobFileOutputWriter forWorker(Worker<?> worker,
      String fileName, String mimeType) {
    return forRegistry(worker.getLifecycleListenerRegistry(), fileName, mimeType);
  }

  public static BlobFileOutputWriter forRegistry(LifecycleListenerRegistry registry,
      String fileName, String mimeType) {
    BlobFileOutputWriter writer = new BlobFileOutputWriter(mimeType, fileName);
    // We could now add a listener to registry but it so happens that we don't
    // currently care about {begin,end}{Slice,Shard}.
    return writer;
  }

  private void ensureOpen() throws IOException {
    if (channel != null) {
      // This only works if slices are <30 seconds.  TODO(ohler): close and
      // reopen every 29 seconds.  Better yet, change fileproxy to not require
      // the file to be open.
      return;
    }
    if (file == null) {
      file = FILE_SERVICE.createNewBlobFile(mimeType, fileName);
    }
    channel = FILE_SERVICE.openWriteChannel(file, false);
  }

  @Override public void write(ByteBuffer bytes) throws IOException {
    Preconditions.checkState(!closed, "%s: already closed", this);
    if (bytes.hasRemaining()) {
      ensureOpen();
      channel.write(bytes);
    }
  }

  @Override public void close() throws IOException {
    if (closed) {
      return;
    }
    if (file == null) {
      closed = true;
      return;
    }
    if (channel != null) {
      channel.close();
    }
    FileUtil.ensureFinalized(file);
    closed = true;
  }

  /**
   * Returns the file, or null if no data has been written.  Must not be called
   * before {@link #close}.
   */
  public AppEngineFile getFile() {
    Preconditions.checkState(closed, "%s: still open", this);
    return file;
  }

}
