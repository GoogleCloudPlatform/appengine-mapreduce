package com.google.appengine.tools.mapreduce.inputs;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.GcsFileMetadata;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.mapreduce.InputReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

/**
 * A simple wrapper of LevelDb wrapper for GCS to provide getProgress() and do lazy initialization.
 */
public final class GoogleCloudStorageLevelDbInputReader extends InputReader<ByteBuffer> {
 
  private static final GcsService gcsService = GcsServiceFactory.createGcsService();
  private static final long serialVersionUID = 1014960525070958327L;

  private LevelDbInputReader reader;
  private GcsFilename file;
  private int bufferSize;
  private double length = -1;

  /**
   * @param file File to be read.
   * @param bufferSize The buffersize to be used by the Gcs prefetching read channel.
   */
  public GoogleCloudStorageLevelDbInputReader(GcsFilename file, int bufferSize) {
    this.file = checkNotNull(file, "Null file");
    this.bufferSize = bufferSize;
    checkArgument(bufferSize > 0, "Buffersize must be > 0");
  }
  
  @Override
  public ByteBuffer next() throws IOException, NoSuchElementException {
    return reader.next();
  }

  @Override
  public Double getProgress() {
    if (length == -1) {
      GcsFileMetadata metadata = null;
      try {
        metadata = gcsService.getMetadata(file);
      } catch (IOException e) {
        // It is just an estimate so it's probably not worth throwing.
      }
      if (metadata == null) {
        return null;
      }
      length = metadata.getLength();
    }
    if (length == 0f) {
      return null;
    }
    return reader.getBytesRead() / length;
  }

  @Override
  public void beginSlice() throws IOException {
    if (reader == null) {
      this.reader = new LevelDbInputReader(
          gcsService.openPrefetchingReadChannel(file, 0, bufferSize));
    }
    reader.beginSlice();
  }

  @Override
  public void endSlice() throws IOException {
    reader.endSlice();
  }
  
}