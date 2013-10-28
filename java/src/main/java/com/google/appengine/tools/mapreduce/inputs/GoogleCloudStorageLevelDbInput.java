package com.google.appengine.tools.mapreduce.inputs;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;
import com.google.appengine.tools.mapreduce.outputs.LevelDbOutput;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * GoogleCloudStorageLevelDbInput creates LevelDbInputReaders to read input written out by
 * {@link LevelDbOutput} to files in Google Cloud Storage.
 *
 */
public class GoogleCloudStorageLevelDbInput extends Input<ByteBuffer> {

  private static final long serialVersionUID = -5135725511174133847L;
  private final GoogleCloudStorageFileSet files;
  private final int bufferSize;

  public GoogleCloudStorageLevelDbInput(GoogleCloudStorageFileSet files) {
    this(files, MapReduceConstants.DEFAULT_IO_BUFFER_SIZE);
  }

  /**
   * @param files The set of files to create readers for. One reader per file.
   * @param bufferSize The size of the buffer used for each file.
   */
  public GoogleCloudStorageLevelDbInput(GoogleCloudStorageFileSet files, int bufferSize) {
    this.files = checkNotNull(files, "Null files");
    this.bufferSize = bufferSize;
    checkArgument(bufferSize > 0, "Buffersize must be > 0");
  }


  @Override
  public List<InputReader<ByteBuffer>> createReaders() {
    List<InputReader<ByteBuffer>> result = new ArrayList<InputReader<ByteBuffer>>();
    for (GcsFilename file : files.getAllFiles()) {
      result.add(new GoogleCloudStorageLevelDbInputReader(file, bufferSize));
    }
    return result;
  }
}
