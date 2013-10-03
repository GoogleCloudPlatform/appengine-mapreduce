package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;
import com.google.appengine.tools.mapreduce.impl.util.LevelDbConstants;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An composition of {@link GoogleCloudStorageLevelDbOutputWriter} and
 * {@link GoogleCloudStorageFileOutputWriter} that pads blocks to GCS write boundaries on end of
 * slice. This is needed because GCS requires data to be passed in in 256kb but LevelDb uses 32kb
 * blocks this class provides a way get this class to pad the output by writing empty blocks.
 *
 */
public class GoogleCloudStorageLevelDbOutputWriter extends LevelDbOutputWriter {
  private static final long serialVersionUID = 6507809614070157553L;

  public GoogleCloudStorageLevelDbOutputWriter(OutputWriter<ByteBuffer> delegate) {
    super(delegate);
  }

  @Override
  public void endSlice() throws IOException {
    padAndWriteBlock(false);
    while (
        (getNumBlocksWritten() * LevelDbConstants.BLOCK_SIZE) % MapReduceConstants.GCS_IO_BLOCK_SIZE
        != 0) {
      padAndWriteBlock(true);
    }
    getDelegate().endSlice();
  }

}
