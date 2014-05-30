package com.google.appengine.tools.mapreduce.impl.pipeline;

import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.GCS_RETRY_PARAMETERS;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.RetriesExhaustedException;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Value;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A job which deletes all the files in the provided GoogleCloudStorageFileSet
 */
public class DeleteFilesJob extends Job1<Void, List<GcsFilename>> {

  private static final long serialVersionUID = 4821135390816992131L;
  private static final GcsService gcs = GcsServiceFactory.createGcsService(GCS_RETRY_PARAMETERS);
  private static final Logger log = Logger.getLogger(DeleteFilesJob.class.getName());

  /**
   * Deletes the files in the provided GoogleCloudStorageFileSet
   */
  @Override
  public Value<Void> run(List<GcsFilename> files) throws Exception {
    for (GcsFilename file : files) {
      try {
        gcs.delete(file);
      } catch (RetriesExhaustedException | IOException e) {
        log.log(Level.WARNING, "Failed to cleanup file: " + file, e);
      }
    }
    return null;
  }
}