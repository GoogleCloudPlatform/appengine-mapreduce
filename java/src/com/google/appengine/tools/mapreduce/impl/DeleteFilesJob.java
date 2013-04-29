// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreServiceFactory;
import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.tools.mapreduce.impl.util.FileUtil;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Value;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.logging.Logger;

/**
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class DeleteFilesJob extends Job1<Void, List<AppEngineFile>> {
  private static final long serialVersionUID = 701830786556967921L;

  private static final Logger log = Logger.getLogger(DeleteFilesJob.class.getName());

  // 20 is too many, leads to timeout.
  private static final int MAX_BLOB_DELETIONS_PER_RPC = 1;
  private static final int MAX_BLOB_DELETIONS_PER_JOB = 20;

  private static class DeleteBlobsJob extends Job1<Void, List<AppEngineFile>> {
    private static final long serialVersionUID = 832681572879957125L;

    private DeleteBlobsJob() {
    }

    @Override public Value<Void> run(List<AppEngineFile> filesToDelete) {
      List<BlobKey> keys = Lists.newArrayListWithCapacity(filesToDelete.size());
      for (AppEngineFile file : filesToDelete) {
        Preconditions.checkArgument(file.getFileSystem() == AppEngineFile.FileSystem.BLOBSTORE,
            "%s: File has unexpected file system: %s", this, file);
        keys.add(FileUtil.getBlobKey(file));
      }
      for (List<BlobKey> blobs : Lists.partition(keys, MAX_BLOB_DELETIONS_PER_RPC)) {
        log.info("Deleting " + blobs);
        // Deleting an unknown file is a no-op, so no special logic needed to
        // make retries idempotent.
        BlobstoreServiceFactory.getBlobstoreService().delete(blobs.toArray(new BlobKey[0]));
      }
      return null;
    }
  }

  private final String description;

  public DeleteFilesJob(String description) {
    this.description = description;
  }

  @Override public String toString() {
    return "DeleteFilesJob(" + description + ")";
  }

  @Override public Value<Void> run(List<AppEngineFile> filesToDelete) {
    Iterable<List<AppEngineFile>> batches =
        Iterables.partition(filesToDelete, MAX_BLOB_DELETIONS_PER_JOB);
    for (List<AppEngineFile> batch : batches) {
      // Copy to make it serializable.
      List<AppEngineFile> copy = ImmutableList.copyOf(batch);
      if (true) {
        // Schedule separate job for parallelism
        futureCall(new DeleteBlobsJob(), immediate(copy));
      } else {
        log.info(this + ": Was supposed to delete " + copy + " but left them for debugging");
      }
    }
    return null;
  }

}
