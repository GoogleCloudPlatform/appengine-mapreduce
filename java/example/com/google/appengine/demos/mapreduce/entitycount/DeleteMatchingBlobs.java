// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.demos.mapreduce.entitycount;

import com.google.appengine.api.blobstore.BlobInfo;
import com.google.appengine.api.blobstore.BlobInfoFactory;
import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreServiceFactory;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Job2;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.appengine.tools.pipeline.Value;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author ohler@google.com (Christian Ohler)
 */
// TODO(ohler): make this a MapReduce over BlobInfo entities.
public class DeleteMatchingBlobs extends Job1<Void, Predicate<BlobInfo>> {
  private static final long serialVersionUID = 880699488120908550L;

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(DeleteMatchingBlobs.class.getName());

  public static String start(Predicate<BlobInfo> predicate) {
    PipelineService pipelineService = PipelineServiceFactory.newPipelineService();
    return pipelineService.startNewPipeline(new DeleteMatchingBlobs(), predicate);
  }

  private static final int MAX_RPCS_PER_TASK = 50;
  private static final int MAX_BLOBINFOS_SCANNED_PER_TASK = 1000;

  private static class DeleteBlobs extends Job1<Void, List<BlobKey>> {
    private static final long serialVersionUID = 431023608404828441L;

    @Override public Value<Void> run(List<BlobKey> blobKeys) {
      for (BlobKey key : blobKeys) {
        log.info("Deleting " + key);
        BlobstoreServiceFactory.getBlobstoreService().delete(key);
      }
      return null;
    }
  }

  private static class DeleteBlobsStartingWith
      extends Job2<Void, Predicate<BlobInfo>, BlobKey> {
    private static final long serialVersionUID = 832681572879957125L;

    @Override public Value<Void> run(Predicate<BlobInfo> predicate, BlobKey startKey) {
      Iterator<BlobInfo> infos = new BlobInfoFactory().queryBlobInfosAfter(startKey);
      ImmutableList.Builder<BlobKey> toDelete = ImmutableList.builder();
      for (int i = 0; i < MAX_BLOBINFOS_SCANNED_PER_TASK && infos.hasNext(); i++) {
        BlobInfo info = infos.next();
        if (predicate.apply(info)) {
          toDelete.add(info.getBlobKey());
        }
      }
      for (List<BlobKey> partition : Lists.partition(toDelete.build(), MAX_RPCS_PER_TASK)) {
        List<BlobKey> list = ImmutableList.copyOf(partition);
        futureCall(new DeleteBlobs(), immediate(list));
      }
      if (infos.hasNext()) {
        futureCall(new DeleteBlobsStartingWith(),
            immediate(predicate), immediate(infos.next().getBlobKey()));
      }
      return null;
    }
  }

  public DeleteMatchingBlobs() {
  }

  public Value<Void> run(Predicate<BlobInfo> predicate) {
    return futureCall(new DeleteBlobsStartingWith(),
        immediate(predicate), this.<BlobKey>immediate(null));
  }

}
