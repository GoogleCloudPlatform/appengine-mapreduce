// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.util;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.files.FileWriteChannel;
import com.google.appengine.api.files.FinalizationException;
import com.google.apphosting.api.ApiProxy;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class FileUtil {

  private static final Logger log = Logger.getLogger(FileUtil.class.getName());

  private static final FileService FILE_SERVICE = FileServiceFactory.getFileService();

  private FileUtil() {}

  public static boolean isErrorCode(IOException e, int code) {
    return e.getCause() != null
        && e.getCause() instanceof ApiProxy.ApplicationException
        && ((ApiProxy.ApplicationException) e.getCause()).getApplicationError() == code;
  }

  public static void tryClosingOnce(FileWriteChannel c) throws IOException {
    // TODO(ohler): Fix up Files API so that we can reliably close a
    // FileWriteChannel without all this.  ("File wasn't open" should not be the
    // same exception type as "closing failed due to e.g. timeout".)  (Or maybe
    // locks are just a mess and should be removed, or made optional for
    // finalization.)
    if (c.isOpen()) {
      try {
        c.close();
      } catch (IOException e) {
        if (isErrorCode(e, 10)) {
          log.log(Level.INFO, "File " + c + " already closed, ignoring exception: "
              + RetryHelper.messageChain(e));
          return;
        }
        log.log(Level.INFO, "Got IOException closing " + c, e);
        throw e;
      }
    }
  }

  public static BlobKey getBlobKey(final AppEngineFile finalizedBlobFile) {
    return RetryHelper.runWithRetries(
        new RetryHelper.Body<BlobKey>() {
          @Override public String toString() {
            return "get BlobKey for " + finalizedBlobFile;
          }
          @Override public BlobKey run() throws IOException {
            BlobKey key = FILE_SERVICE.getBlobKey(finalizedBlobFile);
            if (key == null) {
              // I have the impression that this can happen because of HRD's
              // eventual consistency.  Retry.
              throw new IOException(this + ": getBlobKey() returned null");
            }
            return key;
          }
        });
  }

  // TODO(ohler): Find out if the Files API already has a way to do this.
  public static AppEngineFile getReadHandle(AppEngineFile file) {
    if (file.getFileSystem() == AppEngineFile.FileSystem.BLOBSTORE) {
      // TODO(ohler): Replace FileServiceImpl.getBlobFile() with this, or
      // understand why all the other work it's doing is needed.
      return new AppEngineFile(file.getFileSystem(), getBlobKey(file).getKeyString());
    } else {
      // TODO(ohler): Figure out what needs to happen for bigstore files.
      return file;
    }
  }

  // TODO(ohler): Simplify this.  Better yet, make fileproxy require no lock for
  // finalization, make finalization idempotent, make the Files API provide read
  // handles, and then get rid of this.
  public static AppEngineFile ensureFinalized(final AppEngineFile file) {
    return RetryHelper.runWithRetries(
        new RetryHelper.Body<AppEngineFile>() {
          @Override public String toString() {
            return "finalizing file " + file;
          }
          @SuppressWarnings("unused") String stage = "init";
          FileWriteChannel out = null;
          @Override public AppEngineFile run() throws IOException {
            if (out != null) {
              stage = "close previous";
              tryClosingOnce(out);
              out = null;
            }
            boolean alreadyFinalized;
            stage = "acquire lock";
            try {
              out = FILE_SERVICE.openWriteChannel(file, true);
              alreadyFinalized = false;
            } catch (FinalizationException e) {
              log.info(this + ": File already finalized, ignoring exception: "
                  + RetryHelper.messageChain(e));
              alreadyFinalized = true;
            } catch (IOException e) {
              if (isErrorCode(e, 101)) {
                log.info(this + ": File already finalized, ignoring exception: "
                    + RetryHelper.messageChain(e));
                alreadyFinalized = true;
              } else {
                throw e;
              }
            }
            if (!alreadyFinalized) {
              stage = "close and finalize";
              out.closeFinally();
            } else {
              Preconditions.checkState(out == null, "%s: %s", this, out);
            }
            return getReadHandle(file);
          }
        });
  }

}
