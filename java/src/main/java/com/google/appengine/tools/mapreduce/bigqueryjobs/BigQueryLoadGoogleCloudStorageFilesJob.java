package com.google.appengine.tools.mapreduce.bigqueryjobs;

import com.google.api.client.googleapis.extensions.appengine.auth.oauth2.AppIdentityCredential;
import com.google.api.client.googleapis.services.GoogleClientRequestInitializer;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClientRequest;
import com.google.api.client.googleapis.services.json.CommonGoogleJsonClientRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryRequest;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.impl.BigQueryConstants;
import com.google.appengine.tools.mapreduce.impl.util.SerializableValue;
import com.google.appengine.tools.mapreduce.outputs.BigQueryStoreResult;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Value;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A pipeline job that loads files stored in Google Cloud Storage into a bigquery table.
 */
public final class BigQueryLoadGoogleCloudStorageFilesJob extends
    Job1<List<BigQueryLoadJobReference>, BigQueryStoreResult<GoogleCloudStorageFileSet>> {

  private static final long serialVersionUID = 4162438273017726233L;
  private final String dataset;
  private final String tableName;
  private final String projectId;

  private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
  private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

  private static final Logger log =
      Logger.getLogger(BigQueryLoadGoogleCloudStorageFilesJob.class.getName());

  static Bigquery getBigquery() {
    List<String> scopes = Lists.newArrayList();
    scopes.add(BigQueryConstants.BQ_SCOPE);
    AppIdentityCredential credential = new AppIdentityCredential.Builder(scopes).build();
    GoogleClientRequestInitializer initializer = new CommonGoogleJsonClientRequestInitializer() {
      @SuppressWarnings("unused")
      public void initialize(
          @SuppressWarnings("rawtypes") AbstractGoogleJsonClientRequest request) {
        @SuppressWarnings("rawtypes")
        BigqueryRequest bigqueryRequest = (BigqueryRequest) request;
        bigqueryRequest.setPrettyPrint(true);
      }
    };
    return new Bigquery.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
        .setHttpRequestInitializer(credential).setGoogleClientRequestInitializer(initializer)
        .build();
  }

  /**
   * @param dataset the name of the bigquery dataset.
   * @param tableName name of the bigquery table to load data.
   * @param projectId bigquery project Id.
   */
  public BigQueryLoadGoogleCloudStorageFilesJob(String dataset, String tableName,
      String projectId) {
    this.dataset = dataset;
    this.tableName = tableName;
    this.projectId = projectId;
  }

  /**
   * Divides the files into bundles having size less than or equal to the maximum size allowed per
   * bigquery load {@link Job} and then starts separate pipeline jobs for each of the bundles.
   * Returns a list of {@link JobReference}s.
   */
  @Override
  public Value<List<BigQueryLoadJobReference>> run(
      BigQueryStoreResult<GoogleCloudStorageFileSet> bigQueryStoreResult) throws Exception {
    BigQueryStoreResult<GoogleCloudStorageFileSet> outputResult = bigQueryStoreResult;
    List<GcsFilename> files = outputResult.getResult().getFiles();
    List<List<GcsFilename>> bundles =
        bundleFiles(files, BigQueryConstants.BIGQUERY_LOAD_DATA_SIZE_LIMIT);
    List<FutureValue<BigQueryLoadJobReference>> jobReferenceList = new ArrayList<>();
    for (List<GcsFilename> bundle : bundles) {
      jobReferenceList.add(futureCall(new BigQueryLoadFileSetJob(dataset, tableName, projectId,
          bundle, SerializableValue.of(Marshallers.getGenericJsonMarshaller(TableSchema.class),
              outputResult.getSchema())), immediate(Integer.valueOf(0))));
    }
    return futureList(jobReferenceList);
  }

  /**
   * @param files list of gcs files to load into bigquery.
   * @param bundleSizeLimit size limit of the bundle of files.
   * @return List of files bundled together so that the combined size of the files is less than the
   *         specified bundle size limit.
   * @throws IOException
   */
  private List<List<GcsFilename>> bundleFiles(List<GcsFilename> files, long bundleSizeLimit)
      throws IOException {
    List<List<GcsFilename>> bundles = new ArrayList<>();
    List<GcsFilename> currentBundle = new ArrayList<>();
    long currentBundleSize = 0;
    GcsService GCS_SERVICE = GcsServiceFactory.createGcsService();
    for (GcsFilename file : files) {
      long fileSize = GCS_SERVICE.getMetadata(file).getLength();
      if (currentBundleSize + fileSize > bundleSizeLimit) {
        bundles.add(ImmutableSet.copyOf(currentBundle).asList());
        currentBundle = new ArrayList<>();
        currentBundleSize = 0;
      }
      currentBundle.add(file);
      currentBundleSize += fileSize;
    }
    bundles.add(ImmutableSet.copyOf(currentBundle).asList());
    return bundles;
  }

  public Value<BigQueryStoreResult<GoogleCloudStorageFileSet>> handleException(Throwable t)
      throws Throwable {
    log.log(Level.SEVERE, "Bigquery data load job failed because of : ", t);
    throw t;
  }
}
