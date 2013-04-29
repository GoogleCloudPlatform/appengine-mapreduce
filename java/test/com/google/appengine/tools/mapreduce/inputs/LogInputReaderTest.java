package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.log.LogQuery;
import com.google.appengine.api.log.RequestLogs;
import com.google.appengine.api.log.dev.LocalLogService;
import com.google.appengine.tools.development.testing.LocalLogServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * Test the LogInputReader class
 */
public class LogInputReaderTest extends TestCase {
  private static final long BASE_TIME = LogInput.EARLIEST_LOG_TIME;
  private static final String VERSION_ID = "1";
  private static final String APP_ID = "a";
  private static final String IP = "1.2.3.4";
  private static final String NICKNAME = "nickname";
  private static final String METHOD = "POST";
  private static final String RESOURCE = "/resource";
  private static final String HTTP_VERSION = "HTTP/1.1";
  private static final String USER_AGENT = "agent";
  private static final int STATUS_CODE = 200;
  private static final String REFERRER = "referrer";

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalLogServiceTestConfig());

  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

  private void checkReader(LogInputReader reader, String[] requestIds) {
    for (String expectedRequestId : requestIds) {
      RequestLogs log = reader.next();
      assertEquals(expectedRequestId, log.getRequestId());
    }
    try {
      RequestLogs log = reader.next();
      fail("Too many logs found");
    } catch (NoSuchElementException expected) {
      // We intentionally perform one extra fetch to ensure there are no extra logs
    }
  }

  private LogInputReader createReader(long startTimeUsec, long endTimeUsec) {
    LogQuery logQuery = LogQuery.Builder.withDefaults();
    logQuery.startTimeUsec(startTimeUsec);
    logQuery.endTimeUsec(endTimeUsec);
    ArrayList<String> versionList = new ArrayList<String>();
    versionList.add(VERSION_ID);
    logQuery.majorVersionIds(versionList);
    return new LogInputReader(logQuery);
  }

  public void testStartTimeBefore() throws Exception {
    LocalLogService localService = LocalLogServiceTestConfig.getLocalLogService();
    localService.addRequestInfo(APP_ID, VERSION_ID, "req01", IP, NICKNAME, BASE_TIME + 10L,
        BASE_TIME + 20L, METHOD, RESOURCE, HTTP_VERSION, USER_AGENT, true, STATUS_CODE, REFERRER);

    LogInputReader reader = createReader(BASE_TIME + 19L, BASE_TIME + 25L);
    reader.beginSlice();
    checkReader(reader, new String[] { "req01" });
  }

  public void testStartTimeEqual() throws Exception {
    LocalLogService localService = LocalLogServiceTestConfig.getLocalLogService();
    localService.addRequestInfo(APP_ID, VERSION_ID, "req01", IP, NICKNAME, BASE_TIME + 10L,
        BASE_TIME + 20L, METHOD, RESOURCE, HTTP_VERSION, USER_AGENT, true, STATUS_CODE, REFERRER);

    LogInputReader reader = createReader(BASE_TIME + 20L, BASE_TIME + 25L);
    reader.beginSlice();
    checkReader(reader, new String[] { "req01" });
  }

  public void testStartTimeAfter() throws Exception {
    LocalLogService localService = LocalLogServiceTestConfig.getLocalLogService();
    localService.addRequestInfo(APP_ID, VERSION_ID, "req01", IP, NICKNAME, BASE_TIME + 10L,
        BASE_TIME + 20L, METHOD, RESOURCE, HTTP_VERSION, USER_AGENT, true, STATUS_CODE, REFERRER);

    LogInputReader reader = createReader(BASE_TIME + 21L, BASE_TIME + 25L);
    reader.beginSlice();
    checkReader(reader, new String[] {});
  }

  public void testEndTimeEqual() throws Exception {
    LocalLogService localService = LocalLogServiceTestConfig.getLocalLogService();
    localService.addRequestInfo(APP_ID, VERSION_ID, "req01", IP, NICKNAME, BASE_TIME + 10L,
        BASE_TIME + 20L, METHOD, RESOURCE, HTTP_VERSION, USER_AGENT, true, STATUS_CODE, REFERRER);

    LogInputReader reader = createReader(BASE_TIME + 15L, BASE_TIME + 20L);
    reader.beginSlice();
    checkReader(reader, new String[] {});
  }

  public void testEndTimeAfter() throws Exception {
    LocalLogService localService = LocalLogServiceTestConfig.getLocalLogService();
    localService.addRequestInfo(APP_ID, VERSION_ID, "req01", IP, NICKNAME, BASE_TIME + 10L,
        BASE_TIME + 20L, METHOD, RESOURCE, HTTP_VERSION, USER_AGENT, true, STATUS_CODE, REFERRER);

    LogInputReader reader = createReader(BASE_TIME + 15L, BASE_TIME + 21L);
    reader.beginSlice();
    checkReader(reader, new String[] { "req01" });
  }

  public void testSlicing() throws Exception {
    // While the local log interface exposes request ID as a string, the log API's offset handling
    // assumes this is a number and will attempt to parse it
    LocalLogService localService = LocalLogServiceTestConfig.getLocalLogService();
    localService.addRequestInfo(APP_ID, VERSION_ID, "1", IP, NICKNAME, BASE_TIME + 10L,
        BASE_TIME + 20L, METHOD, RESOURCE, HTTP_VERSION, USER_AGENT, true, STATUS_CODE, REFERRER);

    localService.addRequestInfo(APP_ID, VERSION_ID, "2", IP, NICKNAME, BASE_TIME + 20L,
        BASE_TIME + 30L, METHOD, RESOURCE, HTTP_VERSION, USER_AGENT, true, STATUS_CODE, REFERRER);

    LogInputReader reader1 = createReader(BASE_TIME + 0L, BASE_TIME + 200L);
    // Begin and end a slice before reading any items
    reader1.beginSlice();
    reader1.endSlice();

    // Serialize and reconstruct and read one of the two logs
    LogInputReader reader2 = (LogInputReader) SerializationUtil.deserializeFromByteArray(
        SerializationUtil.serializeToByteArray(reader1));
    reader2.beginSlice();
    RequestLogs logReq02 = reader2.next();
    assertEquals("2", logReq02.getRequestId());
    reader2.endSlice();

    // Serialize and reconstruct and read the remaining log
    LogInputReader reader3 = (LogInputReader) SerializationUtil.deserializeFromByteArray(
        SerializationUtil.serializeToByteArray(reader2));
    reader3.beginSlice();
    RequestLogs logReq01 = reader3.next();
    assertEquals("1", logReq01.getRequestId());
    reader3.endSlice();
  }

  public void testProgress() throws Exception {
    LocalLogService localService = LocalLogServiceTestConfig.getLocalLogService();
    localService.addRequestInfo(APP_ID, VERSION_ID, "req01", IP, NICKNAME, BASE_TIME + 10L,
        BASE_TIME + 20L, METHOD, RESOURCE, HTTP_VERSION, USER_AGENT, true, STATUS_CODE, REFERRER);

    localService.addRequestInfo(APP_ID, VERSION_ID, "req02", IP, NICKNAME, BASE_TIME + 20L,
        BASE_TIME + 100L, METHOD, RESOURCE, HTTP_VERSION, USER_AGENT, true, STATUS_CODE, REFERRER);

    LogInputReader reader = createReader(BASE_TIME + 0L, BASE_TIME + 200L);
    // Before any logs are read
    assertEquals(0.0, reader.getProgress());
    // Read log at 100 usec of 200 usec (logs are read backwards, so 100 usec left)
    reader.beginSlice();
    reader.next();
    assertEquals(0.5, reader.getProgress());
    // Read log at 20 usec of 200 usec (logs are read backwards, so only 20 usec left)
    reader.next();
    assertEquals(0.9, reader.getProgress());
  }
}